//! Minimal hand-written CRI `RuntimeService` gRPC client.
//!
//! Only the two RPCs Talia needs are implemented (`ListPodSandbox` and
//! `PodSandboxStatus`). The client dials the runtime over a Unix domain
//! socket, which is how every CRI-compatible runtime (containerd, CRI-O,
//! cri-dockerd) exposes its API.

use std::future::Future;
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use http::uri::PathAndQuery;
use hyper_util::rt::TokioIo;
use tokio::net::UnixStream;
use tonic::Request;
use tonic::transport::Channel;
use tonic::transport::Endpoint;
use tonic::transport::Uri;
use tonic_prost::ProstCodec;
use tower_service::Service;

use super::cri::ListPodSandboxRequest;
use super::cri::ListPodSandboxResponse;
use super::cri::PodSandbox;
use super::cri::PodSandboxStatusRequest;
use super::cri::PodSandboxStatusResponse;

/// gRPC method paths on `runtime.v1.RuntimeService`.
const LIST_POD_SANDBOX_PATH: &str = "/runtime.v1.RuntimeService/ListPodSandbox";
const POD_SANDBOX_STATUS_PATH: &str = "/runtime.v1.RuntimeService/PodSandboxStatus";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// `tower_service::Service` that dials a Unix domain socket for every call.
///
/// Mirrors the connector tonic itself uses for Unix sockets, but `pub(crate)`
/// there, so it is reimplemented here.
struct UnixSocketConnector {
    socket: std::path::PathBuf,
}

impl Service<Uri> for UnixSocketConnector {
    type Response = TokioIo<UnixStream>;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<TokioIo<UnixStream>, io::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _uri: Uri) -> Self::Future {
        let socket = self.socket.clone();
        Box::pin(async move {
            let stream = UnixStream::connect(socket).await?;
            Ok(TokioIo::new(stream))
        })
    }
}

/// Thin async client for the CRI `RuntimeService`.
pub struct RuntimeServiceClient {
    grpc: tonic::client::Grpc<Channel>,
}

impl RuntimeServiceClient {
    /// Dials the CRI API at the given Unix socket.
    pub async fn connect(socket: &Path) -> Result<Self, tonic::transport::Error> {
        let channel = Endpoint::from_static("http://localhost")
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .connect_with_connector(UnixSocketConnector {
                socket: socket.to_path_buf(),
            })
            .await?;
        Ok(Self {
            grpc: tonic::client::Grpc::new(channel),
        })
    }

    /// Lists pod sandboxes matching `filter`.
    pub async fn list_pod_sandboxes(
        &mut self,
        filter: super::cri::PodSandboxFilter,
    ) -> Result<Vec<PodSandbox>, tonic::Status> {
        self.grpc
            .ready()
            .await
            .map_err(|error| tonic::Status::unknown(format!("CRI service not ready: {error}")))?;
        let codec = ProstCodec::<ListPodSandboxRequest, ListPodSandboxResponse>::default();
        let request = Request::new(ListPodSandboxRequest {
            filter: Some(filter),
        });
        let response = self
            .grpc
            .unary(
                request,
                PathAndQuery::from_static(LIST_POD_SANDBOX_PATH),
                codec,
            )
            .await?;
        Ok(response.into_inner().items)
    }

    /// Returns verbose status for one pod sandbox.
    pub async fn pod_sandbox_status(
        &mut self,
        pod_sandbox_id: &str,
    ) -> Result<PodSandboxStatusResponse, tonic::Status> {
        self.grpc
            .ready()
            .await
            .map_err(|error| tonic::Status::unknown(format!("CRI service not ready: {error}")))?;
        let codec = ProstCodec::<PodSandboxStatusRequest, PodSandboxStatusResponse>::default();
        let request = Request::new(PodSandboxStatusRequest {
            pod_sandbox_id: pod_sandbox_id.to_string(),
            verbose: true,
        });
        let response = self
            .grpc
            .unary(
                request,
                PathAndQuery::from_static(POD_SANDBOX_STATUS_PATH),
                codec,
            )
            .await?;
        Ok(response.into_inner())
    }
}
