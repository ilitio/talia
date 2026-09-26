//! Export sinks: destinations for collected pipeline samples.

mod otlp;
mod stdout;

pub use otlp::OtlpSink;
pub use stdout::StdoutSink;
