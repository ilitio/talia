use tracing_subscriber::EnvFilter;

pub(crate) fn init(log_filter: &str) {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(log_filter))
        .init();
}
