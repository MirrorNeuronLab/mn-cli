//! Native MirrorNeuron command line.
mod app;
mod blueprints;
mod client;
mod config;
mod display;
mod error;
mod input;
mod jobs;
mod nodes;
mod operations;
mod output;
mod runs;
mod runtime;

pub use app::run;

async fn interrupt() {
    #[cfg(not(target_os = "wasi"))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
    #[cfg(target_os = "wasi")]
    {
        let Some(dir) = std::env::var_os("MN_WASI_HOST_DIR") else {
            return std::future::pending().await;
        };
        let path = std::path::PathBuf::from(dir).join("interrupt");
        while !path.is_file() {
            tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        }
    }
}
