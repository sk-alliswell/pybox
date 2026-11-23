//! Shared entry point for the gateway binaries.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use axum::serve::ListenerExt;
use tracing_subscriber::EnvFilter;

use crate::config::Config;
use crate::obs::store::Backend;
use crate::obs::{Obs, ObsConfig};
use crate::App;

/// A persistent store a binary links in: the `obs.backend` name that selects
/// it and how to open it. The plain binary has none.
pub type StoreFactory = (&'static str, fn(&ObsConfig) -> Result<Arc<dyn Backend>>);

pub async fn run(bin: &str, store: Option<StoreFactory>) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let mut args = std::env::args().skip(1);
    let mut config = PathBuf::from("gateway.toml");
    while let Some(a) = args.next() {
        match a.as_str() {
            "-c" | "--config" => config = args.next().context("--config needs a path")?.into(),
            "-h" | "--help" => {
                println!("usage: {bin} [--config gateway.toml]");
                return Ok(());
            }
            other => bail!("unknown argument {other:?}"),
        }
    }
    let cfg = Config::load(&config)?;
    let addr: SocketAddr = cfg.listen.parse().context("listen address")?;
    if !addr.ip().is_loopback() {
        tracing::warn!(%addr, "listening on a non-loopback address; anyone who can reach it can spend your API keys");
    }
    if cfg.admin_token.is_none() {
        tracing::warn!("admin_token is not set; /gateway/* admin endpoints, the dashboard data and the OTLP receiver are unauthenticated");
    }
    let app = match store {
        Some((name, make)) if cfg.obs.enabled && cfg.obs.backend == name => {
            let backend = make(&cfg.obs)?;
            let obs = Obs::with_backend(cfg.obs.clone(), backend)?;
            App::with_obs(cfg, Some(obs))?
        }
        _ => App::new(cfg)?,
    };
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, upstreams = ?app.upstreams.keys().collect::<Vec<_>>(), "{bin} listening");
    if app.obs.is_some() {
        tracing::info!("dashboard at http://{addr}/gateway/dashboard");
    }
    // Small SSE writes must not sit in the kernel waiting for more data.
    let listener = listener.tap_io(|tcp| {
        let _ = tcp.set_nodelay(true);
    });
    axum::serve(
        listener,
        // Connect info gives the proxy path the client's address.
        app.router()
            .into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async {
        // A service manager stops a process with SIGTERM; treat it like
        // Ctrl-C so the log is flushed either way.
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            match signal(SignalKind::terminate()) {
                Ok(mut term) => {
                    tokio::select! {
                        _ = tokio::signal::ctrl_c() => {}
                        _ = term.recv() => {}
                    }
                }
                Err(_) => {
                    let _ = tokio::signal::ctrl_c().await;
                }
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
    })
    .await?;
    if let Some(obs) = &app.obs {
        obs.flush().await;
    }
    Ok(())
}
