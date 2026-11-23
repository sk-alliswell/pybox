//! gwsim: a discrete-event simulator for the gateway stack.
//!
//! Describe the components (gateway, LiteLLM-style proxies, model servers,
//! hosted APIs, MCP tool servers, the log store), the hosts they share and
//! the clients that send load; run it; read latency percentiles, throughput,
//! queues and where the bottleneck is. See `README.md` for the model and
//! `VALIDATION.md` for how its predictions compare with measured runs.

pub mod calibrate;
#[cfg(not(target_arch = "wasm32"))]
pub mod load;
pub mod metrics;
pub mod replay;
pub mod report;
pub mod rng;
pub mod scenario;
pub mod sched;
pub mod sim;
pub mod tune;
pub mod usage;
pub mod validate;
#[cfg(target_arch = "wasm32")]
pub mod wasm;

pub use report::{Record, Report};
pub use scenario::Scenario;
pub use sim::Sim;

/// Run a scenario to the end and summarise it.
pub fn run(sc: Scenario) -> Result<Report, String> {
    let mut sim = Sim::new(sc)?;
    sim.run();
    Ok(Report::from_sim(&sim))
}
