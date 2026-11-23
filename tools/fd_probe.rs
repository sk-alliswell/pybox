use anyhow::Result;
#[cfg(unix)]
use python_sandbox_controller::{guard, platform, Cancel};
#[cfg(unix)]
use std::sync::{atomic::AtomicBool, Arc};
#[cfg(unix)]
use std::time::{Duration, Instant};
#[cfg(unix)]
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.first().is_some_and(|v| v == "--__limit-exec") {
        platform::limiter(&args[1..])?;
        return Ok(());
    }
    let cwd = std::env::current_dir()?;
    let mut receipts = Vec::new();
    for round in 0..10 {
        let mut joins = Vec::new();
        for i in 0..10 {
            let cwd = cwd.clone();
            joins.push(std::thread::spawn(move || {
                let cancel = Cancel {
                    signal: Arc::new(AtomicBool::new(false)),
                    began: Instant::now(),
                    after: if i % 2 == 0 {
                        Some(Duration::from_millis(25))
                    } else {
                        None
                    },
                };
                guard::run(
                    std::path::Path::new("/usr/bin/sleep"),
                    &["0.3".into()],
                    &cwd,
                    guard::Profile::Execution,
                    Duration::from_secs(2),
                    8192,
                    &cancel,
                    false,
                    "fd-probe",
                )
            }));
        }
        for join in joins {
            let receipt = join.join().unwrap()?;
            anyhow::ensure!(
                receipt.resources_reclaimed
                    && receipt
                        .policy_receipt
                        .as_ref()
                        .is_some_and(|p| p["applied"] == true),
                "bad receipt"
            );
            anyhow::ensure!(
                ["completed", "cancelled"].contains(&receipt.status.as_str()),
                "bad status"
            );
            receipts.push(receipt);
        }
        eprintln!("round {round} passed");
    }
    println!(
        "{}",
        serde_json::json!({"passed":true,"rounds":10,"native_launches":100,"concurrency":10,"receipts":receipts})
    );
    Ok(())
}

#[cfg(windows)]
fn main() -> Result<()> {
    anyhow::bail!("Linux-only descriptor inheritance probe")
}
