use anyhow::{bail, Context, Result};
use python_sandbox_controller::{guard, platform, Cancel, Image, Lease};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
mod host_gate;

#[cfg(feature = "manager")]
mod addons;

struct Args {
    action: String,
    root: PathBuf,
    cold: bool,
    events: bool,
    timeout: Duration,
    cancel_after: Option<Duration>,
}
fn arguments() -> Result<Args> {
    let mut args = std::env::args_os().skip(1);
    let mut parsed = Args {
        action: "run".into(),
        root: std::env::current_exe()?
            .parent()
            .context("executable directory")?
            .to_owned(),
        cold: false,
        events: false,
        timeout: Duration::from_secs(15),
        cancel_after: None,
    };
    while let Some(arg) = args.next() {
        match arg.to_str().context("non-UTF8 option")? {
            "run" | "prepare" | "status" => parsed.action = arg.to_str().unwrap().to_owned(),
            "--root" => {
                parsed.root = PathBuf::from(args.next().context("--root needs a directory")?)
            }
            "--cold" => parsed.cold = true,
            "--events" => parsed.events = true,
            "--timeout-ms" => {
                let n: u64 = args
                    .next()
                    .context("timeout missing")?
                    .to_str()
                    .context("invalid timeout")?
                    .parse()?;
                if !(1..=15000).contains(&n) {
                    bail!("execution timeout must be 1..15000 ms");
                }
                parsed.timeout = Duration::from_millis(n);
            }
            "--cancel-after-ms" => {
                let n: u64 = args
                    .next()
                    .context("cancel delay missing")?
                    .to_str()
                    .context("invalid cancel delay")?
                    .parse()?;
                if n == 0 {
                    bail!("cancel delay must be positive");
                }
                parsed.cancel_after = Some(Duration::from_millis(n));
            }
            "--help" | "-h" => {
                println!("pybox [run|prepare|status] [--root DIR] [--cold] [--events] [--timeout-ms 1..15000] [--cancel-after-ms N]");
                #[cfg(feature = "manager")]
                println!("pybox manage [--root DIR] [--data DIR] [--port 8790] [--open]\npybox console | jobs | stop JOB_ID\npybox submit [--project NAME] [--cold] [--timeout-ms N] [--action run|prepare|verify]");
                println!(
                    "Native controller; no host Python, uv or Docker required. WHP/KVM required."
                );
                std::process::exit(0);
            }
            other => bail!("unknown option: {other}"),
        }
    }
    Ok(parsed)
}

fn execute(args: &Args, cancel: &Cancel) -> Result<Value> {
    let _manager_gate = host_gate::acquire_standalone()?;
    let image = Image::open(&args.root, cancel)?;
    let capability = platform::capability();
    if capability["available"] != true {
        bail!("host virtualization unavailable; no fallback: {capability}");
    }
    if image.root.join("state/unclean.json").exists() {
        bail!("unhealthy: unresolved cleanup receipt");
    }
    std::fs::create_dir_all(image.root.join("state"))?;
    if args.action == "status" {
        let present = image.root.join("cache/snapshot-ready").exists();
        if present {
            image.snapshot(cancel, false)?;
        }
        return Ok(
            json!({"passed":true,"experimental_ready":true,"snapshot_ready":present,"host_capability":capability,
            "scope":"Full artifact, snapshot, capability and cleanup checks; not a fresh guest self-test", "host_python_required":false}),
        );
    }
    let lease = Lease::acquire(&image.root.join("state/leases"))?;
    let (snapshot, prep) = if !args.cold || args.action == "prepare" {
        let (path, ms) = image.snapshot(cancel, args.events)?;
        (Some(path), ms)
    } else {
        (None, 0.0)
    };
    if args.action == "prepare" {
        return Ok(
            json!({"passed":true,"snapshot_prepare_ms":prep,"snapshot":snapshot,"host_python_required":false}),
        );
    }
    let native = image.execute(snapshot.as_deref(), args.timeout, cancel, args.events)?;
    let body: Value = serde_json::from_str(&native.stdout).unwrap_or(Value::Null);
    let passed = native.status == "completed"
        && native.resources_reclaimed
        && !native.output_truncated
        && image.accepts(&body);
    Ok(
        json!({"passed":passed,"controller":"rust","controller_version":env!("CARGO_PKG_VERSION"),
        "host_python_required":false,"mode":if args.cold {"cold"} else {"ready-snapshot"},
        "snapshot_prepare_ms":prep,"caller_e2e_ms":cancel.began.elapsed().as_secs_f64()*1000.0,
        "source_docker_image":image.manifest["image_id"],"guest_memory_mib":512,"uv_scope":"build only; absent in guest",
        "admission_slot":lease.slot,"output":body,"native_result":native}),
    )
}

fn main() {
    #[cfg(any(unix, feature = "manager"))]
    let raw: Vec<_> = std::env::args_os().skip(1).collect();
    #[cfg(unix)]
    {
        if raw.first().is_some_and(|v| v == "--__limit-exec") {
            if let Err(error) = platform::limiter(&raw[1..]) {
                eprintln!("native limiter: {error:#}");
                std::process::exit(126);
            }
            return;
        }
    }
    #[cfg(feature = "manager")]
    if let Some(action) = raw.first().and_then(|s| s.to_str()) {
        if ["manage", "console", "jobs", "submit", "stop"].contains(&action) {
            let result = if action == "manage" {
                addons::manager::run(&raw[1..])
            } else {
                addons::manager::client(action, &raw[1..])
            };
            if let Err(error) = result {
                eprintln!("{error:#}");
                std::process::exit(1);
            }
            return;
        }
    }
    let args = match arguments() {
        Ok(value) => value,
        Err(e) => {
            eprintln!("{e:#}");
            std::process::exit(2);
        }
    };
    let signal = Arc::new(AtomicBool::new(false));
    let handler = signal.clone();
    if let Err(e) = ctrlc::set_handler(move || handler.store(true, Ordering::Relaxed)) {
        eprintln!("signal handler unavailable: {e}");
        std::process::exit(2);
    }
    let cancel = Cancel {
        signal,
        began: Instant::now(),
        after: args.cancel_after,
    };
    let result = execute(&args, &cancel);
    match result {
        Ok(value) => {
            println!("{}", serde_json::to_string_pretty(&value).unwrap());
            if value["passed"] != true {
                std::process::exit(1);
            }
        }
        Err(error) => {
            if error.downcast_ref::<guard::CleanupError>().is_some() {
                let state = args.root.join("state");
                let _ = std::fs::create_dir_all(&state);
                let _ = std::fs::write(
                    state.join("unclean.json"),
                    json!({"error":error.to_string()}).to_string(),
                );
            }
            println!(
                "{}",
                json!({"passed":false,"status":if cancel.cancelled(){"cancelled"}else{"refused_or_failed"},
                "error":format!("{error:#}"),"controller":"rust","host_python_required":false})
            );
            std::process::exit(1);
        }
    }
}
