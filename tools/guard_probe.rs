//! Development-only guard exerciser. Not included in runtime archives.
use python_sandbox_controller::{guard, platform, Cancel};
use std::sync::{atomic::AtomicBool, Arc};
use std::time::{Duration, Instant};
fn main() {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    #[cfg(unix)]
    if args.first().is_some_and(|a| a == "--__limit-exec") {
        if let Err(e) = platform::limiter(&args[1..]) {
            eprintln!("{e:#}");
            std::process::exit(126);
        }
        return;
    }
    #[cfg(windows)]
    let _ = &platform::capability;
    assert!(
        args.len() >= 3,
        "guard-probe FIXTURE MODE TIMEOUT_MS [CANCEL_MS]"
    );
    let limit: u64 = args[2].to_str().unwrap().parse().unwrap();
    let after = args
        .get(3)
        .map(|v| Duration::from_millis(v.to_str().unwrap().parse().unwrap()));
    let cancel = Cancel {
        signal: Arc::new(AtomicBool::new(false)),
        began: Instant::now(),
        after,
    };
    let program = std::path::absolute(&args[0]).unwrap();
    let result = guard::run(
        &program,
        &args[1..2],
        program.parent().unwrap(),
        guard::Profile::Execution,
        Duration::from_millis(limit),
        8192,
        &cancel,
        false,
        "fixture",
    );
    match result {
        Ok(receipt) => println!("{}", serde_json::to_string(&receipt).unwrap()),
        Err(e) => {
            eprintln!("{e:#}");
            std::process::exit(1);
        }
    }
}
