use crate::{platform, Cancel};
use anyhow::Result;
use serde::Serialize;
use serde_json::Value;
use std::ffi::OsString;
use std::io::Read;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug)]
pub enum Profile {
    Execution,
    Preparation,
}
impl Profile {
    pub fn windows_commit_mib(self) -> usize {
        match self {
            Self::Execution => 384,
            Self::Preparation => 1536,
        }
    }
    pub fn linux_as_bytes(self) -> u64 {
        match self {
            Self::Execution => 1024u64.pow(3),
            Self::Preparation => 3 * 1024u64.pow(3),
        }
    }
    pub fn linux_cpu_seconds(self) -> u64 {
        match self {
            Self::Execution => 5,
            Self::Preparation => 30,
        }
    }
    pub fn description(self) -> String {
        if cfg!(windows) {
            format!(
                "Windows Job at birth: {} MiB commit, 1 process, 1 CPU, kill-on-close",
                self.windows_commit_mib()
            )
        } else {
            format!("Linux native exec limiter: CPU {}s, AS {} bytes, 1 CPU, session/group, parent-death SIGKILL",self.linux_cpu_seconds(),self.linux_as_bytes())
        }
    }
}

#[derive(Debug)]
pub struct CleanupError(pub String);
impl std::fmt::Display for CleanupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "cleanup uncertainty: {}", self.0)
    }
}
impl std::error::Error for CleanupError {}

#[derive(Serialize, Debug)]
pub struct Receipt {
    pub status: String,
    pub exit_code: Option<i64>,
    pub process_id: u32,
    pub stdout: String,
    pub stderr: String,
    pub captured_bytes: [usize; 2],
    pub observed_output_bytes: [u64; 2],
    pub output_truncated: bool,
    pub resources_reclaimed: bool,
    pub e2e_ms: f64,
    pub host_spawn_ms: f64,
    pub host_policy_ready_ms: f64,
    pub policy_receipt: Option<Value>,
    pub process_metrics: Value,
    pub host_guard: String,
    pub cleanup_receipt: Value,
    pub error: Option<String>,
}

struct Capture {
    data: Vec<u8>,
    observed: u64,
    error: Option<String>,
}
pub type Observer = Arc<dyn Fn(&[u8]) + Send + Sync>;
/// Optional per-worker telemetry. It does not change execution policy.
#[derive(Clone)]
pub struct Monitor {
    pub spawned: Arc<dyn Fn(u32, &str) + Send + Sync>,
    pub output: Arc<dyn Fn(&str, &[u8]) + Send + Sync>,
    pub reaped: Arc<dyn Fn(&Receipt) + Send + Sync>,
}
thread_local! {
    static MONITOR: std::cell::RefCell<Option<Monitor>> = const { std::cell::RefCell::new(None) };
}
pub fn set_monitor(monitor: Option<Monitor>) {
    MONITOR.with(|slot| *slot.borrow_mut() = monitor);
}
fn capture(
    mut stream: Box<dyn Read + Send>,
    cap: usize,
    overflow: Arc<AtomicBool>,
    observer: Option<Observer>,
) -> thread::JoinHandle<Capture> {
    thread::spawn(move || {
        let mut result = Capture {
            data: Vec::new(),
            observed: 0,
            error: None,
        };
        let mut chunk = [0u8; 4096];
        loop {
            match stream.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => {
                    if let Some(ref observe) = observer {
                        observe(&chunk[..n]);
                    }
                    result.observed = result.observed.saturating_add(n as u64);
                    let keep = n.min(cap.saturating_sub(result.data.len()));
                    result.data.extend_from_slice(&chunk[..keep]);
                    if result.observed > cap as u64 {
                        overflow.store(true, Ordering::Relaxed);
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => {
                    result.error = Some(e.to_string());
                    break;
                }
            }
        }
        result
    })
}

#[allow(clippy::too_many_arguments)]
pub fn run(
    program: &Path,
    args: &[OsString],
    cwd: &Path,
    profile: Profile,
    timeout: Duration,
    cap: usize,
    cancel: &Cancel,
    events: bool,
    phase: &str,
) -> Result<Receipt> {
    run_streaming(
        program, args, cwd, profile, timeout, cap, cancel, events, phase, None,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn run_streaming(
    program: &Path,
    args: &[OsString],
    cwd: &Path,
    profile: Profile,
    timeout: Duration,
    cap: usize,
    cancel: &Cancel,
    events: bool,
    phase: &str,
    observer: Option<Observer>,
) -> Result<Receipt> {
    let start = Instant::now();
    if cancel.cancelled() {
        anyhow::bail!("cancelled before native launch");
    }
    let mut child =
        platform::NativeProcess::spawn(program, args, cwd, profile, &crate::environment())?;
    let spawn_ms = start.elapsed().as_secs_f64() * 1000.0;
    let pid = child.pid;
    let monitor = MONITOR.with(|slot| slot.borrow().clone());
    if let Some(m) = &monitor {
        (m.spawned)(pid, phase);
    }
    if events {
        eprintln!(
            "{}",
            serde_json::json!({"event":"native_spawned","pid":pid,"phase":phase})
        );
    }
    let overflow = Arc::new(AtomicBool::new(false));
    let out_monitor = monitor.clone();
    let observed: Option<Observer> = Some(Arc::new(move |bytes| {
        if let Some(m) = &out_monitor {
            (m.output)("stdout", bytes);
        }
        if let Some(o) = &observer {
            o(bytes);
        }
    }));
    let err_monitor = monitor.clone();
    let err_observed: Option<Observer> = Some(Arc::new(move |bytes| {
        if let Some(m) = &err_monitor {
            (m.output)("stderr", bytes);
        }
    }));
    let stdout = capture(
        child.stdout.take().unwrap(),
        cap,
        overflow.clone(),
        observed,
    );
    let stderr = capture(
        child.stderr.take().unwrap(),
        cap,
        overflow.clone(),
        err_observed,
    );
    let policy_overflow = Arc::new(AtomicBool::new(false));
    let policy_reader = child
        .receipt
        .take()
        .map(|r| capture(r, 4096, policy_overflow.clone(), None));
    let mut cause = None;
    let mut error = None;
    if cancel.cancelled() {
        cause = Some("cancelled".to_owned());
    } else if start.elapsed() >= timeout {
        cause = Some("timeout".to_owned());
    } else if let Err(e) = child.ready() {
        cause = Some("policy_error".to_owned());
        error = Some(e.to_string());
    }
    let ready_ms = start.elapsed().as_secs_f64() * 1000.0;
    let mut code = None;
    while cause.is_none() {
        if overflow.load(Ordering::Relaxed) {
            cause = Some("output_limit".into());
            break;
        }
        if cancel.cancelled() {
            cause = Some("cancelled".into());
            break;
        }
        if start.elapsed() >= timeout {
            cause = Some("timeout".into());
            break;
        }
        match child.poll() {
            Ok(Some(value)) => {
                code = Some(value);
                break;
            }
            Ok(None) => {}
            Err(e) => {
                cause = Some("runtime_error".into());
                error = Some(e.to_string());
                break;
            }
        }
        thread::sleep(Duration::from_millis(2));
    }
    if cause.is_some() {
        if let Err(e) = child.kill() {
            error = Some(e.to_string());
        }
    }
    let deadline = Instant::now() + Duration::from_secs(2);
    while code.is_none() && Instant::now() < deadline {
        match child.poll() {
            Ok(v) => code = v,
            Err(e) => {
                error = Some(e.to_string());
                break;
            }
        }
        if code.is_none() {
            thread::sleep(Duration::from_millis(2));
        }
    }
    let group_clean = child.cleanup_group().is_ok();
    let drain = Instant::now() + Duration::from_secs(2);
    while (!stdout.is_finished()
        || !stderr.is_finished()
        || policy_reader.as_ref().is_some_and(|r| !r.is_finished()))
        && Instant::now() < drain
    {
        thread::sleep(Duration::from_millis(2));
    }
    if code.is_none()
        || !group_clean
        || !stdout.is_finished()
        || !stderr.is_finished()
        || policy_reader.as_ref().is_some_and(|r| !r.is_finished())
    {
        return Err(CleanupError(format!(
            "pid={pid}, exited={}, group_clean={group_clean}, pipes_drained={}, policy_drained={}",
            code.is_some(),
            stdout.is_finished() && stderr.is_finished(),
            policy_reader.as_ref().is_none_or(|r| r.is_finished())
        ))
        .into());
    }
    let out = stdout
        .join()
        .map_err(|_| CleanupError("stdout reader panicked".into()))?;
    let err = stderr
        .join()
        .map_err(|_| CleanupError("stderr reader panicked".into()))?;
    let policy = if let Some(reader) = policy_reader {
        let result = reader
            .join()
            .map_err(|_| CleanupError("policy reader panicked".into()))?;
        if result.error.is_some() || policy_overflow.load(Ordering::Relaxed) {
            None
        } else {
            serde_json::from_slice::<Value>(&result.data).ok()
        }
    } else {
        child.policy()
    };
    if out.error.is_some() || err.error.is_some() {
        cause = Some("capture_error".into());
        error = out.error.or(err.error);
    }
    if overflow.load(Ordering::Relaxed) {
        cause = Some("output_limit".into());
    }
    if cause.is_none()
        && policy
            .as_ref()
            .and_then(|p| p.get("applied"))
            .and_then(Value::as_bool)
            != Some(true)
    {
        cause = Some("policy_error".into());
    }
    let metrics = child.metrics();
    let receipt = Receipt {
        status: cause.unwrap_or_else(|| {
            if code == Some(0) {
                "completed".into()
            } else {
                "runtime_error".into()
            }
        }),
        exit_code: code,
        process_id: pid,
        stdout: String::from_utf8_lossy(&out.data).into_owned(),
        stderr: String::from_utf8_lossy(&err.data).into_owned(),
        captured_bytes: [out.data.len(), err.data.len()],
        observed_output_bytes: [out.observed, err.observed],
        output_truncated: overflow.load(Ordering::Relaxed),
        resources_reclaimed: true,
        e2e_ms: start.elapsed().as_secs_f64() * 1000.0,
        host_spawn_ms: spawn_ms,
        host_policy_ready_ms: ready_ms,
        policy_receipt: policy,
        process_metrics: metrics,
        host_guard: profile.description(),
        cleanup_receipt: serde_json::json!({"parent_exit_verified":true,"readers_drained":true,
            "job_associated_at_birth":cfg!(windows),"process_group_terminated":cfg!(unix)}),
        error,
    };
    if let Some(m) = monitor {
        (m.reaped)(&receipt);
    }
    Ok(receipt)
}
