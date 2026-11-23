//! Local, single-user control plane for the qualified sandbox workload.
use anyhow::{bail, Context, Result};
use fs2::FileExt;
use python_sandbox_controller::{guard, platform, Cancel, Image, Lease};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const MAX_RECORDS: usize = 500;
const MAX_QUEUED: usize = 32;
const LOG_BYTES: usize = 16384;
const HTML: &str = include_str!("../console/index.html");
const CSS: &str = include_str!("../console/style.css");
const JS: &str = include_str!("../console/app.js");

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
fn random_id() -> Result<String> {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).map_err(|e| anyhow::anyhow!("random source: {e}"))?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}
fn home() -> Result<PathBuf> {
    Ok(PathBuf::from(
        std::env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" })
            .context("user home is unavailable")?,
    )
    .join(".pybox"))
}
fn private_directory(path: &Path) -> Result<()> {
    fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}
pub fn offline_gate() -> Result<File> {
    gate(false)
}
fn gate(exclusive: bool) -> Result<File> {
    let folder = home()?;
    private_directory(&folder)?;
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(folder.join("manager.lock"))?;
    if exclusive {
        FileExt::try_lock_exclusive(&f)
            .context("another manager or standalone CLI is active for this user")?;
    } else {
        FileExt::try_lock_shared(&f)
            .context("manager is active; use pybox submit, jobs or stop")?;
    }
    Ok(f)
}
fn save(path: &Path, value: &impl Serialize) -> Result<()> {
    let temp = path.with_extension("pending");
    let mut file = File::create(&temp)?;
    file.write_all(&serde_json::to_vec_pretty(value)?)?;
    file.sync_all()?;
    drop(file);
    fs::rename(temp, path)?;
    Ok(())
}
fn read<T: for<'a> Deserialize<'a>>(path: &Path) -> Result<T> {
    anyhow::ensure!(
        fs::metadata(path)?.len() <= 1024 * 1024,
        "oversized manager record"
    );
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
struct Settings {
    max_concurrent: usize,
    timeout_ms: u64,
    cold: bool,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            max_concurrent: 2,
            timeout_ms: 15000,
            cold: false,
        }
    }
}
impl Settings {
    fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            (1..=2).contains(&self.max_concurrent),
            "concurrency must be 1..2"
        );
        anyhow::ensure!(
            (1..=15000).contains(&self.timeout_ms),
            "timeout must be 1..15000 ms"
        );
        Ok(())
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Submission {
    project: String,
    #[serde(default = "run_action")]
    action: String,
    timeout_ms: Option<u64>,
    cold: Option<bool>,
}
fn run_action() -> String {
    "run".into()
}
#[derive(Clone, Serialize, Deserialize)]
struct Applied {
    timeout_ms: u64,
    cold: bool,
    guest_memory_mib: u64,
    output_limit_bytes_per_stream: usize,
    network: String,
    image_id: String,
    rootfs_sha256: String,
    host_execution_policy: String,
}
#[derive(Clone, Serialize, Deserialize)]
struct Record {
    id: String,
    project: String,
    action: String,
    state: String,
    created_at: u64,
    started_at: Option<u64>,
    finished_at: Option<u64>,
    pid: Option<u32>,
    last_pid: Option<u32>,
    cancel_requested: bool,
    cleanup_verified: Option<bool>,
    config: Applied,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    logs_truncated: bool,
    events: Vec<Value>,
    result: Option<Value>,
    error: Option<String>,
}
impl Record {
    fn terminal(&self) -> bool {
        matches!(
            self.state.as_str(),
            "completed" | "failed" | "cancelled" | "interrupted"
        )
    }
    fn public(&self, detail: bool) -> Value {
        let mut value = serde_json::to_value(self).unwrap();
        let map = value.as_object_mut().unwrap();
        map.remove("stdout");
        map.remove("stderr");
        if detail {
            map.insert(
                "stdout".into(),
                json!(String::from_utf8_lossy(&self.stdout)),
            );
            map.insert(
                "stderr".into(),
                json!(String::from_utf8_lossy(&self.stderr)),
            );
        } else {
            map.remove("events");
            map.remove("result");
        }
        value
    }
}
struct State {
    settings: Settings,
    jobs: BTreeMap<String, Record>,
    signals: BTreeMap<String, Arc<AtomicBool>>,
    unhealthy: Option<String>,
    image: Value,
    last_verified_at: Option<u64>,
}
struct Manager {
    root: PathBuf,
    data: PathBuf,
    state: Mutex<State>,
    preparation: Mutex<()>,
    shutdown: Arc<AtomicBool>,
}
impl Manager {
    fn load(root: PathBuf, data: PathBuf, shutdown: Arc<AtomicBool>) -> Result<Self> {
        private_directory(&data)?;
        private_directory(&data.join("jobs"))?;
        let settings = if data.join("settings.json").exists() {
            read(&data.join("settings.json"))?
        } else {
            Settings::default()
        };
        settings.validate()?;
        let mut jobs = BTreeMap::new();
        let mut unhealthy = None;
        for entry in fs::read_dir(data.join("jobs"))? {
            let path = entry?.path();
            if path.extension().is_some_and(|e| e == "json") {
                let mut record: Record = read(&path)?;
                anyhow::ensure!(
                    path.file_stem().and_then(|s| s.to_str()) == Some(&record.id),
                    "record ID mismatch"
                );
                if !record.terminal() {
                    if record.state == "queued" {
                        record.state = "cancelled".into();
                        record.cleanup_verified = Some(true);
                        record.error = Some(
                            "manager restarted before dispatch; not automatically replayed".into(),
                        );
                    } else {
                        record.state = "interrupted".into();
                        record.cleanup_verified = Some(false);
                        record.error = Some(
                            "manager interrupted; cleanup requires operator investigation".into(),
                        );
                    }
                    record.pid = None;
                    record.finished_at = Some(now());
                    save(&path, &record)?;
                }
                if record.cleanup_verified == Some(false) {
                    unhealthy =
                        Some("unverified cleanup in retained history; launches blocked".into());
                }
                jobs.insert(record.id.clone(), record);
            }
        }
        anyhow::ensure!(
            jobs.len() <= MAX_RECORDS,
            "manager history exceeds retention bound"
        );
        let manifest: Value = read(&root.join("image-manifest.json")).unwrap_or(Value::Null);
        let image = json!({"id":"default","label":"Python agent runtime","manifest":manifest,
            "bundle_path":root,"integrity":"not_checked","snapshot_present":root.join("cache/snapshot-ready").exists()});
        Ok(Self {
            root,
            data,
            state: Mutex::new(State {
                settings,
                jobs,
                signals: BTreeMap::new(),
                unhealthy,
                image,
                last_verified_at: None,
            }),
            preparation: Mutex::new(()),
            shutdown,
        })
    }
    fn edit(&self, id: &str, f: impl FnOnce(&mut Record)) {
        let mut state = self.state.lock().unwrap();
        if let Some(job) = state.jobs.get_mut(id) {
            f(job);
            if let Err(error) = save(&self.data.join("jobs").join(format!("{id}.json")), job) {
                state.unhealthy = Some(format!("history persistence failed: {error}"));
                for signal in state.signals.values() {
                    signal.store(true, Ordering::Relaxed);
                }
            }
        }
    }
    fn submit(&self, request: Submission) -> Result<Value> {
        anyhow::ensure!(
            !self.shutdown.load(Ordering::Relaxed),
            "manager is shutting down"
        );
        anyhow::ensure!(
            !request.project.trim().is_empty()
                && request.project.len() <= 120
                && !request.project.chars().any(char::is_control),
            "project must be 1..120 bytes without control characters"
        );
        anyhow::ensure!(
            ["run", "prepare", "verify"].contains(&request.action.as_str()),
            "unsupported action"
        );
        let mut state = self.state.lock().unwrap();
        anyhow::ensure!(
            !self.shutdown.load(Ordering::Relaxed),
            "manager is shutting down"
        );
        anyhow::ensure!(
            state.unhealthy.is_none(),
            "manager unhealthy: {}",
            state.unhealthy.as_deref().unwrap_or("")
        );
        anyhow::ensure!(
            !self.root.join("state/unclean.json").exists(),
            "bundle has an unresolved cleanup receipt"
        );
        anyhow::ensure!(
            state.jobs.len() < MAX_RECORDS,
            "500 retained records reached; archive history while manager is stopped"
        );
        anyhow::ensure!(
            state.jobs.values().filter(|j| j.state == "queued").count() < MAX_QUEUED,
            "queue full (32)"
        );
        let timeout = request.timeout_ms.unwrap_or(state.settings.timeout_ms);
        anyhow::ensure!(
            (1..=15000).contains(&timeout),
            "timeout must be 1..15000 ms"
        );
        let id = format!("{}-{}", now(), &random_id()?[..12]);
        let job = Record {
            id: id.clone(),
            project: request.project.trim().into(),
            action: request.action,
            state: "queued".into(),
            created_at: now(),
            started_at: None,
            finished_at: None,
            pid: None,
            last_pid: None,
            cancel_requested: false,
            cleanup_verified: None,
            config: Applied {
                timeout_ms: timeout,
                cold: request.cold.unwrap_or(state.settings.cold),
                guest_memory_mib: 512,
                output_limit_bytes_per_stream: 8192,
                network: "disabled".into(),
                image_id: state.image["manifest"]["image_id"]
                    .as_str()
                    .unwrap_or("unverified")
                    .into(),
                rootfs_sha256: state.image["manifest"]["rootfs_sha256"]
                    .as_str()
                    .unwrap_or("unverified")
                    .into(),
                host_execution_policy: guard::Profile::Execution.description(),
            },
            stdout: Vec::new(),
            stderr: Vec::new(),
            logs_truncated: false,
            events: Vec::new(),
            result: None,
            error: None,
        };
        save(&self.data.join("jobs").join(format!("{id}.json")), &job)?;
        let response = job.public(true);
        state.jobs.insert(id, job);
        Ok(response)
    }
    fn stop(&self, id: &str) -> Result<Value> {
        let mut state = self.state.lock().unwrap();
        let signal = state.signals.get(id).cloned();
        let job = state.jobs.get_mut(id).context("job not found")?;
        if !job.terminal() {
            job.cancel_requested = true;
            if job.state == "queued" {
                job.state = "cancelled".into();
                job.finished_at = Some(now());
                job.cleanup_verified = Some(true);
            } else {
                job.state = "stopping".into();
            }
            if let Some(signal) = signal {
                signal.store(true, Ordering::Relaxed);
            }
            save(&self.data.join("jobs").join(format!("{id}.json")), job)?;
        }
        Ok(job.public(true))
    }
    fn snapshot(&self) -> Value {
        let state = self.state.lock().unwrap();
        let mut jobs: Vec<_> = state.jobs.values().collect();
        jobs.sort_by_key(|j| std::cmp::Reverse(j.created_at));
        let count = |name: &str| jobs.iter().filter(|j| j.state == name).count();
        let live = jobs
            .iter()
            .filter(|j| j.pid.is_some() && !j.terminal())
            .count();
        let active = jobs
            .iter()
            .filter(|j| !j.terminal() && j.state != "queued")
            .count();
        let mut image = state.image.clone();
        image["snapshot_present"] = json!(self.root.join("cache/snapshot-ready").exists());
        image["last_verified_at"] = json!(state.last_verified_at);
        json!({"version":env!("CARGO_PKG_VERSION"), "manager_pid":std::process::id(),
            "scope":"current user's manager-owned jobs; legacy controllers and other users are not inventoried",
            "counts":{"live_vms":live,"active":active,"queued":count("queued"),"completed":count("completed"),
                "failed":count("failed")+count("interrupted"),"cancelled":count("cancelled"),"total":jobs.len()},
            "settings":state.settings,"unhealthy":state.unhealthy,
            "bundle_cleanup_blocked":self.root.join("state/unclean.json").exists(),
            "capability":platform::capability(),"image":image,"history_limit":MAX_RECORDS,
            "jobs":jobs.iter().map(|j| j.public(false)).collect::<Vec<_>>()})
    }
    fn schedule(self: Arc<Self>) {
        loop {
            let shutting_down = self.shutdown.load(Ordering::Relaxed);
            let mut dispatch = None;
            {
                let mut state = self.state.lock().unwrap();
                if shutting_down || state.unhealthy.is_some() {
                    for signal in state.signals.values() {
                        signal.store(true, Ordering::Relaxed);
                    }
                }
                if shutting_down {
                    let ids: Vec<_> = state
                        .jobs
                        .values()
                        .filter(|j| j.state == "queued")
                        .map(|j| j.id.clone())
                        .collect();
                    drop(state);
                    for id in ids {
                        let _ = self.stop(&id);
                    }
                    if self.state.lock().unwrap().signals.is_empty() {
                        break;
                    }
                } else if state.unhealthy.is_none()
                    && state.signals.len() < state.settings.max_concurrent
                {
                    let next = state
                        .jobs
                        .values()
                        .filter(|j| j.state == "queued")
                        .min_by_key(|j| j.created_at)
                        .map(|j| j.id.clone());
                    if let Some(id) = next {
                        let signal = Arc::new(AtomicBool::new(false));
                        let job = state.jobs.get_mut(&id).unwrap();
                        job.state = "verifying".into();
                        job.started_at = Some(now());
                        match save(&self.data.join("jobs").join(format!("{id}.json")), job) {
                            Ok(()) => {
                                state.signals.insert(id.clone(), signal.clone());
                                dispatch = Some((id, signal));
                            }
                            Err(error) => {
                                state.unhealthy =
                                    Some(format!("dispatch persistence failed: {error}"))
                            }
                        }
                    }
                }
            }
            if let Some((id, signal)) = dispatch {
                let manager = self.clone();
                thread::spawn(move || {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        manager.execute(&id, signal.clone())
                    }));
                    match result {
                        Ok(Ok(value)) => manager.edit(&id, |j| {
                            let status = value["native_result"]["status"].as_str();
                            j.state = if value["passed"] == true {
                                "completed"
                            } else if status == Some("cancelled") {
                                "cancelled"
                            } else {
                                "failed"
                            }
                            .into();
                            j.result = Some(value);
                            j.finished_at = Some(now());
                            j.pid = None;
                            j.cleanup_verified = Some(true);
                        }),
                        Ok(Err(error)) => {
                            let cleanup_error =
                                error.downcast_ref::<guard::CleanupError>().is_some();
                            manager.edit(&id, |j| {
                                let uncertain = cleanup_error || j.pid.is_some();
                                j.state = if signal.load(Ordering::Relaxed) && !uncertain {
                                    "cancelled"
                                } else {
                                    "failed"
                                }
                                .into();
                                j.error = Some(format!("{error:#}"));
                                j.finished_at = Some(now());
                                j.cleanup_verified = Some(!uncertain);
                                j.pid = None;
                            });
                        }
                        Err(_) => manager.edit(&id, |j| {
                            j.state = "interrupted".into();
                            j.error = Some("worker panic; cleanup unverified".into());
                            j.cleanup_verified = Some(false);
                            j.finished_at = Some(now());
                            j.pid = None;
                        }),
                    }
                    guard::set_monitor(None);
                    let mut state = manager.state.lock().unwrap();
                    if state.jobs[&id].cleanup_verified == Some(false) {
                        state.unhealthy =
                            Some("worker cleanup unverified; launches blocked".into());
                        let _ = save(
                            &manager.root.join("state/unclean.json"),
                            &json!({"manager_job":id,"cleanup_verified":false}),
                        );
                    }
                    state.signals.remove(&id);
                });
            }
            thread::sleep(Duration::from_millis(30));
        }
    }
    fn execute(self: &Arc<Self>, id: &str, signal: Arc<AtomicBool>) -> Result<Value> {
        let job = self.state.lock().unwrap().jobs[id].clone();
        let manager = self.clone();
        let owned_id = id.to_string();
        let out_manager = self.clone();
        let out_id = id.to_string();
        let end_manager = self.clone();
        let end_id = id.to_string();
        guard::set_monitor(Some(guard::Monitor {
            spawned: Arc::new(move |pid, phase| {
                manager.edit(&owned_id, |j| {
                    j.pid = Some(pid);
                    j.last_pid = Some(pid);
                    j.cleanup_verified = None;
                    if !j.cancel_requested {
                        j.state = if phase == "prepare" {
                            "preparing"
                        } else {
                            "running"
                        }
                        .into();
                    }
                    j.events
                        .push(json!({"at":now(),"event":"native_spawned","phase":phase,"pid":pid}));
                })
            }),
            output: Arc::new(move |stream, bytes| {
                out_manager.edit(&out_id, |j| {
                    let target = if stream == "stdout" {
                        &mut j.stdout
                    } else {
                        &mut j.stderr
                    };
                    let count = bytes.len().min(LOG_BYTES.saturating_sub(target.len()));
                    target.extend_from_slice(&bytes[..count]);
                    j.logs_truncated |= count < bytes.len();
                })
            }),
            reaped: Arc::new(move |receipt| {
                end_manager.edit(&end_id, |j| {
                    j.pid = None;
                    j.cleanup_verified = Some(receipt.resources_reclaimed);
                    j.events.push(
                        json!({"at":now(),"event":"native_reaped","pid":receipt.process_id,
                    "status":receipt.status,"cleanup":receipt.cleanup_receipt}),
                    );
                })
            }),
        }));
        let cancel = Cancel {
            signal,
            began: Instant::now(),
            after: None,
        };
        let image = Image::open(&self.root, &cancel)?;
        anyhow::ensure!(
            image.manifest["image_id"] == job.config.image_id
                && image.manifest["rootfs_sha256"] == job.config.rootfs_sha256,
            "image changed since submission; restart manager to register new image"
        );
        anyhow::ensure!(
            platform::capability()["available"] == true,
            "host virtualization unavailable; no fallback"
        );
        anyhow::ensure!(
            !self.root.join("state/unclean.json").exists(),
            "unresolved bundle cleanup receipt"
        );
        fs::create_dir_all(self.root.join("state"))?;
        {
            let mut state = self.state.lock().unwrap();
            state.image["integrity"] = json!("verified");
            state.last_verified_at = Some(now());
        }
        if cancel.cancelled() {
            bail!("cancelled before execution");
        }
        let _lease = Lease::acquire(&self.root.join("state/leases"))?;
        let needs_snapshot = job.action == "prepare"
            || (job.action == "run" && !job.config.cold)
            || (job.action == "verify" && self.root.join("cache/snapshot-ready").exists());
        let (snapshot, preparation_ms) = if needs_snapshot {
            self.edit(id, |j| {
                if !j.cancel_requested {
                    j.state = "preparing".into();
                }
            });
            let _preparation = self.preparation.lock().unwrap();
            if cancel.cancelled() {
                bail!("cancelled while waiting for preparation");
            }
            let (path, elapsed) = image.snapshot(&cancel, false)?;
            (Some(path), elapsed)
        } else {
            (None, 0.0)
        };
        if cancel.cancelled() {
            bail!("cancelled before workload");
        }
        if job.action != "run" {
            return Ok(
                json!({"passed":true,"action":job.action,"snapshot_prepare_ms":preparation_ms,
                "snapshot_present":self.root.join("cache/snapshot-ready").exists(),"scope":"artifact/capability/snapshot verification, not a workload run"}),
            );
        }
        self.edit(id, |j| {
            if !j.cancel_requested {
                j.state = "starting".into();
            }
        });
        let receipt = image.execute(
            snapshot.as_deref(),
            Duration::from_millis(job.config.timeout_ms),
            &cancel,
            false,
        )?;
        let body: Value = serde_json::from_str(&receipt.stdout).unwrap_or(Value::Null);
        let passed = receipt.status == "completed"
            && receipt.resources_reclaimed
            && !receipt.output_truncated
            && image.accepts(&body);
        Ok(
            json!({"passed":passed,"output":body,"snapshot_prepare_ms":preparation_ms,"native_result":receipt}),
        )
    }
}

struct Request {
    method: String,
    path: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}
fn request(stream: &mut TcpStream) -> Result<Request> {
    // Windows accepted sockets can inherit the listener's nonblocking mode.
    // Each bounded connection worker uses blocking I/O with explicit deadlines.
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(Duration::from_secs(3)))?;
    let mut data = Vec::new();
    let mut buffer = [0u8; 2048];
    let split = loop {
        let n = stream.read(&mut buffer)?;
        anyhow::ensure!(n > 0, "incomplete HTTP request");
        data.extend_from_slice(&buffer[..n]);
        if let Some(end) = data.windows(4).position(|s| s == b"\r\n\r\n") {
            break end + 4;
        }
        anyhow::ensure!(data.len() <= 16384, "headers too large");
    };
    anyhow::ensure!(split <= 16384, "headers too large");
    let text = std::str::from_utf8(&data[..split])?;
    let mut lines = text.split("\r\n");
    let mut first = lines
        .next()
        .context("missing request line")?
        .split_whitespace();
    let method = first.next().context("missing method")?.to_owned();
    let path = first.next().context("missing path")?.to_owned();
    anyhow::ensure!(
        first.next() == Some("HTTP/1.1") && first.next().is_none(),
        "invalid HTTP version"
    );
    let mut headers = BTreeMap::new();
    for line in lines.filter(|s| !s.is_empty()) {
        let (key, value) = line.split_once(':').context("malformed header")?;
        anyhow::ensure!(
            headers
                .insert(key.to_ascii_lowercase(), value.trim().to_owned())
                .is_none(),
            "duplicate header"
        );
    }
    anyhow::ensure!(
        !headers.contains_key("transfer-encoding"),
        "transfer encoding unsupported"
    );
    let len: usize = headers
        .get("content-length")
        .map(|s| s.parse())
        .transpose()?
        .unwrap_or(0);
    anyhow::ensure!(len <= 8192, "body too large");
    while data.len() < split + len {
        let n = stream.read(&mut buffer)?;
        anyhow::ensure!(n > 0, "truncated body");
        data.extend_from_slice(&buffer[..n]);
    }
    Ok(Request {
        method,
        path,
        headers,
        body: data[split..split + len].to_vec(),
    })
}
fn response(stream: &mut TcpStream, status: u16, mime: &str, body: &[u8]) -> Result<()> {
    stream.set_write_timeout(Some(Duration::from_secs(3)))?;
    let label = if status < 400 { "OK" } else { "Error" };
    write!(stream, "HTTP/1.1 {status} {label}\r\nContent-Type: {mime}\r\nContent-Length: {}\r\nConnection: close\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nReferrer-Policy: no-referrer\r\nContent-Security-Policy: default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; frame-ancestors 'none'; form-action 'self'\r\n\r\n", body.len())?;
    stream.write_all(body)?;
    Ok(())
}
fn json_response(stream: &mut TcpStream, status: u16, body: Value) {
    let _ = response(
        stream,
        status,
        "application/json; charset=utf-8",
        body.to_string().as_bytes(),
    );
}
fn route(manager: &Arc<Manager>, req: &Request) -> Result<Value> {
    match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/api/overview") | ("GET", "/api/jobs") => Ok(manager.snapshot()),
        ("GET", "/api/settings") => Ok(serde_json::to_value(
            &manager.state.lock().unwrap().settings,
        )?),
        ("POST", "/api/settings") => {
            let value: Settings = serde_json::from_slice(&req.body)?;
            value.validate()?;
            let mut state = manager.state.lock().unwrap();
            save(&manager.data.join("settings.json"), &value)?;
            state.settings = value;
            Ok(serde_json::to_value(&state.settings)?)
        }
        ("POST", "/api/jobs") => manager.submit(serde_json::from_slice(&req.body)?),
        ("POST", "/api/stop-all") => {
            let ids: Vec<_> = manager
                .state
                .lock()
                .unwrap()
                .jobs
                .values()
                .filter(|j| !j.terminal())
                .map(|j| j.id.clone())
                .collect();
            for id in &ids {
                manager.stop(id)?;
            }
            Ok(json!({"requested":ids.len()}))
        }
        ("POST", "/api/shutdown") => {
            manager.shutdown.store(true, Ordering::Relaxed);
            Ok(json!({"stopping":true}))
        }
        _ => {
            if let Some(tail) = req.path.strip_prefix("/api/jobs/") {
                if let Some(id) = tail.strip_suffix("/stop") {
                    if req.method == "POST" {
                        return manager.stop(id);
                    }
                } else if req.method == "GET" {
                    return Ok(manager
                        .state
                        .lock()
                        .unwrap()
                        .jobs
                        .get(tail)
                        .context("job not found")?
                        .public(true));
                }
            }
            bail!("unknown route")
        }
    }
}
fn serve_connection(mut stream: TcpStream, manager: Arc<Manager>, authority: &str, token: &str) {
    let req = match request(&mut stream) {
        Ok(req) => req,
        Err(_) => {
            json_response(&mut stream, 400, json!({"error":"invalid HTTP request"}));
            return;
        }
    };
    if req.headers.get("host").map(String::as_str) != Some(authority)
        || req
            .headers
            .get("origin")
            .is_some_and(|o| o != &format!("http://{authority}"))
    {
        json_response(
            &mut stream,
            403,
            json!({"error":"loopback origin required"}),
        );
        return;
    }
    if req.method == "GET" {
        let asset = match req.path.as_str() {
            "/" => Some(("text/html; charset=utf-8", HTML)),
            "/style.css" => Some(("text/css; charset=utf-8", CSS)),
            "/app.js" => Some(("text/javascript; charset=utf-8", JS)),
            _ => None,
        };
        if let Some((mime, text)) = asset {
            let _ = response(&mut stream, 200, mime, text.as_bytes());
            return;
        }
    }
    if req.headers.get("authorization") != Some(&format!("Bearer {token}")) {
        json_response(
            &mut stream,
            401,
            json!({"error":"manager access token required; open with pybox console"}),
        );
        return;
    }
    if req.method == "POST"
        && req.headers.get("content-type").map(String::as_str) != Some("application/json")
    {
        json_response(
            &mut stream,
            415,
            json!({"error":"application/json required"}),
        );
        return;
    }
    match route(&manager, &req) {
        Ok(value) => json_response(
            &mut stream,
            if req.method == "POST" && req.path == "/api/jobs" {
                202
            } else {
                200
            },
            value,
        ),
        Err(error) => json_response(&mut stream, 400, json!({"error":format!("{error:#}")})),
    }
}

fn open_browser(url: &str) -> Result<()> {
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        std::process::Command::new("rundll32.exe")
            .arg("url.dll,FileProtocolHandler")
            .arg(url)
            .creation_flags(0x08000000)
            .spawn()?;
    }
    #[cfg(unix)]
    {
        std::process::Command::new("xdg-open").arg(url).spawn()?;
    }
    Ok(())
}
pub fn run(args: &[std::ffi::OsString]) -> Result<()> {
    let mut root = std::env::current_exe()?.parent().unwrap().to_owned();
    let mut data = home()?.join("manager");
    let mut port: u16 = 8790;
    let mut explicit_port = false;
    let mut open = false;
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        match arg.to_str().context("invalid option")? {
            "--root" => root = PathBuf::from(it.next().context("--root needs path")?),
            "--data" => data = PathBuf::from(it.next().context("--data needs path")?),
            "--port" => {
                explicit_port = true;
                port = it
                    .next()
                    .context("--port needs value")?
                    .to_str()
                    .context("invalid port")?
                    .parse()?
            }
            "--open" => open = true,
            "--help" | "-h" => {
                println!("pybox manage [--root BUNDLE] [--data STATE_DIR] [--port 8790] [--open]\nLocal single-user manager; one registered image, fixed workload, two execution slots.");
                return Ok(());
            }
            _ => bail!("unknown manager option"),
        }
    }
    let _gate = gate(true)?;
    let root = root.canonicalize()?;
    let shutdown = Arc::new(AtomicBool::new(false));
    let signal = shutdown.clone();
    ctrlc::set_handler(move || signal.store(true, Ordering::Relaxed))?;
    let manager = Arc::new(Manager::load(root, data, shutdown.clone())?);
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)).or_else(|error| {
        if !explicit_port && error.kind() == std::io::ErrorKind::AddrInUse {
            TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        } else {
            Err(error)
        }
    })?;
    listener.set_nonblocking(true)?;
    let authority = listener.local_addr()?.to_string();
    let token = random_id()?;
    let endpoint = home()?.join("endpoint.json");
    save(
        &endpoint,
        &json!({"authority":authority,"token":token,"pid":std::process::id()}),
    )?;
    let url = format!("http://{authority}/");
    println!("Pybox manager: {url}\nOpen the authenticated console with: pybox console\nCtrl+C stops owned jobs before exiting.");
    if open {
        if let Err(error) = open_browser(&format!("{url}#{token}")) {
            eprintln!("browser launch: {error}");
        }
    }
    let worker = manager.clone();
    let scheduler = thread::spawn(move || worker.schedule());
    let connections = Arc::new(AtomicUsize::new(0));
    while !shutdown.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((stream, _)) => {
                if connections.load(Ordering::Relaxed) >= 32 {
                    drop(stream);
                    continue;
                }
                connections.fetch_add(1, Ordering::Relaxed);
                let manager = manager.clone();
                let authority = authority.clone();
                let token = token.clone();
                let count = connections.clone();
                thread::spawn(move || {
                    serve_connection(stream, manager, &authority, &token);
                    count.fetch_sub(1, Ordering::Relaxed);
                });
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(20))
            }
            Err(e) => {
                shutdown.store(true, Ordering::Relaxed);
                eprintln!("listener failed: {e}");
            }
        }
    }
    scheduler
        .join()
        .map_err(|_| anyhow::anyhow!("scheduler panicked"))?;
    // Keep the exclusive gate until every HTTP handler has stopped mutating state.
    while connections.load(Ordering::Relaxed) > 0 {
        thread::sleep(Duration::from_millis(20));
    }
    if read::<Value>(&endpoint)
        .ok()
        .is_some_and(|v| v["token"] == token)
    {
        fs::remove_file(endpoint)?;
    }
    Ok(())
}
pub fn client(action: &str, args: &[std::ffi::OsString]) -> Result<()> {
    let endpoint: Value =
        read(&home()?.join("endpoint.json")).context("no manager endpoint; start pybox manage")?;
    let authority = endpoint["authority"].as_str().context("invalid endpoint")?;
    let address: std::net::SocketAddr = authority.parse()?;
    anyhow::ensure!(
        address.ip() == std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        "manager must use IPv4 loopback"
    );
    let token = endpoint["token"].as_str().context("invalid token")?;
    if action == "console" {
        return open_browser(&format!("http://{authority}/#{token}"));
    }
    let mut payload = json!({"project":"CLI"});
    let mut it = args.iter();
    let (method, path) = match action {
        "jobs" => {
            anyhow::ensure!(args.is_empty(), "jobs takes no options");
            ("GET", "/api/jobs".to_string())
        }
        "stop" => {
            anyhow::ensure!(args.len() == 1, "stop requires a job ID");
            let id = args[0].to_str().context("invalid ID")?;
            anyhow::ensure!(
                id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-'),
                "invalid ID"
            );
            ("POST", format!("/api/jobs/{id}/stop"))
        }
        "submit" => {
            while let Some(arg) = it.next() {
                match arg.to_str().context("invalid option")? {
                    "--project" => {
                        payload["project"] = json!(it
                            .next()
                            .context("project missing")?
                            .to_str()
                            .context("invalid project")?)
                    }
                    "--cold" => payload["cold"] = json!(true),
                    "--timeout-ms" => {
                        payload["timeout_ms"] = json!(it
                            .next()
                            .context("timeout missing")?
                            .to_str()
                            .context("invalid timeout")?
                            .parse::<u64>()?)
                    }
                    "--action" => {
                        payload["action"] = json!(it
                            .next()
                            .context("action missing")?
                            .to_str()
                            .context("invalid action")?)
                    }
                    _ => bail!("unknown submit option"),
                }
            }
            ("POST", "/api/jobs".into())
        }
        _ => bail!("unknown client action"),
    };
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(3))?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    let body = if method == "GET" {
        String::new()
    } else {
        payload.to_string()
    };
    write!(stream, "{method} {path} HTTP/1.1\r\nHost: {authority}\r\nAuthorization: Bearer {token}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len())?;
    let mut reply = String::new();
    stream.take(8 * 1024 * 1024).read_to_string(&mut reply)?;
    let (header, body) = reply
        .split_once("\r\n\r\n")
        .context("invalid manager reply")?;
    let value: Value = serde_json::from_str(body)?;
    println!("{}", serde_json::to_string_pretty(&value)?);
    anyhow::ensure!(header.starts_with("HTTP/1.1 2"), "manager request failed");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn accepted_socket_waits_for_delayed_split_request() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let (ready, received) = std::sync::mpsc::channel();
        let server = thread::spawn(move || {
            let mut stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(1))
                    }
                    Err(e) => panic!("{e}"),
                }
            };
            ready.send(()).unwrap();
            let parsed = request(&mut stream).unwrap();
            assert_eq!(parsed.path, "/api/overview");
            response(&mut stream, 200, "application/json", b"{}").unwrap();
        });
        let mut client = TcpStream::connect(address).unwrap();
        received.recv_timeout(Duration::from_secs(2)).unwrap();
        thread::sleep(Duration::from_millis(30));
        client.write_all(b"GET /api/overview HTTP/1.1\r\n").unwrap();
        thread::sleep(Duration::from_millis(30));
        client.write_all(b"Host: 127.0.0.1\r\n\r\n").unwrap();
        let mut reply = String::new();
        client.read_to_string(&mut reply).unwrap();
        assert!(reply.starts_with("HTTP/1.1 200"));
        server.join().unwrap();
    }
    fn fixture() -> (PathBuf, Manager) {
        let root = std::env::temp_dir().join(format!(
            "pybox-manager-test-{}",
            &random_id().unwrap()[..12]
        ));
        fs::create_dir_all(&root).unwrap();
        let manager = Manager::load(
            root.clone(),
            root.join("data"),
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();
        (root, manager)
    }
    fn submission() -> Submission {
        serde_json::from_value(json!({"project":"example"})).unwrap()
    }
    #[test]
    fn settings_are_captured_per_job_and_queued_stop_never_spawns() {
        let (root, manager) = fixture();
        let first = manager.submit(submission()).unwrap();
        manager.state.lock().unwrap().settings.timeout_ms = 100;
        let next = manager.submit(submission()).unwrap();
        assert_eq!(first["config"]["timeout_ms"], 15000);
        assert_eq!(next["config"]["timeout_ms"], 100);
        let stopped = manager.stop(first["id"].as_str().unwrap()).unwrap();
        assert_eq!(stopped["state"], "cancelled");
        assert!(stopped["pid"].is_null());
        assert_eq!(stopped["cleanup_verified"], true);
        drop(manager);
        fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn restart_does_not_replay_jobs_or_claim_cleanup() {
        let (root, manager) = fixture();
        let queued = manager.submit(submission()).unwrap();
        let running = manager.submit(submission()).unwrap();
        manager.edit(running["id"].as_str().unwrap(), |j| {
            j.state = "running".into();
            j.pid = Some(123);
        });
        drop(manager);
        let restored = Manager::load(
            root.clone(),
            root.join("data"),
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();
        let state = restored.state.lock().unwrap();
        assert_eq!(
            state.jobs[queued["id"].as_str().unwrap()].state,
            "cancelled"
        );
        assert_eq!(
            state.jobs[running["id"].as_str().unwrap()].state,
            "interrupted"
        );
        assert_eq!(
            state.jobs[running["id"].as_str().unwrap()].cleanup_verified,
            Some(false)
        );
        assert!(state.unhealthy.is_some());
        drop(state);
        assert!(restored.submit(submission()).is_err());
        drop(restored);
        fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn unsupported_settings_and_input_are_refused() {
        assert!(serde_json::from_value::<Settings>(
            json!({"max_concurrent":2,"timeout_ms":100,"cold":false,"network":true})
        )
        .is_err());
        assert!(Settings {
            max_concurrent: 3,
            ..Settings::default()
        }
        .validate()
        .is_err());
        let (root, manager) = fixture();
        let mut request = submission();
        request.timeout_ms = Some(15001);
        assert!(manager.submit(request).is_err());
        let mut request = submission();
        request.action = "shell".into();
        assert!(manager.submit(request).is_err());
        drop(manager);
        fs::remove_dir_all(root).unwrap();
    }
}
