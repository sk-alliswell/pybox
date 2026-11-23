pub mod guard;
pub mod platform;

use anyhow::{bail, Context, Result};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub const READY: &str = "import os; os.environ['LANGSMITH_TRACING']='false'; import sys; sys.path.insert(0,'/app'); import harness,json; print(json.dumps(harness.prepare()))";
pub const SOURCE: &str =
    "import os,runpy; os.chdir('/app'); runpy.run_path('/app/entrypoint.py',run_name='__main__')";

pub struct Cancel {
    pub signal: Arc<AtomicBool>,
    pub began: Instant,
    pub after: Option<Duration>,
}
impl Cancel {
    pub fn cancelled(&self) -> bool {
        self.signal.load(Ordering::Relaxed) || self.after.is_some_and(|d| self.began.elapsed() >= d)
    }
}

pub struct Lease {
    file: File,
    pub slot: usize,
}
impl Lease {
    pub fn acquire(directory: &Path) -> Result<Self> {
        fs::create_dir_all(directory)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(directory, fs::Permissions::from_mode(0o700))?;
        }
        for slot in 0..2 {
            let path = directory.join(format!("slot-{slot}.lock"));
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path)?;
            if file.metadata()?.len() == 0 {
                file.write_all(&[0])?;
                file.flush()?;
            }
            match FileExt::try_lock_exclusive(&file) {
                Ok(()) => return Ok(Self { file, slot }),
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.raw_os_error() == Some(33) => {}
                Err(e) => return Err(e.into()),
            }
        }
        bail!("busy: two cross-process execution leases are held")
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

pub fn sha(path: &Path, cancel: Option<&Cancel>) -> Result<String> {
    let mut stream = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0; 1024 * 1024];
    loop {
        if cancel.is_some_and(Cancel::cancelled) {
            bail!("cancelled during integrity verification");
        }
        let count = stream.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}
fn text_sha(text: &str) -> String {
    format!("{:x}", Sha256::digest(text.as_bytes()))
}

fn relative(root: &Path, name: &str) -> Result<PathBuf> {
    if name.is_empty()
        || name.contains('\\')
        || name.contains('\0')
        || name.split('/').any(|v| v == ".." || v.is_empty())
    {
        bail!("invalid relative artifact path");
    }
    let path = Path::new(name);
    if path
        .components()
        .any(|v| !matches!(v, Component::Normal(_)))
    {
        bail!("artifact path escapes bundle");
    }
    let mut current = root.to_owned();
    for component in path.components() {
        current.push(component);
        let meta = fs::symlink_metadata(&current)?;
        if meta.file_type().is_symlink() {
            bail!("artifact symlinks refused");
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::MetadataExt;
            if meta.file_attributes() & 0x400 != 0 {
                bail!("artifact reparse points refused");
            }
        }
    }
    Ok(current)
}

#[derive(Serialize, Deserialize, Debug)]
struct Entry {
    bytes: u64,
    sha256: String,
}
type Inventory = BTreeMap<String, Entry>;

fn read_json(path: &Path) -> Result<Value> {
    if fs::metadata(path)?.len() > 1024 * 1024 {
        bail!("oversized JSON control file");
    }
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}
fn inventory(root: &Path) -> Result<Inventory> {
    fn walk(base: &Path, at: &Path, found: &mut Inventory) -> Result<()> {
        for entry in fs::read_dir(at)? {
            let entry = entry?;
            let name = entry
                .path()
                .strip_prefix(base)?
                .to_str()
                .context("non-UTF8 snapshot name")?
                .replace('\\', "/");
            let path = relative(base, &name)?;
            let meta = fs::symlink_metadata(&path)?;
            if meta.is_dir() {
                walk(base, &path, found)?;
            } else if meta.is_file() {
                if name != "controller-snapshot.json" {
                    found.insert(
                        name,
                        Entry {
                            bytes: meta.len(),
                            sha256: sha(&path, None)?,
                        },
                    );
                }
            } else {
                bail!("non-regular snapshot artifact");
            }
        }
        Ok(())
    }
    let mut files = BTreeMap::new();
    walk(root, root, &mut files)?;
    Ok(files)
}
fn verify_inventory(root: &Path, expected: &Inventory, cancel: &Cancel) -> Result<()> {
    for (name, entry) in expected {
        let path = relative(root, name)?;
        if !path.is_file()
            || fs::metadata(&path)?.len() != entry.bytes
            || sha(&path, Some(cancel))? != entry.sha256
        {
            bail!("integrity mismatch: {name}");
        }
    }
    Ok(())
}
fn names(root: &Path) -> Result<BTreeSet<String>> {
    fn walk(base: &Path, at: &Path, found: &mut BTreeSet<String>) -> Result<()> {
        for entry in fs::read_dir(at)? {
            let path = entry?.path();
            let name = path
                .strip_prefix(base)?
                .to_str()
                .context("non-UTF8 artifact name")?
                .replace('\\', "/");
            let safe = relative(base, &name)?;
            if safe.is_dir() {
                walk(base, &safe, found)?;
            } else if safe.is_file() && name != "controller-snapshot.json" {
                found.insert(name);
            }
        }
        Ok(())
    }
    let mut found = BTreeSet::new();
    walk(root, root, &mut found)?;
    Ok(found)
}
pub fn environment() -> BTreeMap<std::ffi::OsString, std::ffi::OsString> {
    let allowed = if cfg!(windows) {
        vec![
            "SYSTEMROOT",
            "WINDIR",
            "PATH",
            "TEMP",
            "TMP",
            "USERPROFILE",
            "LOCALAPPDATA",
            "APPDATA",
            "PROGRAMDATA",
            "COMSPEC",
        ]
    } else {
        vec!["PATH", "HOME", "TMPDIR", "LANG", "LC_ALL"]
    };
    std::env::vars_os()
        .filter(|(k, _)| allowed.contains(&k.to_string_lossy().to_uppercase().as_str()))
        .collect()
}

pub struct Image {
    pub root: PathBuf,
    pub binary: PathBuf,
    pub manifest: Value,
    pub expected: Value,
    pub identity: Value,
}
impl Image {
    pub fn open(root: &Path, cancel: &Cancel) -> Result<Self> {
        let root = root.canonicalize()?;
        let map: Inventory =
            serde_json::from_value(read_json(&relative(&root, "BUNDLE_SHA256.json")?)?)?;
        let native = if cfg!(windows) { "hluk.exe" } else { "hluk" };
        for required in [
            native,
            "rootfs.cpio",
            "image-manifest.json",
            "docker-baseline.json",
        ] {
            if !map.contains_key(required) {
                bail!("incomplete bundle manifest: {required}");
            }
        }
        verify_inventory(&root, &map, cancel)?;
        let manifest = read_json(&relative(&root, "image-manifest.json")?)?;
        let expected = read_json(&relative(&root, "docker-baseline.json")?)?["output"].clone();
        if manifest["working_dir"] != "/app"
            || manifest["entrypoint"] != json!(["/usr/local/bin/python3", "/app/entrypoint.py"])
        {
            bail!("unsupported Python image entrypoint/workdir");
        }
        if map["rootfs.cpio"].sha256
            != manifest["rootfs_sha256"]
                .as_str()
                .context("missing rootfs digest")?
        {
            bail!("image/manifest rootfs disagreement");
        }
        let identity = json!({"rootfs_sha256":map["rootfs.cpio"].sha256,"vmm_sha256":map[native].sha256,
            "ready_sha256":text_sha(READY),"guest_memory_mib":512,"snapshot_contract":1});
        let binary = relative(&root, native)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&binary, fs::Permissions::from_mode(0o755))?;
        }
        Ok(Self {
            root,
            binary,
            manifest,
            expected,
            identity,
        })
    }
    pub fn snapshot(&self, cancel: &Cancel, events: bool) -> Result<(PathBuf, f64)> {
        let cache = self.root.join("cache");
        fs::create_dir_all(&cache)?;
        let init = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.root.join("state/preparation.lock"))?;
        FileExt::try_lock_exclusive(&init).context("busy: snapshot preparation in progress")?;
        let active = cache.join("snapshot-ready");
        let mut prep_ms = 0.0;
        if !active.exists() {
            if cancel.cancelled() {
                bail!("cancelled before snapshot preparation");
            }
            let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
            let stage = cache.join(format!(".staging-{}-{nonce}", std::process::id()));
            let start = Instant::now();
            let argv = vec![
                "snapshot".into(),
                "save".into(),
                "--initrd".into(),
                self.root.join("rootfs.cpio").into_os_string(),
                "--scratch-mb".into(),
                "512".into(),
                "--warm-exec".into(),
                READY.into(),
                "-o".into(),
                stage.clone().into_os_string(),
            ];
            let saved = guard::run(
                &self.binary,
                &argv,
                &self.root,
                guard::Profile::Preparation,
                Duration::from_secs(45),
                8192,
                cancel,
                events,
                "prepare",
            )?;
            if saved.status != "completed" || !saved.resources_reclaimed {
                bail!(
                    "snapshot preparation failed: {}",
                    serde_json::to_string(&saved)?
                );
            }
            if serde_json::from_str::<Value>(&saved.stdout)?
                != json!({"checkpoint_threads":0,"agent_has_checkpointer":false})
            {
                bail!("ready hook has nonempty state");
            }
            let files = inventory(&stage)?;
            if !files.contains_key("index.json") {
                bail!("snapshot index missing");
            }
            let pins = json!({"identity":self.identity,"files":files,"preparation_receipt":saved});
            let mut f = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(stage.join("controller-snapshot.json"))?;
            f.write_all(serde_json::to_string_pretty(&pins)?.as_bytes())?;
            f.sync_all()?;
            drop(f);
            if active.exists() {
                bail!("snapshot activation race; stage retained");
            }
            fs::rename(&stage, &active).context("activate prepared snapshot directory")?;
            prep_ms = start.elapsed().as_secs_f64() * 1000.0;
        }
        let pins = read_json(&relative(&active, "controller-snapshot.json")?)?;
        if pins["identity"] != self.identity {
            bail!("snapshot identity differs from image/VMM/ready policy");
        }
        let expected: Inventory = serde_json::from_value(pins["files"].clone())?;
        if names(&active)? != expected.keys().cloned().collect()
            || !expected.contains_key("index.json")
        {
            bail!("snapshot inventory mismatch");
        }
        verify_inventory(&active, &expected, cancel)?;
        FileExt::unlock(&init)?;
        Ok((active, prep_ms))
    }
    pub fn execute(
        &self,
        snapshot: Option<&Path>,
        timeout: Duration,
        cancel: &Cancel,
        events: bool,
    ) -> Result<guard::Receipt> {
        let mut args = if let Some(path) = snapshot {
            vec!["snapshot".into(), "run".into(), path.as_os_str().to_owned()]
        } else {
            vec![
                "run".into(),
                "--initrd".into(),
                self.root.join("rootfs.cpio").into_os_string(),
                "--scratch-mb".into(),
                "512".into(),
                "--cold".into(),
            ]
        };
        let allowed = [
            "PATH",
            "LANG",
            "PYTHON_VERSION",
            "PYTHON_SHA256",
            "SANDBOX_IMAGE_MARKER",
            "LANGSMITH_TRACING",
            "PYTHONDONTWRITEBYTECODE",
        ];
        for (key, value) in self.manifest["environment"]
            .as_object()
            .context("image environment missing")?
        {
            if !allowed.contains(&key.as_str()) {
                bail!("unsupported guest environment key");
            }
            args.push("--env".into());
            args.push(
                format!(
                    "{key}={}",
                    value.as_str().context("invalid environment value")?
                )
                .into(),
            );
        }
        args.extend(["--exec".into(), SOURCE.into()]);
        guard::run(
            &self.binary,
            &args,
            &self.root,
            guard::Profile::Execution,
            timeout,
            8192,
            cancel,
            events,
            "run",
        )
    }
    pub fn accepts(&self, body: &Value) -> bool {
        body["passed"] == true
            && body["uv_in_guest"] == false
            && body["versions"] == self.expected["versions"]
            && body["image_contract"] == self.expected["image_contract"]
            && body["python"] == self.expected["python"]
            && body["langgraph"]["first"] == self.expected["langgraph"]["first"]
            && body["langgraph"]["second"] == self.expected["langgraph"]["second"]
            && body["langgraph"]["history_count"] == self.expected["langgraph"]["history_count"]
            && body["langgraph"]["checks"] == self.expected["langgraph"]["checks"]
            && body["deepagents"]["tool_calls"] == self.expected["deepagents"]["tool_calls"]
            && body["deepagents"]["files"] == self.expected["deepagents"]["files"]
            && body["deepagents"]["tool_outputs"] == self.expected["deepagents"]["tool_outputs"]
            && body["deepagents"]["checks"] == self.expected["deepagents"]["checks"]
            && body["langgraph"]["passed"] == true
            && body["deepagents"]["passed"] == true
    }
}
