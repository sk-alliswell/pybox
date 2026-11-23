mod artifacts;
pub mod guard;
mod image;
pub mod platform;

pub use artifacts::sha;
pub use image::{Image, READY, SOURCE};

use anyhow::{bail, Result};
use fs2::FileExt;
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

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
