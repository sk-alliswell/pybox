use anyhow::{ensure, Context, Result};
use fs2::FileExt;
use serde::{de::DeserializeOwned, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;

const MAX_RECORD: u64 = 16 * 1024;

pub(super) fn private_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    let result = {
        use std::os::unix::fs::DirBuilderExt;
        fs::DirBuilder::new().mode(0o700).create(path)
    };
    #[cfg(windows)]
    let result = fs::create_dir(path);
    match result {
        Ok(()) => sync_directory(path.parent().context("state directory has no parent")?),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists && path.is_dir() => Ok(()),
        Err(error) => Err(error).with_context(|| format!("create {}", path.display())),
    }
}

pub(super) fn lock(directory: &Path, create: bool) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    if create {
        options.create_new(true);
    }
    let file = options
        .open(directory.join("run.lock"))
        .context("open agent run lock")?;
    FileExt::try_lock_exclusive(&file).context("agent run is being updated")?;
    Ok(file)
}

pub(super) fn read<T: DeserializeOwned>(directory: &Path, name: &str) -> Result<Option<T>> {
    let file = match File::open(directory.join(name)) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).with_context(|| format!("open {name}")),
    };
    let mut bytes = Vec::new();
    file.take(MAX_RECORD + 1).read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= MAX_RECORD,
        "oversized agent record {name}"
    );
    serde_json::from_slice(&bytes)
        .with_context(|| format!("invalid agent record {name}"))
        .map(Some)
}

// Callers hold the per-run lock. Published records are immutable.
pub(super) fn publish(directory: &Path, name: &str, record: &impl Serialize) -> Result<()> {
    let bytes = serde_json::to_vec(record)?;
    ensure!(
        bytes.len() as u64 <= MAX_RECORD,
        "oversized agent record {name}"
    );
    let destination = directory.join(name);
    ensure!(
        !destination.try_exists()?,
        "agent record {name} is already committed"
    );
    let pending = destination.with_extension("pending");
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&pending)
        .with_context(|| format!("stage {name}"))?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);
    move_record(&pending, &destination)?;
    sync_directory(directory)
}

#[cfg(unix)]
fn move_record(from: &Path, to: &Path) -> io::Result<()> {
    fs::rename(from, to)
}

#[cfg(windows)]
fn move_record(from: &Path, to: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{MoveFileExW, MOVEFILE_WRITE_THROUGH};

    let from: Vec<u16> = from.as_os_str().encode_wide().chain([0]).collect();
    let to: Vec<u16> = to.as_os_str().encode_wide().chain([0]).collect();
    // The same-directory paths are NUL-terminated. Avoid replacement of committed state.
    if unsafe { MoveFileExW(from.as_ptr(), to.as_ptr(), MOVEFILE_WRITE_THROUGH) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(windows)]
fn sync_directory(_path: &Path) -> Result<()> {
    // The file flush and write-through move are performed by publish.
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    #[test]
    fn interrupted_publication_never_creates_a_committed_record() {
        let directory = std::env::temp_dir().join(format!(
            "pybox-agent-store-{}",
            super::super::random_id().unwrap()
        ));
        private_directory(&directory).unwrap();
        publish(&directory, "prior.json", &json!({"revision": 1})).unwrap();
        fs::write(directory.join("memory.pending"), b"{interrupted").unwrap();
        assert!(publish(&directory, "memory.json", &json!({"revision": 2})).is_err());
        assert_eq!(read::<Value>(&directory, "memory.json").unwrap(), None);
        assert_eq!(
            read::<Value>(&directory, "prior.json").unwrap(),
            Some(json!({"revision": 1}))
        );
        fs::remove_file(directory.join("prior.json")).unwrap();
        fs::remove_file(directory.join("memory.pending")).unwrap();
        fs::remove_dir(directory).unwrap();
    }
}
