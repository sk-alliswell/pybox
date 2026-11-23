//! Coordinates standalone CLI and manager processes for the same user.
use anyhow::{Context, Result};
use fs2::FileExt;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};

pub(super) fn home() -> Result<PathBuf> {
    Ok(PathBuf::from(
        std::env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" })
            .context("user home is unavailable")?,
    )
    .join(".pybox"))
}
pub(super) fn private_directory(path: &Path) -> Result<()> {
    fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}
pub(super) fn acquire_standalone() -> Result<File> {
    try_lock(&home()?, false)
}

#[cfg(feature = "manager")]
pub(super) fn acquire_manager() -> Result<File> {
    try_lock(&home()?, true)
}

fn try_lock(folder: &Path, exclusive: bool) -> Result<File> {
    private_directory(folder)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standalone_and_manager_locks_exclude_each_other() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let folder =
            std::env::temp_dir().join(format!("pybox-gate-{}-{nonce}", std::process::id()));

        let manager = try_lock(&folder, true).unwrap();
        assert!(try_lock(&folder, false).is_err());
        drop(manager);

        let first = try_lock(&folder, false).unwrap();
        let second = try_lock(&folder, false).unwrap();
        assert!(try_lock(&folder, true).is_err());
        drop((first, second));

        drop(try_lock(&folder, true).unwrap());
        fs::remove_file(folder.join("manager.lock")).unwrap();
        fs::remove_dir(folder).unwrap();
    }
}
