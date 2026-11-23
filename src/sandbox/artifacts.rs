use super::Cancel;
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::Read;
use std::path::{Component, Path, PathBuf};

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
pub(super) fn text_sha(text: &str) -> String {
    format!("{:x}", Sha256::digest(text.as_bytes()))
}

pub(super) fn relative(root: &Path, name: &str) -> Result<PathBuf> {
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
pub(super) struct Entry {
    bytes: u64,
    pub(super) sha256: String,
}
pub(super) type Inventory = BTreeMap<String, Entry>;

pub(super) fn read_json(path: &Path) -> Result<Value> {
    if fs::metadata(path)?.len() > 1024 * 1024 {
        bail!("oversized JSON control file");
    }
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}
pub(super) fn inventory(root: &Path) -> Result<Inventory> {
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
pub(super) fn verify_inventory(root: &Path, expected: &Inventory, cancel: &Cancel) -> Result<()> {
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
pub(super) fn names(root: &Path) -> Result<BTreeSet<String>> {
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
