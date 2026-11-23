//! Reading scenario files, with `extends = "base.toml"` so a variant only
//! states what it changes.

use std::path::Path;

use crate::scenario::Scenario;

fn read(path: &Path) -> Result<String, String> {
    std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))
}

/// Tables merge key by key. Arrays of named tables (`[[client]]`,
/// `[[model_server]]`, ...) merge entry by entry on `name`, so a variant only
/// states what it changes. Anything else in `over` replaces `base`.
fn merge(base: &mut toml::Value, over: toml::Value) {
    let named = |v: &toml::Value| {
        v.as_array().is_some_and(|a| {
            !a.is_empty()
                && a.iter()
                    .all(|x| x.get("name").is_some_and(toml::Value::is_str))
        })
    };
    match (base, over) {
        (toml::Value::Table(b), toml::Value::Table(o)) => {
            for (k, v) in o {
                match b.get_mut(&k) {
                    Some(slot) => merge(slot, v),
                    None => {
                        b.insert(k, v);
                    }
                }
            }
        }
        (slot, v) if named(slot) && named(&v) => {
            let (toml::Value::Array(b), toml::Value::Array(o)) = (slot, v) else {
                unreachable!("both are arrays")
            };
            for item in o {
                let name = item.get("name").cloned();
                match b.iter_mut().find(|x| x.get("name") == name.as_ref()) {
                    Some(existing) => merge(existing, item),
                    None => b.push(item),
                }
            }
        }
        (slot, v) => *slot = v,
    }
}

/// Load a scenario, applying `extends = "other.toml"` (relative to the file).
fn load_value(path: &Path, depth: usize) -> Result<toml::Value, String> {
    if depth > 8 {
        return Err(format!("{}: `extends` chain is too deep", path.display()));
    }
    let mut v: toml::Value =
        toml::from_str(&read(path)?).map_err(|e| format!("{}: {e}", path.display()))?;
    let parent = v
        .as_table_mut()
        .and_then(|t| t.remove("extends"))
        .and_then(|p| p.as_str().map(str::to_string));
    match parent {
        Some(p) => {
            let dir = path.parent().unwrap_or(Path::new("."));
            let mut base = load_value(&dir.join(p), depth + 1)?;
            merge(&mut base, v);
            Ok(base)
        }
        None => Ok(v),
    }
}

/// Load a scenario from a TOML or JSON file and check it.
pub fn load(path: &str) -> Result<Scenario, String> {
    let p = Path::new(path);
    if path.ends_with(".json") {
        return Scenario::from_json(&read(p)?).map_err(|e| format!("{path}: {e}"));
    }
    let v = load_value(p, 0)?;
    let mut sc: Scenario = v.try_into().map_err(|e| format!("{path}: {e}"))?;
    if sc.name.is_empty() {
        sc.name = p
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
    }
    sc.ready().map_err(|e| format!("{path}: {e}"))
}
