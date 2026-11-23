//! Agents and keys changed while the gateway runs, kept across restarts.
//!
//! The config file names the agents the gateway starts with. Whatever the
//! admin API changes after that (an agent created, patched, rotated or
//! removed) used to live in memory only. With a persistent store it is now
//! written to the store's metadata and applied again at start:
//!
//! - kind `agent`, id `sha256:<hex of the token>`: an agent that is not in
//!   the config, or a config agent whose settings were changed;
//! - kind `agent_revoked`, same id: a config token that was removed or
//!   rotated away, so a restart does not bring it back.
//!
//! Tokens themselves are never stored, only their SHA-256. A stored agent
//! is therefore looked up by the hash of the token a caller presents.
//! Console jobs (`job:*`) have keys that die with the job and are left out.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde_json::json;

use crate::{AgentInfo, App};

const AGENT: &str = "agent";
const REVOKED: &str = "agent_revoked";
const PREFIX: &str = "sha256:";

/// The name a token is stored and looked up under.
pub fn token_key(token: &str) -> String {
    use sha2::{Digest, Sha256};
    let hex: String = Sha256::digest(token.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    format!("{PREFIX}{hex}")
}

/// Whether a presented token is shaped like a stored name. Such a token is
/// never accepted as it is: knowing a hash must not be as good as knowing
/// the token.
pub fn is_key(token: &str) -> bool {
    token.starts_with(PREFIX)
}

fn stored_name(map_key: &str) -> String {
    if is_key(map_key) {
        map_key.to_string()
    } else {
        token_key(map_key)
    }
}

impl App {
    /// Apply what was stored on top of the agents from the config.
    pub(crate) fn restore_agents(&self) {
        let Some(obs) = &self.obs else { return };
        let revoked: HashSet<String> = obs
            .backend
            .meta_list(REVOKED)
            .unwrap_or_default()
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        let stored = obs.backend.meta_list(AGENT).unwrap_or_default();
        if revoked.is_empty() && stored.is_empty() {
            return;
        }
        let mut agents = self.agents.write().unwrap();
        agents.retain(|token, _| !revoked.contains(&token_key(token)));
        // Config tokens by stored name, to put changed settings back under
        // the token itself.
        let by_name: HashMap<String, String> =
            agents.keys().map(|t| (token_key(t), t.clone())).collect();
        let mut n = 0;
        for (name, v) in stored {
            match serde_json::from_value::<AgentInfo>(v) {
                Ok(info) if !info.id.is_empty() => {
                    let key = by_name.get(&name).cloned().unwrap_or(name);
                    agents.insert(key, Arc::new(info));
                    n += 1;
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(agent = %name, error = %e, "stored agent skipped"),
            }
        }
        tracing::info!(
            agents = n,
            revoked = revoked.len(),
            "agents restored from the store"
        );
    }

    /// Write the difference between the agents now and the config to the
    /// store. Called after every change made over the admin API.
    pub(crate) fn save_agents(&self) {
        let Some(obs) = &self.obs else { return };
        let config: HashMap<String, &crate::config::AgentConfig> = self
            .cfg
            .agents
            .iter()
            .map(|a| (a.token.clone(), a))
            .collect();
        let mut want: HashMap<String, serde_json::Value> = HashMap::new();
        let mut gone: HashMap<String, serde_json::Value> = HashMap::new();
        {
            let agents = self.agents.read().unwrap();
            for (key, info) in agents.iter() {
                if info.id.starts_with("job:") {
                    continue;
                }
                let unchanged = config
                    .get(key)
                    .is_some_and(|c| json!(AgentInfo::from_config(c)) == json!(**info));
                if !unchanged {
                    want.insert(stored_name(key), json!(**info));
                }
            }
            for (token, c) in &config {
                if !agents.contains_key(token) {
                    gone.insert(token_key(token), json!({ "id": c.id }));
                }
            }
        }
        for (kind, want) in [(AGENT, &want), (REVOKED, &gone)] {
            let have: HashMap<String, serde_json::Value> = obs
                .backend
                .meta_list(kind)
                .unwrap_or_default()
                .into_iter()
                .collect();
            for (id, v) in want {
                if have.get(id) != Some(v) {
                    if let Err(e) = obs.backend.meta_put(kind, id, v.clone()) {
                        tracing::warn!(error = %e, "agent not stored");
                    }
                }
            }
            for id in have.keys().filter(|id| !want.contains_key(*id)) {
                let _ = obs.backend.meta_delete(kind, id);
            }
        }
    }
}
