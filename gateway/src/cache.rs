//! Opt-in response cache with request coalescing for non-streaming calls.
//!
//! Identical requests (same upstream, path and body bytes) that arrive while
//! one is in flight wait for it instead of hitting the API again, and a
//! successful result is reused until it expires. This is only safe for
//! deterministic calls, so it is enabled per request (`x-gateway-cache: on`)
//! unless `cache.default_on` is set.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http::{HeaderMap, StatusCode};
use sha2::{Digest, Sha256};
use tokio::sync::watch;

pub type Key = [u8; 32];

#[derive(Debug)]
pub struct Cached {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Bytes,
}

enum Slot {
    Ready(Arc<Cached>, Instant),
    Pending(watch::Receiver<Option<Arc<Cached>>>),
}

pub struct Cache {
    ttl: Duration,
    max_entries: usize,
    slots: Mutex<HashMap<Key, Slot>>,
}

pub enum Lookup {
    Hit(Arc<Cached>),
    /// Another request is computing this; wait for it.
    Wait(watch::Receiver<Option<Arc<Cached>>>),
    /// We compute it and must call [`Leader::complete`].
    Leader(Leader),
}

pub struct Leader {
    cache: Arc<Cache>,
    key: Key,
    tx: Option<watch::Sender<Option<Arc<Cached>>>>,
}

pub fn key(upstream: &str, path_and_query: &str, body: &[u8]) -> Key {
    let mut h = Sha256::new();
    h.update(upstream.as_bytes());
    h.update([0]);
    h.update(path_and_query.as_bytes());
    h.update([0]);
    h.update(body);
    h.finalize().into()
}

impl Cache {
    pub fn new(ttl: Duration, max_entries: usize) -> Arc<Self> {
        Arc::new(Self {
            ttl,
            max_entries: max_entries.max(1),
            slots: Mutex::default(),
        })
    }

    pub fn lookup(self: &Arc<Self>, key: Key) -> Lookup {
        let mut slots = self.slots.lock().unwrap();
        match slots.get(&key) {
            Some(Slot::Ready(c, exp)) if *exp > Instant::now() => return Lookup::Hit(c.clone()),
            Some(Slot::Pending(rx)) => return Lookup::Wait(rx.clone()),
            _ => {}
        }
        let (tx, rx) = watch::channel(None);
        slots.insert(key, Slot::Pending(rx));
        Lookup::Leader(Leader {
            cache: self.clone(),
            key,
            tx: Some(tx),
        })
    }

    fn evict(&self, slots: &mut HashMap<Key, Slot>) {
        if slots.len() <= self.max_entries {
            return;
        }
        let now = Instant::now();
        slots.retain(|_, s| !matches!(s, Slot::Ready(_, exp) if *exp <= now));
        while slots.len() > self.max_entries {
            let oldest = slots
                .iter()
                .filter_map(|(k, s)| match s {
                    Slot::Ready(_, exp) => Some((*k, *exp)),
                    Slot::Pending(_) => None,
                })
                .min_by_key(|(_, exp)| *exp)
                .map(|(k, _)| k);
            match oldest {
                Some(k) => {
                    slots.remove(&k);
                }
                None => break,
            }
        }
    }
}

impl Leader {
    /// Publish the result. `None` (or a non-success status) is shared with
    /// current waiters but not stored.
    pub fn complete(mut self, result: Option<Arc<Cached>>) {
        let tx = self.tx.take().expect("complete once");
        let mut slots = self.cache.slots.lock().unwrap();
        match &result {
            Some(c) if c.status.is_success() => {
                slots.insert(
                    self.key,
                    Slot::Ready(c.clone(), Instant::now() + self.cache.ttl),
                );
                self.cache.evict(&mut slots);
            }
            _ => {
                slots.remove(&self.key);
            }
        }
        drop(slots);
        let _ = tx.send(result);
    }
}

impl Drop for Leader {
    fn drop(&mut self) {
        if let Some(tx) = self.tx.take() {
            // Abandoned (client gone or error): unblock waiters, store nothing.
            self.cache.slots.lock().unwrap().remove(&self.key);
            let _ = tx.send(None);
        }
    }
}

/// Wait for a leader's result. `None` means it failed; the caller should go
/// to the upstream itself.
pub async fn wait(mut rx: watch::Receiver<Option<Arc<Cached>>>) -> Option<Arc<Cached>> {
    let res = rx.wait_for(|v| v.is_some()).await.map(|v| v.clone());
    match res {
        Ok(v) => v,
        Err(_) => rx.borrow().clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resp(status: u16) -> Arc<Cached> {
        Arc::new(Cached {
            status: StatusCode::from_u16(status).unwrap(),
            headers: HeaderMap::new(),
            body: Bytes::from_static(b"x"),
        })
    }

    #[tokio::test]
    async fn coalesces_then_hits() {
        let c = Cache::new(Duration::from_secs(60), 10);
        let k = key("a", "/v1", b"{}");
        let Lookup::Leader(leader) = c.lookup(k) else {
            panic!()
        };
        let Lookup::Wait(rx) = c.lookup(k) else {
            panic!()
        };
        let waiter = tokio::spawn(wait(rx));
        leader.complete(Some(resp(200)));
        assert!(waiter.await.unwrap().is_some());
        assert!(matches!(c.lookup(k), Lookup::Hit(_)));
    }

    #[tokio::test]
    async fn failures_are_not_stored() {
        let c = Cache::new(Duration::from_secs(60), 10);
        let k = key("a", "/v1", b"{}");
        let Lookup::Leader(leader) = c.lookup(k) else {
            panic!()
        };
        let Lookup::Wait(rx) = c.lookup(k) else {
            panic!()
        };
        drop(leader);
        assert!(wait(rx).await.is_none());
        assert!(matches!(c.lookup(k), Lookup::Leader(_)));
    }

    #[test]
    fn evicts_oldest() {
        let c = Cache::new(Duration::from_secs(60), 2);
        for i in 0..3u8 {
            let Lookup::Leader(l) = c.lookup(key("a", "/", &[i])) else {
                panic!()
            };
            l.complete(Some(resp(200)));
        }
        assert_eq!(c.slots.lock().unwrap().len(), 2);
    }
}
