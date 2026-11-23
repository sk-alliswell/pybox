//! Stops an agent that keeps sending the same request.
//!
//! A stuck agent loop usually looks like one request repeated back to back.
//! The guard counts consecutive identical requests per agent that were on
//! their way upstream (cache hits cost nothing and are not counted) and
//! refuses them once the count passes the limit. Anything different, or a
//! pause longer than the window, resets the count.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::cache::Key;

pub struct LoopGuard {
    max_repeats: u32,
    window: Duration,
    last: Mutex<HashMap<String, (Key, u32, Instant)>>,
}

impl LoopGuard {
    pub fn new(max_repeats: u32, window: Duration) -> Self {
        Self {
            max_repeats,
            window,
            last: Mutex::default(),
        }
    }

    /// Record one upstream-bound request. `Some(n)` means it is the `n`th
    /// identical request in a row and must be refused.
    pub fn check(&self, agent: &str, key: Key, now: Instant) -> Option<u32> {
        let mut last = self.last.lock().unwrap();
        let e = last.entry(agent.to_string()).or_insert((key, 0, now));
        if e.0 == key && now.saturating_duration_since(e.2) < self.window {
            e.1 += 1;
        } else {
            *e = (key, 1, now);
        }
        e.2 = now;
        (e.1 > self.max_repeats).then_some(e.1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocks_after_the_limit_and_resets_on_change_or_pause() {
        let g = LoopGuard::new(3, Duration::from_secs(60));
        let (a, b) = ([1u8; 32], [2u8; 32]);
        let t = Instant::now();
        for _ in 0..3 {
            assert_eq!(g.check("x", a, t), None);
        }
        assert_eq!(g.check("x", a, t), Some(4));
        assert_eq!(g.check("x", a, t), Some(5));
        // Another agent is unaffected; a different request resets.
        assert_eq!(g.check("y", a, t), None);
        assert_eq!(g.check("x", b, t), None);
        assert_eq!(g.check("x", a, t), None);
        // So does going quiet for the whole window.
        for _ in 0..2 {
            g.check("x", a, t);
        }
        assert_eq!(g.check("x", a, t), Some(4));
        assert_eq!(g.check("x", a, t + Duration::from_secs(61)), None);
    }
}
