//! A model of a provider's prompt cache, following Anthropic's published
//! rules: exact prefix match at breakpoints, a 20-block lookback, a minimum
//! cacheable length, and a lifetime that restarts on every read.
//!
//! The gateway uses it to predict what a request will read and write, the
//! simulator uses it as the provider, and comparing its prediction with the
//! usage a real response reports tells us how far to trust it.

use std::collections::HashMap;

use super::fingerprint::Ttl;

#[derive(Clone, Copy, Debug)]
pub struct CacheRules {
    pub min_tokens: u32,
    pub lookback: usize,
}

impl Default for CacheRules {
    fn default() -> Self {
        Self {
            min_tokens: 1024,
            lookback: 20,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CacheUse {
    pub read: u32,
    pub write: u32,
    pub fresh: u32,
    /// Block index of the cache entry that was read.
    pub hit: Option<usize>,
}

/// Prices relative to one base input token.
#[derive(Clone, Copy, Debug)]
pub struct Weights {
    pub read: f64,
    pub write_5m: f64,
    pub write_1h: f64,
    pub output: f64,
}

impl Default for Weights {
    fn default() -> Self {
        Self {
            read: 0.1,
            write_5m: 1.25,
            write_1h: 2.0,
            output: 5.0,
        }
    }
}

impl Weights {
    pub fn write(&self, ttl: Ttl) -> f64 {
        match ttl {
            Ttl::M5 => self.write_5m,
            Ttl::H1 => self.write_1h,
        }
    }
    /// Cost of a request's input in base-input-token units.
    pub fn input_cost(&self, u: CacheUse, ttl: Ttl) -> f64 {
        f64::from(u.fresh) + f64::from(u.read) * self.read + f64::from(u.write) * self.write(ttl)
    }
}

#[derive(Default)]
pub struct ProviderCache {
    entries: HashMap<u64, (i64, Ttl)>,
    ops: u32,
}

impl ProviderCache {
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn alive(&self, cum: u64, now: i64) -> bool {
        self.entries.get(&cum).is_some_and(|(exp, _)| *exp > now)
    }

    /// What a request with these blocks and breakpoints would use, without
    /// touching the cache. `chain` is (cumulative hash, cumulative tokens).
    pub fn peek(&self, chain: &[(u64, u32)], bps: &[usize], now: i64, r: CacheRules) -> CacheUse {
        let total = chain.last().map_or(0, |c| c.1);
        let valid: Vec<usize> = bps
            .iter()
            .copied()
            .filter(|&i| i < chain.len() && chain[i].1 >= r.min_tokens)
            .collect();
        let Some(&last) = valid.iter().max() else {
            return CacheUse {
                fresh: total,
                ..CacheUse::default()
            };
        };
        let mut hit: Option<usize> = None;
        for &bp in &valid {
            let lo = (bp + 1).saturating_sub(r.lookback);
            if let Some(j) = (lo..=bp).rev().find(|&j| self.alive(chain[j].0, now)) {
                hit = hit.max(Some(j));
            }
        }
        let read = hit.map_or(0, |j| chain[j].1);
        CacheUse {
            read,
            write: chain[last].1.saturating_sub(read),
            fresh: total - chain[last].1,
            hit,
        }
    }

    /// Like `peek`, and records the reads and writes.
    pub fn apply(
        &mut self,
        chain: &[(u64, u32)],
        bps: &[usize],
        ttl: Ttl,
        now: i64,
        r: CacheRules,
    ) -> CacheUse {
        let u = self.peek(chain, bps, now, r);
        if let Some(j) = u.hit {
            if let Some(e) = self.entries.get_mut(&chain[j].0) {
                e.0 = now + e.1.ms();
            }
        }
        for &bp in bps {
            if bp < chain.len() && chain[bp].1 >= r.min_tokens {
                let e = self.entries.entry(chain[bp].0).or_insert((0, ttl));
                e.0 = e.0.max(now + e.1.ms());
            }
        }
        self.ops += 1;
        if self.ops % 4096 == 0 {
            self.entries.retain(|_, (exp, _)| *exp > now);
        }
        u
    }

    /// When the entry at this prefix expires, if it is alive.
    pub fn expiry(&self, cum: u64, now: i64) -> Option<i64> {
        self.entries.get(&cum).map(|e| e.0).filter(|e| *e > now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chain(n: usize) -> Vec<(u64, u32)> {
        (0..n)
            .map(|i| (1000 + i as u64, (i as u32 + 1) * 1000))
            .collect()
    }
    const R: CacheRules = CacheRules {
        min_tokens: 1024,
        lookback: 20,
    };

    #[test]
    fn write_then_read_then_expire() {
        let mut c = ProviderCache::default();
        let ch = chain(4);
        let u = c.apply(&ch[..3], &[2], Ttl::M5, 0, R);
        assert_eq!((u.read, u.write, u.fresh), (0, 3000, 0));
        let u = c.apply(&ch, &[3], Ttl::M5, 60_000, R);
        assert_eq!((u.read, u.write, u.fresh, u.hit), (3000, 1000, 0, Some(2)));
        // The read at 60s restarted the 5 minute lifetime of the old entry.
        assert_eq!(c.expiry(ch[2].0, 61_000), Some(360_000));
        let u = c.peek(&ch, &[3], 60_000 + 300_001, R);
        assert_eq!((u.read, u.write), (0, 4000));
    }

    #[test]
    fn no_breakpoint_means_no_cache_and_min_length_applies() {
        let mut c = ProviderCache::default();
        let ch = chain(3);
        assert_eq!(c.apply(&ch, &[], Ttl::M5, 0, R).fresh, 3000);
        assert!(c.is_empty());
        let small = [(1u64, 500u32), (2, 900)];
        let u = c.apply(&small, &[1], Ttl::M5, 0, R);
        assert_eq!((u.write, u.fresh), (0, 900));
    }

    #[test]
    fn lookback_is_twenty_blocks() {
        let mut c = ProviderCache::default();
        let ch = chain(40);
        c.apply(&ch[..5], &[4], Ttl::M5, 0, R);
        // Breakpoint 20 blocks later: the old entry is inside the window.
        assert_eq!(c.peek(&ch[..24], &[23], 1, R).hit, Some(4));
        assert_eq!(c.peek(&ch[..25], &[24], 1, R).hit, None);
        // A second breakpoint near the old one rescues the read.
        assert_eq!(c.peek(&ch[..25], &[6, 24], 1, R).hit, Some(4));
    }

    #[test]
    fn one_hour_entries_outlive_five_minute_ones() {
        let mut c = ProviderCache::default();
        let ch = chain(2);
        c.apply(&ch, &[1], Ttl::H1, 0, R);
        assert_eq!(c.peek(&ch, &[1], 3_000_000, R).read, 2000);
        assert_eq!(c.peek(&ch, &[1], 3_600_001, R).read, 0);
    }
}
