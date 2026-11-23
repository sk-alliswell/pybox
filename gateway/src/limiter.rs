//! Token-bucket rate limiting plus a shared cooldown set by upstream 429s.

use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct Bucket {
    cap: f64,
    tokens: f64,
    per_sec: f64,
    last: Instant,
}

impl Bucket {
    /// A bucket that allows `per_minute` units per minute, bursting up to one minute's worth.
    pub fn per_minute(per_minute: u32, now: Instant) -> Self {
        let cap = per_minute.max(1) as f64;
        Self {
            cap,
            tokens: cap,
            per_sec: cap / 60.0,
            last: now,
        }
    }

    fn refill(&mut self, now: Instant) {
        let dt = now.saturating_duration_since(self.last).as_secs_f64();
        self.tokens = (self.tokens + dt * self.per_sec).min(self.cap);
        self.last = now;
    }

    /// How long until `n` units are available. Requests larger than the bucket
    /// are clamped so they can still run (after a full refill).
    pub fn wait_for(&mut self, n: f64, now: Instant) -> Duration {
        self.refill(now);
        let n = n.min(self.cap);
        if self.tokens >= n {
            Duration::ZERO
        } else {
            Duration::from_secs_f64((n - self.tokens) / self.per_sec)
        }
    }

    pub fn take(&mut self, n: f64, now: Instant) {
        self.refill(now);
        self.tokens -= n.min(self.cap);
    }

    /// Correct an earlier estimate. May go negative, which delays later requests.
    pub fn adjust(&mut self, delta: f64) {
        self.tokens = (self.tokens - delta).min(self.cap);
    }
}

#[derive(Debug, Default)]
pub struct RateLimiter {
    requests: Option<Bucket>,
    input_tokens: Option<Bucket>,
    cooldown_until: Option<Instant>,
}

impl RateLimiter {
    pub fn new(rpm: Option<u32>, input_tpm: Option<u32>) -> Self {
        let now = Instant::now();
        Self {
            requests: rpm.map(|r| Bucket::per_minute(r, now)),
            input_tokens: input_tpm.map(|t| Bucket::per_minute(t, now)),
            cooldown_until: None,
        }
    }

    pub fn delay(&mut self, cost: u32, now: Instant) -> Duration {
        let mut d = Duration::ZERO;
        if let Some(until) = self.cooldown_until {
            d = d.max(until.saturating_duration_since(now));
        }
        if let Some(b) = &mut self.requests {
            d = d.max(b.wait_for(1.0, now));
        }
        if let Some(b) = &mut self.input_tokens {
            d = d.max(b.wait_for(cost as f64, now));
        }
        d
    }

    pub fn take(&mut self, cost: u32, now: Instant) {
        if let Some(b) = &mut self.requests {
            b.take(1.0, now);
        }
        if let Some(b) = &mut self.input_tokens {
            b.take(cost as f64, now);
        }
    }

    /// Replace an input-token estimate with the real count once known.
    pub fn correct_tokens(&mut self, estimated: u32, actual: u64) {
        if let Some(b) = &mut self.input_tokens {
            b.adjust(actual as f64 - estimated as f64);
        }
    }

    /// Pause all dispatch for this upstream (e.g. after a 429).
    pub fn cooldown(&mut self, d: Duration, now: Instant) {
        let until = now + d;
        if self.cooldown_until.is_none_or(|u| u < until) {
            self.cooldown_until = Some(until);
        }
    }

    pub fn cooldown_remaining(&self, now: Instant) -> Duration {
        self.cooldown_until
            .map(|u| u.saturating_duration_since(now))
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_refills_over_time() {
        let t0 = Instant::now();
        let mut b = Bucket::per_minute(60, t0);
        b.take(60.0, t0);
        assert!(b.wait_for(1.0, t0) > Duration::from_millis(900));
        assert_eq!(b.wait_for(1.0, t0 + Duration::from_secs(1)), Duration::ZERO);
    }

    #[test]
    fn oversized_requests_are_clamped() {
        let t0 = Instant::now();
        let mut b = Bucket::per_minute(100, t0);
        assert_eq!(b.wait_for(1_000.0, t0), Duration::ZERO);
    }

    #[test]
    fn cooldown_extends_but_never_shrinks() {
        let t0 = Instant::now();
        let mut l = RateLimiter::new(None, None);
        l.cooldown(Duration::from_secs(5), t0);
        l.cooldown(Duration::from_secs(1), t0);
        assert_eq!(l.delay(0, t0), Duration::from_secs(5));
    }
}
