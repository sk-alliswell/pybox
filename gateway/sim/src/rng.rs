//! Deterministic random numbers (splitmix64), so a scenario and a seed always
//! give the same run on every platform, including the browser build.

#[derive(Clone, Debug)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed ^ 0x5DEE_CE66_D1CE_4E5B)
    }

    /// A generator for one independent stream (an arrival process, a server).
    pub fn fork(&mut self, salt: u64) -> Rng {
        Rng(self.next_u64() ^ salt.wrapping_mul(0x9E37_79B9_7F4A_7C15))
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in [0, 1).
    pub fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    pub fn range(&mut self, lo: f64, hi: f64) -> f64 {
        lo + (hi - lo) * self.unit()
    }

    pub fn below(&mut self, n: usize) -> usize {
        ((self.unit() * n as f64) as usize).min(n.saturating_sub(1))
    }

    /// Exponential with the given mean.
    pub fn exp(&mut self, mean: f64) -> f64 {
        -mean * (1.0 - self.unit()).max(1e-300).ln()
    }

    pub fn normal(&mut self) -> f64 {
        let (a, b) = (self.unit().max(1e-300), self.unit());
        (-2.0 * a.ln()).sqrt() * (std::f64::consts::TAU * b).cos()
    }

    /// Log-normal with the given median; `sigma` is the spread of the log.
    pub fn lognormal(&mut self, median: f64, sigma: f64) -> f64 {
        if sigma <= 0.0 {
            return median;
        }
        median * (sigma * self.normal()).exp()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn moments() {
        let mut r = Rng::new(7);
        let n = 200_000;
        let mean = (0..n).map(|_| r.exp(2.0)).sum::<f64>() / n as f64;
        assert!((mean - 2.0).abs() < 0.03, "{mean}");
        let mut v: Vec<f64> = (0..n).map(|_| r.lognormal(5.0, 0.4)).collect();
        v.sort_by(f64::total_cmp);
        assert!((v[n / 2] - 5.0).abs() < 0.05);
        let u = (0..n).map(|_| r.unit()).sum::<f64>() / n as f64;
        assert!((u - 0.5).abs() < 0.005);
    }

    #[test]
    fn same_seed_same_stream() {
        let (mut a, mut b) = (Rng::new(1), Rng::new(1));
        assert!((0..100).all(|_| a.next_u64() == b.next_u64()));
    }
}
