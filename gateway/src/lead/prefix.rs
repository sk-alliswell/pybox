//! Which model server still holds which prompt prefix.
//!
//! A model server keeps the attention state (KV cache) of the prompts it
//! read last, and reads only what differs when the next prompt starts the
//! same way. Which prompts it keeps is not reported, so the gateway keeps
//! its own picture: for each backend, the prompts it was sent most recently,
//! as many as the server has cache slots. A request is then worth sending to
//! the backend whose remembered prompts share the longest start with it.
//!
//! Prompts are compared block by block through their cumulative hashes (see
//! `fingerprint`), so two prompts share a prefix exactly as far as those
//! hashes agree. The server compares token by token and may reuse a little
//! more than this picture says, never less while the entry is really there.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

/// (cumulative hash, cumulative estimated tokens) per prompt block.
pub type Chain = Arc<[(u64, u32)]>;

/// How requests are steered towards warm caches.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Affinity {
    /// Load only.
    Off,
    /// The server that answered the run's previous turn.
    Run,
    /// The server that holds the longest matching prompt prefix, whoever
    /// sent it.
    #[default]
    Prefix,
}

impl Affinity {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "off" => Some(Self::Off),
            "run" => Some(Self::Run),
            "prefix" => Some(Self::Prefix),
            _ => None,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Run => "run",
            Self::Prefix => "prefix",
        }
    }
}

/// Share of a new prompt that must match a remembered one for the server
/// to continue in that prompt's slot (llama.cpp `--slot-prompt-similarity`).
const SLOT_SIMILARITY: f64 = 0.10;
/// Prompts pushed out lately that are remembered per backend, to notice a
/// server that keeps more than believed.
const GHOSTS: usize = 16;
/// How far the learned number of slots may grow past the configured one.
const SLOTS_MAX: usize = 256;

#[derive(Default)]
struct Held {
    /// Most recently used first.
    lru: VecDeque<(Chain, i64)>,
    /// Every cumulative hash of every remembered prompt, with how many of
    /// them contain it.
    index: HashMap<u64, u32>,
    slots: usize,
    /// Tail hashes of prompts pushed out lately.
    ghosts: VecDeque<u64>,
    /// The server has reported reused prompt tokens at least once, so a
    /// report of none means none.
    reports: bool,
    /// Times the belief was corrected from a server's report.
    corrected: u64,
}

/// Blocks two prompts share from the start.
fn common(a: &[(u64, u32)], b: &[(u64, u32)]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x.0 == y.0).count()
}

impl Held {
    fn add(&mut self, c: &Chain) {
        for (h, _) in c.iter() {
            *self.index.entry(*h).or_default() += 1;
        }
    }
    fn drop_chain(&mut self, c: &Chain) {
        for (h, _) in c.iter() {
            if let Some(n) = self.index.get_mut(h) {
                *n -= 1;
                if *n == 0 {
                    self.index.remove(h);
                }
            }
        }
    }
    fn evict_last(&mut self) {
        if let Some((old, _)) = self.lru.pop_back() {
            self.drop_chain(&old);
            if let Some(t) = old.last() {
                self.ghosts.push_front(t.0);
                self.ghosts.truncate(GHOSTS);
            }
        }
    }
    /// Blocks of `chain` this backend holds, counted from the start.
    fn depth(&self, chain: &[(u64, u32)]) -> usize {
        // A remembered prompt contains all of its own prefixes, so "held"
        // is true up to some block and false after it.
        let (mut lo, mut hi) = (0, chain.len());
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.index.contains_key(&chain[mid].0) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }
    /// The slot the server would continue this prompt in: the remembered
    /// prompt sharing the longest start with it, if that is a large enough
    /// share of the new prompt.
    fn slot_for(&self, chain: &[(u64, u32)]) -> Option<usize> {
        let total = f64::from(chain.last()?.1.max(1));
        let (i, d) = self
            .lru
            .iter()
            .enumerate()
            .map(|(i, (c, _))| (i, common(c, chain)))
            .max_by_key(|&(i, d)| (d, std::cmp::Reverse(i)))?;
        (d > 0 && f64::from(chain[d - 1].1) / total >= SLOT_SIMILARITY).then_some(i)
    }
}

/// What the map believed about a request when it was routed, kept to be
/// checked against what the server reports afterwards.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Belief {
    /// Estimated tokens believed held, and in the prompt.
    pub warm: u32,
    pub total: u32,
    /// Position (0 = most recent) of the remembered prompt that matched.
    pub rank: Option<usize>,
    /// Nothing matched, but a prompt pushed out lately would have.
    pub ghost: bool,
}

/// The gateway's picture of every backend's prefix cache.
pub struct PrefixMap {
    per: Vec<Held>,
    /// Entries unused for this long are forgotten (0 = never).
    ttl_ms: i64,
}

impl PrefixMap {
    /// `slots[i]` is how many prompts backend `i` keeps.
    pub fn new(slots: &[usize], ttl_ms: i64) -> Self {
        Self {
            per: slots
                .iter()
                .map(|&s| Held {
                    slots: s.max(1),
                    ..Held::default()
                })
                .collect(),
            ttl_ms,
        }
    }

    /// Estimated tokens of `chain` that backend `b` can reuse.
    pub fn warm(&self, b: usize, chain: &[(u64, u32)]) -> u32 {
        let Some(h) = self.per.get(b) else { return 0 };
        match h.depth(chain) {
            0 => 0,
            d => chain[d - 1].1,
        }
    }

    /// What is believed about `chain` on backend `b` right now.
    pub fn belief(&self, b: usize, chain: &[(u64, u32)]) -> Belief {
        let total = chain.last().map_or(0, |c| c.1);
        let Some(h) = self.per.get(b) else {
            return Belief::default();
        };
        let d = h.depth(chain);
        let rank = (d > 0)
            .then(|| h.lru.iter().position(|(c, _)| common(c, chain) >= d))
            .flatten();
        let ghost = d == 0
            && chain
                .iter()
                .any(|(hash, _)| h.ghosts.iter().any(|g| g == hash));
        Belief {
            warm: if d > 0 { chain[d - 1].1 } else { 0 },
            total,
            rank,
            ghost,
        }
    }

    /// How long ago the prompt that `chain` would push out of backend `b`
    /// was last used. `None` when nothing would be pushed out: a slot is
    /// empty, or the prompt continues in a slot of its own kind.
    pub fn victim_idle(&self, b: usize, chain: &[(u64, u32)], now: i64) -> Option<i64> {
        let h = self.per.get(b)?;
        if h.lru.len() < h.slots || h.slot_for(chain).is_some() {
            return None;
        }
        h.lru.back().map(|(_, t)| now - *t)
    }

    /// Backend `b` was sent this prompt.
    pub fn note(&mut self, b: usize, chain: &Chain, now: i64) {
        let Some(h) = self.per.get_mut(b) else { return };
        if chain.is_empty() {
            return;
        }
        // The server continues in the slot whose prompt is most like this
        // one, and that slot then holds this prompt instead.
        if let Some(i) = h.slot_for(chain) {
            let (old, _) = h.lru.remove(i).expect("position is in range");
            h.drop_chain(&old);
        }
        h.add(chain);
        h.lru.push_front((chain.clone(), now));
        while h.lru.len() > h.slots {
            h.evict_last();
        }
    }

    /// The server answered and said how much of the prompt it reused
    /// (`cached` of `cached + fresh` tokens). Where that contradicts what
    /// was believed at routing time, the believed number of slots moves:
    /// down to where the lost prompt stood, or up by one when a prompt
    /// thought to be pushed out was still there.
    pub fn observe(&mut self, b: usize, was: Belief, cached: u64, fresh: u64) {
        let Some(h) = self.per.get_mut(b) else { return };
        if cached > 0 {
            h.reports = true;
        }
        if !h.reports || cached + fresh == 0 || was.total == 0 {
            return;
        }
        let got = cached as f64 / (cached + fresh) as f64;
        let expected = f64::from(was.warm) / f64::from(was.total);
        if let (Some(rank), true) = (was.rank, expected >= 0.3 && got < expected / 2.0) {
            // It was gone: the server keeps fewer prompts than that.
            let keep = rank.max(1);
            if keep < h.slots {
                h.slots = keep;
                h.corrected += 1;
                while h.lru.len() > h.slots {
                    h.evict_last();
                }
            }
        } else if was.ghost && got >= 0.3 && h.slots < SLOTS_MAX {
            h.slots += 1;
            h.corrected += 1;
        }
    }

    /// Forget what has not been used for the configured time.
    pub fn expire(&mut self, now: i64) {
        if self.ttl_ms <= 0 {
            return;
        }
        for h in &mut self.per {
            while h.lru.back().is_some_and(|(_, t)| now - *t > self.ttl_ms) {
                let (old, _) = h.lru.pop_back().expect("checked above");
                h.drop_chain(&old);
            }
        }
    }

    /// The backend failed or was restarted: its cache is gone.
    pub fn clear(&mut self, b: usize) {
        if let Some(h) = self.per.get_mut(b) {
            h.lru.clear();
            h.index.clear();
            h.ghosts.clear();
        }
    }

    pub fn entries(&self, b: usize) -> usize {
        self.per.get(b).map_or(0, |h| h.lru.len())
    }
    /// Prompts backend `b` is believed to keep (configured, then learned).
    pub fn slots(&self, b: usize) -> usize {
        self.per.get(b).map_or(0, |h| h.slots)
    }
    pub fn corrections(&self, b: usize) -> u64 {
        self.per.get(b).map_or(0, |h| h.corrected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A prompt made of blocks: each number is one block of 100 tokens.
    fn chain(blocks: &[u64]) -> Chain {
        let mut cum = 7u64;
        blocks
            .iter()
            .enumerate()
            .map(|(i, b)| {
                cum = cum.wrapping_mul(1_000_003).wrapping_add(*b);
                (cum, 100 * (i as u32 + 1))
            })
            .collect::<Vec<_>>()
            .into()
    }

    #[test]
    fn warm_is_the_shared_start() {
        let mut m = PrefixMap::new(&[2, 2], 0);
        m.note(0, &chain(&[1, 2, 3]), 0);
        assert_eq!(m.warm(0, &chain(&[1, 2, 3, 4])), 300);
        assert_eq!(m.warm(0, &chain(&[1, 2, 9])), 200);
        assert_eq!(m.warm(0, &chain(&[8, 2, 3])), 0);
        assert_eq!(m.warm(1, &chain(&[1, 2, 3])), 0);
    }

    #[test]
    fn a_server_keeps_as_many_prompts_as_it_has_slots() {
        let mut m = PrefixMap::new(&[2], 0);
        m.note(0, &chain(&[1, 2]), 0);
        m.note(0, &chain(&[5, 6]), 1);
        assert_eq!(m.warm(0, &chain(&[1, 2, 3])), 200);
        // A third conversation pushes out the one used longest ago.
        m.note(0, &chain(&[8, 9]), 2);
        assert_eq!(m.warm(0, &chain(&[1, 2, 3])), 0);
        assert_eq!(m.warm(0, &chain(&[5, 6, 7])), 200);
        assert_eq!(m.entries(0), 2);
    }

    #[test]
    fn the_next_turn_reuses_its_conversations_slot() {
        let mut m = PrefixMap::new(&[2], 0);
        m.note(0, &chain(&[1, 2]), 0);
        m.note(0, &chain(&[5, 6]), 1);
        m.note(0, &chain(&[1, 2, 3]), 2);
        assert_eq!(m.entries(0), 2);
        assert_eq!(m.warm(0, &chain(&[5, 6, 7])), 200, "still there");
        assert_eq!(m.warm(0, &chain(&[1, 2, 3, 4])), 300);
        // The same prompt again does not take a second slot either.
        m.note(0, &chain(&[1, 2, 3]), 3);
        assert_eq!(m.entries(0), 2);
    }

    #[test]
    fn a_prompt_like_a_remembered_one_takes_over_its_slot() {
        let mut m = PrefixMap::new(&[2], 0);
        m.note(0, &chain(&[1, 2]), 0);
        m.note(0, &chain(&[5, 6]), 1);
        // Same first block, different question: the server continues in
        // that slot, so the other conversation is not pushed out.
        m.note(0, &chain(&[1, 3]), 2);
        assert_eq!(m.entries(0), 2);
        assert_eq!(m.warm(0, &chain(&[5, 6, 7])), 200);
        assert_eq!(m.warm(0, &chain(&[1, 2])), 100, "only the shared block");
        assert_eq!(m.warm(0, &chain(&[1, 3, 4])), 200);
        // A start too short to count (under a tenth of the prompt) does not.
        let long: Vec<u64> = std::iter::once(5).chain(100..111).collect();
        m.note(0, &chain(&long), 3);
        assert_eq!(
            m.warm(0, &chain(&[5, 6, 7])),
            100,
            "pushed out as the oldest"
        );
        assert_eq!(m.warm(0, &chain(&[1, 3, 4])), 200);
    }

    #[test]
    fn nothing_is_pushed_out_while_a_slot_is_free_or_the_kind_has_one() {
        let mut m = PrefixMap::new(&[2], 0);
        assert_eq!(m.victim_idle(0, &chain(&[1]), 50), None);
        m.note(0, &chain(&[1, 2]), 10);
        m.note(0, &chain(&[5, 6]), 30);
        assert_eq!(m.victim_idle(0, &chain(&[1, 9]), 50), None, "its own slot");
        assert_eq!(m.victim_idle(0, &chain(&[8]), 50), Some(40));
    }

    #[test]
    fn a_servers_report_corrects_how_many_prompts_it_is_believed_to_keep() {
        let mut m = PrefixMap::new(&[3], 0);
        for (t, b) in [1u64, 2, 3].iter().enumerate() {
            m.note(0, &chain(&[*b, 9]), t as i64);
        }
        let was = m.belief(0, &chain(&[1, 9, 9]));
        assert_eq!((was.warm, was.total, was.rank), (200, 300, Some(2)));
        // A server that never reported reuse tells us nothing by reporting none.
        m.observe(0, was, 0, 300);
        assert_eq!(m.slots(0), 3);
        // Once it has, "none" where two thirds were expected means the
        // third-newest prompt was already gone: it keeps two.
        m.observe(0, m.belief(0, &chain(&[3, 9, 9])), 200, 100);
        m.observe(0, was, 0, 300);
        assert_eq!((m.slots(0), m.entries(0)), (2, 2));
        assert_eq!(m.warm(0, &chain(&[1, 9])), 0);
        // The other way round: a prompt believed pushed out was still there.
        let ghost = m.belief(0, &chain(&[1, 9, 9]));
        assert!(ghost.ghost && ghost.warm == 0);
        m.observe(0, ghost, 200, 100);
        assert_eq!(m.slots(0), 3);
        assert_eq!(m.corrections(0), 2);
    }

    #[test]
    fn idle_entries_expire_and_a_failed_server_starts_empty() {
        let mut m = PrefixMap::new(&[4, 4], 1000);
        m.note(0, &chain(&[1]), 0);
        m.note(0, &chain(&[2]), 900);
        m.note(1, &chain(&[1]), 900);
        m.expire(1500);
        assert_eq!(m.warm(0, &chain(&[1])), 0);
        assert_eq!(m.warm(0, &chain(&[2])), 100);
        m.clear(1);
        assert_eq!(m.warm(1, &chain(&[1])), 0);
    }
}
