//! Prefix fingerprint: one cumulative hash per prompt block, in the order
//! providers build their cache prefix (tools, then system, then messages).
//!
//! Two requests share a cacheable prefix exactly as far as their cumulative
//! hashes agree. `cache_control` markers are left out of the hash because
//! they move from turn to turn without changing what the model reads.

use super::json::{elems, hash, members, skip_value, ws};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BKind {
    Tool,
    System,
    Msg,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Ttl {
    #[default]
    M5,
    H1,
}

impl Ttl {
    pub fn ms(self) -> i64 {
        match self {
            Ttl::M5 => 300_000,
            Ttl::H1 => 3_600_000,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Ttl::M5 => "5m",
            Ttl::H1 => "1h",
        }
    }
}

#[derive(Clone, Debug)]
pub struct Block {
    pub kind: BKind,
    /// Byte range of the block in the body.
    pub start: usize,
    pub end: usize,
    /// Hash of everything up to and including this block.
    pub cum: u64,
    /// Estimated tokens up to and including this block.
    pub tok: u32,
    /// The client put a cache breakpoint here.
    pub cc: Option<Ttl>,
    /// The block is a JSON object, so a breakpoint can be attached to it.
    pub obj: bool,
    /// A tool result (candidate for context trimming).
    pub tool_result: bool,
}

#[derive(Clone, Debug, Default)]
pub struct Fingerprint {
    pub model: u64,
    /// Request options that affect the message-level cache (tool_choice,
    /// thinking and so on).
    pub opts: u64,
    pub blocks: Vec<Block>,
    pub n_tools: usize,
    pub n_system: usize,
    /// Top-level `cache_control` (Anthropic automatic caching).
    pub auto_cc: Option<Ttl>,
    /// Offset of the closing brace of the top-level object.
    pub top_end: usize,
    /// Order-insensitive hash of the tool definitions.
    pub tools_set: u64,
    /// Hash of the system prompt with digits and long hex runs masked.
    pub sys_masked: u64,
    pub has_cache_key: bool,
    /// Hash of the first conversation message alone, whatever precedes it.
    pub first_msg: u64,
    /// The API caches prefixes on its own (OpenAI style).
    pub implicit_cache: bool,
}

impl Fingerprint {
    pub fn total_tokens(&self) -> u32 {
        self.blocks.last().map_or(0, |b| b.tok)
    }
    pub fn tail(&self) -> u64 {
        self.blocks.last().map_or(self.model, |b| b.cum)
    }
    /// Tokens in tools plus system.
    pub fn static_tokens(&self) -> u32 {
        let n = self.n_tools + self.n_system;
        if n == 0 {
            0
        } else {
            self.blocks[n - 1].tok
        }
    }
    pub fn client_breakpoints(&self) -> bool {
        self.auto_cc.is_some() || self.blocks.iter().any(|b| b.cc.is_some())
    }
    /// Block indexes where the provider will read or write its cache.
    pub fn breakpoints(&self) -> Vec<usize> {
        if self.implicit_cache {
            return (0..self.blocks.len()).collect();
        }
        let mut v: Vec<usize> = self
            .blocks
            .iter()
            .enumerate()
            .filter(|(_, b)| b.cc.is_some())
            .map(|(i, _)| i)
            .collect();
        if self.auto_cc.is_some() && !self.blocks.is_empty() {
            let last = self.blocks.len() - 1;
            if v.last() != Some(&last) {
                v.push(last);
            }
        }
        v
    }
    pub fn ttl(&self) -> Ttl {
        let any_h1 =
            self.auto_cc == Some(Ttl::H1) || self.blocks.iter().any(|b| b.cc == Some(Ttl::H1));
        if any_h1 {
            Ttl::H1
        } else {
            Ttl::M5
        }
    }
}

const SKIP_TOP: &[&[u8]] = &[
    b"tools",
    b"system",
    b"messages",
    b"input",
    b"model",
    b"stream",
    b"max_tokens",
    b"max_completion_tokens",
    b"max_output_tokens",
    b"metadata",
    b"cache_control",
    b"stream_options",
    b"prompt_cache_key",
    b"user",
];

fn ttl_of(v: &[u8]) -> Ttl {
    if v.windows(4).any(|w| w == b"\"1h\"") {
        Ttl::H1
    } else {
        Ttl::M5
    }
}

/// Hash one block, skipping its `cache_control` member. Returns (hash, cc).
fn hash_block(b: &[u8], start: usize, end: usize, seed: u64) -> (u64, Option<Ttl>, bool, bool) {
    if b[start] != b'{' {
        return (hash(seed, &b[start..end]), None, false, false);
    }
    let Some(ms) = members(b, start) else {
        return (hash(seed, &b[start..end]), None, false, false);
    };
    let mut h = seed ^ 0x6f62_6a65_6374;
    let mut cc = None;
    let mut tool_result = false;
    for (k, v) in ms {
        let key = &b[k.clone()];
        if key == b"cache_control" {
            if &b[v.clone()] != b"null" {
                cc = Some(ttl_of(&b[v]));
            }
            continue;
        }
        if key == b"type" && &b[v.clone()] == b"\"tool_result\"" {
            tool_result = true;
        }
        if key == b"role" && &b[v.clone()] == b"\"tool\"" {
            tool_result = true;
        }
        h = hash(h, key);
        h = hash(h, &b[v]);
    }
    (h, cc, true, tool_result)
}

fn masked_hash(seed: u64, bytes: &[u8]) -> u64 {
    // Digits become '0'; runs of 16+ hex/dash characters collapse to '#'.
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        if c.is_ascii_hexdigit() || c == b'-' {
            let s = i;
            while i < bytes.len() && (bytes[i].is_ascii_hexdigit() || bytes[i] == b'-') {
                i += 1;
            }
            if i - s >= 16 {
                out.push(b'#');
            } else {
                out.extend(
                    bytes[s..i]
                        .iter()
                        .map(|c| if c.is_ascii_digit() { b'0' } else { *c }),
                );
            }
        } else {
            out.push(c);
            i += 1;
        }
    }
    hash(seed, &out)
}

/// Fingerprint a chat request body. `None` when it is not a JSON object with
/// a message list.
pub fn fingerprint(body: &[u8], anthropic: bool) -> Option<Fingerprint> {
    let start = ws(body, 0);
    let top = members(body, start)?;
    let top_end = skip_value(body, start)? - 1;
    let find = |name: &[u8]| {
        top.iter()
            .find(|(k, _)| &body[k.clone()] == name)
            .map(|(_, v)| v.clone())
    };
    let msgs = find(b"messages").or_else(|| find(b"input"))?;
    if body[msgs.start] != b'[' {
        return None;
    }
    let mut fp = Fingerprint {
        top_end,
        implicit_cache: !anthropic,
        has_cache_key: find(b"prompt_cache_key").is_some(),
        ..Fingerprint::default()
    };
    fp.model = find(b"model").map_or(1, |v| hash(0x006d_6f64_656c, &body[v]));
    if let Some(v) = find(b"cache_control") {
        if &body[v.clone()] != b"null" {
            fp.auto_cc = Some(ttl_of(&body[v]));
        }
    }
    let mut opts = 0x6f70_7473u64;
    for (k, v) in &top {
        let key = &body[k.clone()];
        if SKIP_TOP.contains(&key) {
            continue;
        }
        opts = hash(hash(opts, key), &body[v.clone()]);
    }
    fp.opts = opts;

    let mut cum = fp.model;
    let mut bytes = 0usize;
    let mut push = |fp: &mut Fingerprint, kind: BKind, s: usize, e: usize, extra: u64| {
        let (h, cc, obj, tool_result) = hash_block(body, s, e, cum ^ extra);
        cum = h;
        bytes += e - s;
        fp.blocks.push(Block {
            kind,
            start: s,
            end: e,
            cum,
            tok: (bytes / 4) as u32,
            cc,
            obj,
            tool_result,
        });
        h
    };

    if let Some(v) = find(b"tools") {
        if body[v.start] == b'[' {
            for e in elems(body, v.start)? {
                let before = fp.blocks.len();
                push(&mut fp, BKind::Tool, e.start, e.end, 0);
                let (own, ..) = hash_block(body, e.start, e.end, 0x746f_6f6c);
                fp.tools_set = fp.tools_set.wrapping_add(own);
                debug_assert_eq!(fp.blocks.len(), before + 1);
            }
        }
    }
    fp.n_tools = fp.blocks.len();
    let mut sys_masked = 0x0073_7973_u64;
    if let Some(v) = find(b"system") {
        if body[v.start] == b'[' {
            for e in elems(body, v.start)? {
                push(&mut fp, BKind::System, e.start, e.end, 0);
                sys_masked = masked_hash(sys_masked, &body[e]);
            }
        } else if &body[v.clone()] != b"null" {
            push(&mut fp, BKind::System, v.start, v.end, 0);
            sys_masked = masked_hash(sys_masked, &body[v]);
        }
    }
    for m in elems(body, msgs.start)? {
        let parts = members(body, m.start);
        let role = parts.as_ref().and_then(|p| {
            p.iter()
                .find(|(k, _)| &body[k.clone()] == b"role")
                .map(|(_, v)| v.clone())
        });
        let content = parts.as_ref().and_then(|p| {
            p.iter()
                .find(|(k, _)| &body[k.clone()] == b"content")
                .map(|(_, v)| v.clone())
        });
        let role_bytes = role.map_or(&b""[..], |r| &body[r]);
        let system_msg = !anthropic && matches!(role_bytes, b"\"system\"" | b"\"developer\"");
        let kind = if system_msg {
            BKind::System
        } else {
            BKind::Msg
        };
        // Anthropic content arrays carry one cacheable block per element.
        let split = anthropic && content.as_ref().is_some_and(|c| body[c.start] == b'[');
        if split {
            let role_seed = hash(0x726f_6c65, role_bytes);
            let items = elems(body, content.unwrap().start)?;
            if items.is_empty() {
                push(&mut fp, kind, m.start, m.end, 0);
            }
            for (i, e) in items.into_iter().enumerate() {
                push(
                    &mut fp,
                    kind,
                    e.start,
                    e.end,
                    if i == 0 { role_seed } else { 0 },
                );
            }
        } else {
            let n = fp.blocks.len();
            push(&mut fp, kind, m.start, m.end, 0);
            // A breakpoint cannot hang on a message object itself.
            fp.blocks[n].obj = false;
            if system_msg {
                sys_masked = masked_hash(sys_masked, &body[m]);
            }
        }
    }
    if !anthropic {
        // System messages lead the list in OpenAI-style requests.
        fp.n_system = fp.blocks[fp.n_tools..]
            .iter()
            .take_while(|b| b.kind == BKind::System)
            .count();
    } else {
        fp.n_system = fp.blocks.iter().filter(|b| b.kind == BKind::System).count();
    }
    if let Some(b) = fp.blocks.iter().find(|b| b.kind == BKind::Msg) {
        fp.first_msg = hash_block(body, b.start, b.end, 0x6669_7273).0;
    }
    fp.sys_masked = sys_masked;
    Some(fp)
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: &str = r#"{"model":"claude-x","max_tokens":100,"tools":[{"name":"a","input_schema":{}},{"name":"b","input_schema":{}}],"system":[{"type":"text","text":"You are helpful."}],"messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]}"#;

    #[test]
    fn blocks_follow_cache_order() {
        let fp = fingerprint(A.as_bytes(), true).unwrap();
        let kinds: Vec<BKind> = fp.blocks.iter().map(|b| b.kind).collect();
        assert_eq!(kinds, [BKind::Tool, BKind::Tool, BKind::System, BKind::Msg]);
        assert_eq!((fp.n_tools, fp.n_system), (2, 1));
        assert!(!fp.client_breakpoints());
        assert_eq!(A.as_bytes()[fp.top_end], b'}');
    }

    #[test]
    fn next_turn_extends_the_chain() {
        let next = A.replace(
            r#"{"role":"user","content":[{"type":"text","text":"hi"}]}"#,
            r#"{"role":"user","content":[{"type":"text","text":"hi"}]},{"role":"assistant","content":[{"type":"text","text":"hello"}]},{"role":"user","content":"more"}"#,
        );
        let (a, b) = (
            fingerprint(A.as_bytes(), true).unwrap(),
            fingerprint(next.as_bytes(), true).unwrap(),
        );
        assert_eq!(b.blocks.len(), 6);
        for i in 0..a.blocks.len() {
            assert_eq!(a.blocks[i].cum, b.blocks[i].cum, "block {i}");
        }
        assert_eq!(a.tail(), b.blocks[3].cum);
    }

    #[test]
    fn cache_control_does_not_change_the_hash() {
        let with = A.replace(
            r#"{"type":"text","text":"You are helpful."}"#,
            r#"{"type":"text","text":"You are helpful.","cache_control":{"type":"ephemeral","ttl":"1h"}}"#,
        );
        let (a, b) = (
            fingerprint(A.as_bytes(), true).unwrap(),
            fingerprint(with.as_bytes(), true).unwrap(),
        );
        assert_eq!(a.tail(), b.tail());
        assert_eq!(b.blocks[2].cc, Some(Ttl::H1));
        assert_eq!(b.breakpoints(), [2]);
        let auto = A.replacen(
            "{\"model\"",
            "{\"cache_control\":{\"type\":\"ephemeral\"},\"model\"",
            1,
        );
        let c = fingerprint(auto.as_bytes(), true).unwrap();
        assert_eq!(c.tail(), a.tail());
        assert_eq!(c.breakpoints(), [3]);
    }

    #[test]
    fn tool_order_and_volatile_system_are_detectable() {
        let swapped = A.replace(
            r#"[{"name":"a","input_schema":{}},{"name":"b","input_schema":{}}]"#,
            r#"[{"name":"b","input_schema":{}},{"name":"a","input_schema":{}}]"#,
        );
        let (a, b) = (
            fingerprint(A.as_bytes(), true).unwrap(),
            fingerprint(swapped.as_bytes(), true).unwrap(),
        );
        assert_ne!(a.blocks[0].cum, b.blocks[0].cum);
        assert_eq!(a.tools_set, b.tools_set);
        let t1 = A.replace(
            "You are helpful.",
            "Now 2026-11-12T08:00:01Z id 0f8fad5b-d9cb-469f-a165-70867728950e.",
        );
        let t2 = A.replace(
            "You are helpful.",
            "Now 2026-11-12T09:13:44Z id 7c9e6679-7425-40de-944b-e07fc1f90ae7.",
        );
        let (x, y) = (
            fingerprint(t1.as_bytes(), true).unwrap(),
            fingerprint(t2.as_bytes(), true).unwrap(),
        );
        assert_ne!(x.blocks[2].cum, y.blocks[2].cum);
        assert_eq!(x.sys_masked, y.sys_masked);
        assert_ne!(x.sys_masked, a.sys_masked);
    }

    #[test]
    fn openai_shape() {
        let o = r#"{"model":"gpt","messages":[{"role":"system","content":"s"},{"role":"user","content":"u"},{"role":"tool","tool_call_id":"1","content":"r"}]}"#;
        let fp = fingerprint(o.as_bytes(), false).unwrap();
        assert_eq!((fp.n_tools, fp.n_system, fp.blocks.len()), (0, 1, 3));
        assert!(fp.implicit_cache && fp.blocks[2].tool_result);
        assert_eq!(fp.breakpoints(), [0, 1, 2]);
        assert!(fingerprint(b"{\"model\":\"m\"}", true).is_none());
        assert!(fingerprint(b"not json", true).is_none());
    }
}
