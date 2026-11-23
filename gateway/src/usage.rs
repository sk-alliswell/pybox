//! Extracts token usage from Anthropic / OpenAI style responses, streamed
//! (SSE) or not, without altering the bytes passed to the client.

use std::sync::LazyLock;

use memchr::memmem::Finder;
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Usage {
    pub input: u64,
    pub output: u64,
    pub cache_read: u64,
    pub cache_write: u64,
}

impl Usage {
    pub fn total(&self) -> u64 {
        self.input + self.output + self.cache_read + self.cache_write
    }
}

// Built once: these run on every line of every stream.
static USAGE: LazyLock<Finder<'static>> = LazyLock::new(|| Finder::new(b"\"usage\""));
static FINISH: LazyLock<Finder<'static>> = LazyLock::new(|| Finder::new(b"\"finish_reason\""));

const MAX_LINE: usize = 1 << 20;
const MAX_BODY: usize = 4 << 20;

/// What the response says about itself, for traces and reports.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ResponseMeta {
    pub id: String,
    /// The model that actually answered (may differ from the one asked for).
    pub model: String,
    /// `stop_reason` / `finish_reason`, e.g. `end_turn`, `max_tokens`, `stop`.
    pub finish_reason: String,
}

pub struct UsageParser {
    meta: ResponseMeta,
    sse: bool,
    buf: Vec<u8>,
    overflow: bool,
    usage: Usage,
    content_seen: bool,
    /// Events that arrived after the first piece of model output.
    content_events: u64,
}

impl UsageParser {
    pub fn new(sse: bool) -> Self {
        Self {
            meta: ResponseMeta::default(),
            sse,
            buf: Vec::new(),
            overflow: false,
            usage: Usage::default(),
            content_seen: false,
            content_events: 0,
        }
    }

    pub fn feed(&mut self, chunk: &[u8]) {
        if self.overflow {
            if !self.sse {
                return;
            }
            // Resynchronise at the next line break.
            match chunk.iter().position(|&b| b == b'\n') {
                Some(i) => {
                    self.overflow = false;
                    return self.feed(&chunk[i + 1..]);
                }
                None => return,
            }
        }
        if !self.sse {
            self.buf.extend_from_slice(chunk);
            if self.buf.len() > MAX_BODY {
                self.overflow = true;
                self.buf = Vec::new();
            }
            return;
        }
        // Complete lines are examined in place; only a trailing partial line
        // is copied, so a typical chunk costs one scan and no allocation.
        let mut rest = chunk;
        if !self.buf.is_empty() {
            let Some(i) = memchr::memchr(b'\n', rest) else {
                self.buf.extend_from_slice(rest);
                if self.buf.len() > MAX_LINE {
                    self.buf.clear();
                    self.overflow = true;
                }
                return;
            };
            self.buf.extend_from_slice(&rest[..i]);
            let line = std::mem::take(&mut self.buf);
            self.line(&line);
            self.buf = line;
            self.buf.clear();
            rest = &rest[i + 1..];
        }
        while let Some(i) = memchr::memchr(b'\n', rest) {
            self.line(&rest[..i]);
            rest = &rest[i + 1..];
        }
        if rest.len() > MAX_LINE {
            self.overflow = true;
        } else {
            self.buf.extend_from_slice(rest);
        }
    }

    /// One SSE line. Parsing every event as JSON was the single largest cost
    /// on the streaming path, so only two kinds of line are parsed: those
    /// before the first content delta (to time the first token) and those
    /// that mention `"usage"`.
    fn line(&mut self, line: &[u8]) {
        let Some(data) = line.strip_prefix(b"data:") else {
            return;
        };
        if self.content_seen && !has_usage(data) && !has_finish_reason(data) {
            self.content_events += 1;
            return;
        }
        let data = data.trim_ascii();
        if data == b"[DONE]" {
            return;
        }
        if let Ok(v) = serde_json::from_slice::<Value>(data) {
            absorb(&mut self.usage, &v);
            absorb_meta(&mut self.meta, &v);
            self.content_seen |= has_content(&v);
        }
    }

    pub fn meta(&self) -> &ResponseMeta {
        &self.meta
    }

    /// True once a streamed event carried model output (text, reasoning or
    /// tool-call deltas). Marks the first token for TTFT.
    pub fn content_seen(&self) -> bool {
        self.content_seen
    }

    /// Roughly how many tokens have been streamed so far: providers send
    /// about one event per token. Only an estimate, for streams that end
    /// before the provider reports usage.
    pub fn output_estimate(&self) -> u64 {
        self.content_events + u64::from(self.content_seen)
    }

    pub fn finish(&mut self) -> Usage {
        if !self.sse && !self.overflow {
            if let Ok(v) = serde_json::from_slice::<Value>(&self.buf) {
                absorb(&mut self.usage, &v);
                absorb_meta(&mut self.meta, &v);
            }
            self.buf.clear();
        }
        self.usage
    }
}

/// The first byte of the value following a key that ends at `at`.
fn value_start(data: &[u8], at: usize) -> Option<u8> {
    data[at..]
        .iter()
        .find(|b| !matches!(b, b' ' | b':'))
        .copied()
}

/// True when `data` has a `usage` key with a value. With `include_usage`
/// on, OpenAI puts `"usage":null` on every chunk but the last, so the bare
/// key is not enough to justify a parse.
pub(crate) fn has_usage(data: &[u8]) -> bool {
    const KEY_LEN: usize = 7; // "usage"
    USAGE
        .find_iter(data)
        .any(|i| value_start(data, i + KEY_LEN).is_some_and(|b| b != b'n'))
}

/// True when the line carries a non-null `finish_reason` (the last content
/// chunk of an OpenAI-style stream). Every other chunk says
/// `"finish_reason":null`, so this is a byte check, not a parse.
fn has_finish_reason(data: &[u8]) -> bool {
    const KEY_LEN: usize = 15; // "finish_reason"
    FINISH
        .find_iter(data)
        .any(|i| value_start(data, i + KEY_LEN) == Some(b'"'))
}

fn absorb_meta(m: &mut ResponseMeta, v: &Value) {
    fn text(v: Option<&Value>) -> Option<&str> {
        v.and_then(Value::as_str).filter(|s| !s.is_empty())
    }
    // Anthropic nests the message under `message` in `message_start`.
    let head = v.get("message").filter(|x| x.is_object()).unwrap_or(v);
    if m.id.is_empty() {
        if let Some(id) = text(head.get("id")) {
            m.id = id.chars().take(128).collect();
        }
    }
    if m.model.is_empty() {
        if let Some(model) = text(head.get("model")) {
            m.model = model.chars().take(128).collect();
        }
    }
    let finish = text(v.get("stop_reason"))
        .or_else(|| text(v.get("delta").and_then(|d| d.get("stop_reason"))))
        .or_else(|| {
            v.get("choices")?
                .as_array()?
                .iter()
                .find_map(|c| text(c.get("finish_reason")))
        });
    if let Some(f) = finish {
        m.finish_reason = f.chars().take(64).collect();
    }
}

fn has_content(v: &Value) -> bool {
    // Anthropic: content_block_delta events.
    if v.get("type").and_then(Value::as_str) == Some("content_block_delta") {
        return true;
    }
    // OpenAI: any non-empty field in choices[].delta other than the role.
    v.get("choices")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|c| c.get("delta")?.as_object())
        .any(|d| {
            d.iter().any(|(k, val)| {
                k != "role"
                    && match val {
                        Value::String(s) => !s.is_empty(),
                        Value::Array(a) => !a.is_empty(),
                        Value::Object(o) => !o.is_empty(),
                        _ => false,
                    }
            })
        })
}

/// Merge any `usage` object found at the top level or under `message`
/// (Anthropic `message_start`). Counts are cumulative in both APIs, so the
/// maximum seen per field is the final value.
fn absorb(u: &mut Usage, v: &Value) {
    for obj in [
        v.get("usage"),
        v.get("message").and_then(|m| m.get("usage")),
    ]
    .into_iter()
    .flatten()
    {
        let n = |k: &str| obj.get(k).and_then(Value::as_u64);
        let mx = |slot: &mut u64, val: Option<u64>| {
            if let Some(x) = val {
                *slot = (*slot).max(x);
            }
        };
        mx(&mut u.input, n("input_tokens").or(n("prompt_tokens")));
        mx(&mut u.output, n("output_tokens").or(n("completion_tokens")));
        mx(
            &mut u.cache_read,
            n("cache_read_input_tokens").or_else(|| {
                obj.get("prompt_tokens_details")
                    .and_then(|d| d.get("cached_tokens"))
                    .and_then(Value::as_u64)
            }),
        );
        mx(&mut u.cache_write, n("cache_creation_input_tokens"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anthropic_stream_split_across_chunks() {
        let body = concat!(
            "event: message_start\n",
            "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":120,\"cache_read_input_tokens\":1000,\"output_tokens\":1}}}\n\n",
            "event: content_block_delta\n",
            "data: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"hi\"}}\n\n",
            "event: message_delta\n",
            "data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":42}}\n\n",
        );
        let mut p = UsageParser::new(true);
        for chunk in body.as_bytes().chunks(7) {
            p.feed(chunk);
        }
        let u = p.finish();
        assert_eq!(
            u,
            Usage {
                input: 120,
                output: 42,
                cache_read: 1000,
                cache_write: 0
            }
        );
    }

    #[test]
    fn detects_first_content_delta() {
        let mut p = UsageParser::new(true);
        p.feed(b"data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"\"}}]}\n\n");
        assert!(!p.content_seen());
        p.feed(b"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n");
        assert!(p.content_seen());
        let mut a = UsageParser::new(true);
        a.feed(
            b"data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":1}}}\n\n",
        );
        assert!(!a.content_seen());
        a.feed(b"data: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"x\"}}\n\n");
        assert!(a.content_seen());
    }

    #[test]
    fn response_metadata_from_streams_and_bodies() {
        let mut a = UsageParser::new(true);
        a.feed(b"data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"model\":\"claude-sonnet-5-20260101\",\"usage\":{\"input_tokens\":1}}}\n\n");
        a.feed(b"data: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"x\"}}\n\n");
        a.feed(b"data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"max_tokens\"},\"usage\":{\"output_tokens\":9}}\n\n");
        assert_eq!(
            a.meta(),
            &ResponseMeta {
                id: "msg_1".into(),
                model: "claude-sonnet-5-20260101".into(),
                finish_reason: "max_tokens".into()
            }
        );
        // OpenAI: the finish reason rides on a chunk with no usage in it, in
        // either spacing providers use.
        for finish in [
            "\"finish_reason\":\"length\"",
            "\"finish_reason\": \"length\"",
        ] {
            let mut o = UsageParser::new(true);
            o.feed(b"data: {\"id\":\"chatcmpl-1\",\"model\":\"gpt-x\",\"choices\":[{\"delta\":{\"content\":\"hi\"},\"finish_reason\":null}]}\n\n");
            o.feed(
                format!(
                    "data: {{\"id\":\"chatcmpl-1\",\"choices\":[{{\"delta\":{{}},{finish}}}]}}\n\n"
                )
                .as_bytes(),
            );
            assert_eq!(o.meta().finish_reason, "length");
            assert_eq!(o.meta().model, "gpt-x");
        }
        let mut j = UsageParser::new(false);
        j.feed(br#"{"id":"msg_2","model":"m","stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":2}}"#);
        j.finish();
        assert_eq!(j.meta().finish_reason, "end_turn");
        assert_eq!(j.meta().id, "msg_2");
    }

    #[test]
    fn null_usage_on_every_chunk_is_not_parsed_but_the_real_one_is() {
        assert!(!has_usage(
            br#"{"choices":[{"delta":{"content":"x"}}],"usage":null}"#
        ));
        assert!(!has_usage(br#"{"choices":[],"usage": null}"#));
        assert!(has_usage(br#"{"choices":[],"usage":{"prompt_tokens":3}}"#));
        assert!(has_usage(br#"{"usage":null,"x":{"usage": {"a":1}}}"#));
        let mut p = UsageParser::new(true);
        p.feed(b"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"},\"finish_reason\":null}],\"usage\":null}\n\n");
        p.feed(
            b"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":null}\n\n",
        );
        p.feed(
            b"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":13,\"completion_tokens\":7}}\n\n",
        );
        assert_eq!(p.finish().total(), 20);
        assert_eq!(p.meta().finish_reason, "stop");
    }

    #[test]
    fn openai_json_body() {
        let mut p = UsageParser::new(false);
        p.feed(br#"{"choices":[],"usage":{"prompt_tokens":10,"completion_tokens":5,"prompt_tokens_details":{"cached_tokens":4}}}"#);
        let u = p.finish();
        assert_eq!((u.input, u.output, u.cache_read), (10, 5, 4));
    }

    #[test]
    fn openai_stream_done_marker() {
        let mut p = UsageParser::new(true);
        p.feed(b"data: {\"choices\":[{\"delta\":{}}]}\n\ndata: {\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":2}}\n\ndata: [DONE]\n\n");
        assert_eq!(p.finish().total(), 5);
    }
}
