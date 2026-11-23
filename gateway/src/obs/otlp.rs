//! Minimal OTLP/HTTP trace receiver (`ExportTraceServiceRequest`, protobuf).
//!
//! LiteLLM's `otel` callback exports its spans here, so the LiteLLM side of a
//! call is stored next to the gateway's own spans under one trace id. Only
//! the fields the gateway stores are decoded; unknown fields are skipped, as
//! protobuf requires. Hand-rolled to avoid pulling a gRPC stack into a small
//! proxy binary.

use serde_json::{json, Map, Value};

use super::store::Row;

/// Attribute values longer than this are cut. Spans are for timing and
/// attribution, not payload storage.
const MAX_ATTR_LEN: usize = 256;
const MAX_SPANS_PER_REQUEST: usize = 10_000;

struct Reader<'a> {
    buf: &'a [u8],
}

enum Field<'a> {
    Varint(u64),
    Fixed64(u64),
    Bytes(&'a [u8]),
    Fixed32,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }
    fn varint(&mut self) -> Result<u64, String> {
        let mut out = 0u64;
        for i in 0..10 {
            let Some((&b, rest)) = self.buf.split_first() else {
                return Err("truncated varint".into());
            };
            self.buf = rest;
            out |= u64::from(b & 0x7f) << (7 * i);
            if b & 0x80 == 0 {
                return Ok(out);
            }
        }
        Err("varint too long".into())
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        if self.buf.len() < n {
            return Err("truncated field".into());
        }
        let (head, rest) = self.buf.split_at(n);
        self.buf = rest;
        Ok(head)
    }
    fn next(&mut self) -> Result<Option<(u64, Field<'a>)>, String> {
        if self.buf.is_empty() {
            return Ok(None);
        }
        let tag = self.varint()?;
        let field = match tag & 7 {
            0 => Field::Varint(self.varint()?),
            1 => Field::Fixed64(u64::from_le_bytes(self.take(8)?.try_into().unwrap())),
            2 => {
                let n = self.varint()? as usize;
                Field::Bytes(self.take(n)?)
            }
            5 => {
                self.take(4)?;
                Field::Fixed32
            }
            w => return Err(format!("unsupported wire type {w}")),
        };
        Ok(Some((tag >> 3, field)))
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn text(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

fn any_value(buf: &[u8], depth: u8) -> Result<Value, String> {
    let mut r = Reader::new(buf);
    let mut out = Value::Null;
    while let Some((n, f)) = r.next()? {
        out = match (n, f) {
            (1, Field::Bytes(b)) => {
                let s = String::from_utf8_lossy(b);
                if s.len() > MAX_ATTR_LEN {
                    let cut = (0..=MAX_ATTR_LEN)
                        .rev()
                        .find(|i| s.is_char_boundary(*i))
                        .unwrap_or(0);
                    json!(format!("{}…", &s[..cut]))
                } else {
                    json!(s)
                }
            }
            (2, Field::Varint(v)) => json!(v != 0),
            (3, Field::Varint(v)) => json!(v as i64),
            (4, Field::Fixed64(v)) => {
                let x = f64::from_bits(v);
                if x.is_finite() {
                    json!(x)
                } else {
                    Value::Null
                }
            }
            // Arrays and maps: keep a compact rendering, bounded in depth.
            (5, Field::Bytes(b)) if depth < 3 => {
                let mut items = Vec::new();
                let mut rr = Reader::new(b);
                while let Some((n, f)) = rr.next()? {
                    if let (1, Field::Bytes(v)) = (n, f) {
                        if items.len() < 16 {
                            items.push(any_value(v, depth + 1)?);
                        }
                    }
                }
                Value::Array(items)
            }
            (6, Field::Bytes(b)) if depth < 3 => {
                let mut rr = Reader::new(b);
                let mut m = Map::new();
                while let Some((n, f)) = rr.next()? {
                    if let (1, Field::Bytes(kv)) = (n, f) {
                        if m.len() < 16 {
                            let (k, v) = key_value(kv, depth + 1)?;
                            m.insert(k, v);
                        }
                    }
                }
                Value::Object(m)
            }
            _ => out,
        };
    }
    Ok(out)
}

fn key_value(buf: &[u8], depth: u8) -> Result<(String, Value), String> {
    let mut r = Reader::new(buf);
    let (mut k, mut v) = (String::new(), Value::Null);
    while let Some((n, f)) = r.next()? {
        match (n, f) {
            (1, Field::Bytes(b)) => k = text(b),
            (2, Field::Bytes(b)) => v = any_value(b, depth)?,
            _ => {}
        }
    }
    Ok((k, v))
}

/// Keys that carry prompts, completions or bulky internals.
fn dropped(key: &str) -> bool {
    const BLOCK: &[&str] = &[
        "messages",
        "prompt",
        "completion",
        "content",
        "stack_trace",
        "hidden_params",
        "exception",
        "requester_",
    ];
    // Token counts are numbers, not payload, whatever their key says
    // (`gen_ai.usage.prompt_tokens`, `llm.token_count.completion`, ...).
    const KEEP: &[&str] = &["gen_ai.usage.", "llm.usage.", "llm.token_count."];
    if KEEP.iter().any(|p| key.starts_with(p)) {
        return false;
    }
    BLOCK.iter().any(|b| key.contains(b))
}

fn attributes(into: &mut Map<String, Value>, kv: &[u8]) -> Result<(), String> {
    let (k, v) = key_value(kv, 0)?;
    if !k.is_empty() && !v.is_null() && !dropped(&k) && into.len() < 96 {
        into.insert(k, v);
    }
    Ok(())
}

fn kind_name(k: u64) -> &'static str {
    match k {
        1 => "internal",
        2 => "server",
        3 => "client",
        4 => "producer",
        5 => "consumer",
        _ => "unspecified",
    }
}

fn span(buf: &[u8], service: &str, instance: &str) -> Result<Row, String> {
    let mut r = Reader::new(buf);
    let (mut trace, mut id, mut parent, mut name) =
        (String::new(), String::new(), String::new(), String::new());
    let (mut kind, mut start, mut end, mut status) = (0u64, 0u64, 0u64, 0u64);
    let mut attrs = Map::new();
    while let Some((n, f)) = r.next()? {
        match (n, f) {
            (1, Field::Bytes(b)) => trace = hex(b),
            (2, Field::Bytes(b)) => id = hex(b),
            (4, Field::Bytes(b)) => parent = hex(b),
            (5, Field::Bytes(b)) => name = text(b),
            (6, Field::Varint(v)) => kind = v,
            (7, Field::Fixed64(v)) => start = v,
            (8, Field::Fixed64(v)) => end = v,
            (9, Field::Bytes(b)) => attributes(&mut attrs, b)?,
            (15, Field::Bytes(b)) => {
                let mut rr = Reader::new(b);
                while let Some((n, f)) = rr.next()? {
                    if let (3, Field::Varint(v)) = (n, f) {
                        status = v;
                    }
                }
            }
            _ => {}
        }
    }
    if trace.len() != 32 || id.len() != 16 {
        return Err("span without valid ids".into());
    }
    // Older and vendor attribute names become the current GenAI convention.
    super::genai::normalize_ingest(&mut attrs);
    Ok([
        ("ts", json!((start / 1_000_000) as i64)),
        ("start_us", json!((start / 1_000) as i64)),
        ("duration_ms", json!(end.saturating_sub(start) as f64 / 1e6)),
        ("trace_id", json!(trace)),
        ("span_id", json!(id)),
        ("parent_span_id", json!(parent)),
        ("service", json!(service)),
        ("instance", json!(instance)),
        ("name", json!(name)),
        ("kind", json!(kind_name(kind))),
        ("status", json!(if status == 2 { "error" } else { "ok" })),
        ("attrs", Value::Object(attrs)),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect())
}

/// Decode an `ExportTraceServiceRequest` into span rows.
pub fn decode(body: &[u8]) -> Result<Vec<Row>, String> {
    let mut out = Vec::new();
    let mut top = Reader::new(body);
    while let Some((n, f)) = top.next()? {
        let (1, Field::Bytes(rs)) = (n, f) else {
            continue;
        };
        let mut resource = Map::new();
        let mut scopes = Vec::new();
        let mut r = Reader::new(rs);
        while let Some((n, f)) = r.next()? {
            match (n, f) {
                (1, Field::Bytes(res)) => {
                    let mut rr = Reader::new(res);
                    while let Some((n, f)) = rr.next()? {
                        if let (1, Field::Bytes(kv)) = (n, f) {
                            attributes(&mut resource, kv)?;
                        }
                    }
                }
                (2, Field::Bytes(ss)) => scopes.push(ss),
                _ => {}
            }
        }
        let get = |k: &str| {
            resource
                .get(k)
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        };
        let service = get("service.name");
        let instance = [get("service.instance.id"), get("host.name")]
            .into_iter()
            .find(|s| !s.is_empty())
            .unwrap_or_default();
        for ss in scopes {
            let mut r = Reader::new(ss);
            while let Some((n, f)) = r.next()? {
                if let (2, Field::Bytes(sp)) = (n, f) {
                    if out.len() >= MAX_SPANS_PER_REQUEST {
                        return Err("too many spans in one export".into());
                    }
                    out.push(span(sp, &service, &instance)?);
                }
            }
        }
    }
    Ok(out)
}

pub(crate) mod enc {
    //! Just enough of a protobuf encoder for OTLP export and test fixtures.
    pub fn varint(out: &mut Vec<u8>, mut v: u64) {
        while v >= 0x80 {
            out.push((v as u8) | 0x80);
            v >>= 7;
        }
        out.push(v as u8);
    }
    pub fn bytes(out: &mut Vec<u8>, field: u64, b: &[u8]) {
        varint(out, field << 3 | 2);
        varint(out, b.len() as u64);
        out.extend_from_slice(b);
    }
    pub fn fixed64(out: &mut Vec<u8>, field: u64, v: u64) {
        varint(out, field << 3 | 1);
        out.extend_from_slice(&v.to_le_bytes());
    }
    pub fn uint(out: &mut Vec<u8>, field: u64, v: u64) {
        varint(out, field << 3);
        varint(out, v);
    }
    pub fn kv_str(key: &str, val: &str) -> Vec<u8> {
        let mut any = Vec::new();
        bytes(&mut any, 1, val.as_bytes());
        let mut kv = Vec::new();
        bytes(&mut kv, 1, key.as_bytes());
        bytes(&mut kv, 2, &any);
        kv
    }
    #[cfg(test)]
    pub fn kv_int(key: &str, val: i64) -> Vec<u8> {
        let mut any = Vec::new();
        uint(&mut any, 3, val as u64);
        let mut kv = Vec::new();
        bytes(&mut kv, 1, key.as_bytes());
        bytes(&mut kv, 2, &any);
        kv
    }
    pub fn double(out: &mut Vec<u8>, field: u64, v: f64) {
        fixed64(out, field, v.to_bits());
    }
    pub fn kv(key: &str, any_value: &[u8]) -> Vec<u8> {
        let mut kv = Vec::new();
        bytes(&mut kv, 1, key.as_bytes());
        bytes(&mut kv, 2, any_value);
        kv
    }
}

#[cfg(test)]
pub(crate) mod testenc {
    pub use super::enc::*;
    /// One resource with one span.
    #[allow(clippy::too_many_arguments)]
    pub fn export(
        service: &str,
        trace: [u8; 16],
        id: [u8; 8],
        parent: [u8; 8],
        name: &str,
        start_ns: u64,
        end_ns: u64,
        attrs: &[Vec<u8>],
    ) -> Vec<u8> {
        let mut span = Vec::new();
        bytes(&mut span, 1, &trace);
        bytes(&mut span, 2, &id);
        bytes(&mut span, 4, &parent);
        bytes(&mut span, 5, name.as_bytes());
        uint(&mut span, 6, 2);
        fixed64(&mut span, 7, start_ns);
        fixed64(&mut span, 8, end_ns);
        for a in attrs {
            bytes(&mut span, 9, a);
        }
        let mut status = Vec::new();
        uint(&mut status, 3, 2);
        bytes(&mut span, 15, &status);
        let mut scope_spans = Vec::new();
        bytes(&mut scope_spans, 2, &span);
        let mut resource = Vec::new();
        bytes(&mut resource, 1, &kv_str("service.name", service));
        bytes(&mut resource, 1, &kv_str("service.instance.id", "inst-1"));
        let mut rs = Vec::new();
        bytes(&mut rs, 1, &resource);
        bytes(&mut rs, 2, &scope_spans);
        let mut out = Vec::new();
        bytes(&mut out, 1, &rs);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::testenc::*;
    use super::*;

    #[test]
    fn decodes_span_and_drops_payload_attributes() {
        let body = export(
            "litellm",
            [0xab; 16],
            [1; 8],
            [2; 8],
            "Received Proxy Server Request",
            1_794_000_000_000_000_000,
            1_794_000_000_250_000_000,
            &[
                kv_str("gen_ai.request.model", "gpt-4o-mini"),
                kv_int("gen_ai.usage.input_tokens", 123),
                kv_int("gen_ai.usage.completion_tokens", 45),
                kv_str("gen_ai.system", "anthropic"),
                kv_str("gen_ai.input.messages", "secret prompt"),
                kv_str("long", &"x".repeat(1000)),
            ],
        );
        let rows = decode(&body).unwrap();
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r["trace_id"], "ab".repeat(16));
        assert_eq!(r["span_id"], "01".repeat(8));
        assert_eq!(r["parent_span_id"], "02".repeat(8));
        assert_eq!(r["service"], "litellm");
        assert_eq!(r["instance"], "inst-1");
        assert_eq!(r["kind"], "server");
        assert_eq!(r["status"], "error");
        assert_eq!(r["ts"], 1_794_000_000_000i64);
        assert_eq!(r["duration_ms"], 250.0);
        assert_eq!(r["attrs"]["gen_ai.request.model"], "gpt-4o-mini");
        assert_eq!(r["attrs"]["gen_ai.usage.input_tokens"], 123);
        // Legacy names arrive under the current convention.
        assert_eq!(r["attrs"]["gen_ai.usage.output_tokens"], 45);
        assert_eq!(r["attrs"]["gen_ai.provider.name"], "anthropic");
        assert!(r["attrs"].get("gen_ai.input.messages").is_none());
        assert!(r["attrs"]["long"].as_str().unwrap().chars().count() <= MAX_ATTR_LEN + 1);
    }

    #[test]
    fn rejects_garbage_without_panicking() {
        assert!(decode(&[0x0a, 0xff, 0xff]).is_err());
        assert!(decode(&[0xff; 32]).is_err());
        assert_eq!(decode(&[]).unwrap().len(), 0);
    }
}
