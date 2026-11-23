//! Clearing old tool results from a conversation before it is sent.
//!
//! Long agent runs carry every tool result they ever received. This replaces
//! the content of old ones with a one-line note, keeping the most recent
//! `keep_last` in full. Unlike cache markers this changes what the model
//! sees, so it is off unless asked for and can be run as an experiment.
//!
//! Which results are cleared is a boundary that only moves forward. Moving
//! it rewrites the prompt from that point on, which throws away whatever
//! the provider (or a local server's prefix cache) had stored after it, so
//! the caller moves it only when the tokens saved on later turns outweigh
//! that one rewrite. The note depends only on the size of what it replaces,
//! so a cleared prefix is byte-identical from turn to turn.

use std::ops::Range;

use axum::body::Bytes;

use super::fingerprint::Fingerprint;
use super::json::members;

#[derive(Debug, Clone, Copy)]
pub struct TrimCfg {
    /// Most recent tool results left untouched.
    pub keep_last: usize,
    /// Results smaller than this are not worth replacing.
    pub min_bytes: usize,
}

/// Byte range of the `content` value of each tool result, in order.
pub fn tool_results(body: &[u8], fp: &Fingerprint) -> Vec<Range<usize>> {
    fp.blocks
        .iter()
        .filter(|b| b.tool_result)
        .filter_map(|b| {
            members(body, b.start)?
                .into_iter()
                .find(|(k, _)| &body[k.clone()] == b"content")
                .map(|(_, v)| v)
        })
        .collect()
}

/// The furthest the boundary may go: everything but the last `keep_last`.
pub fn limit(results: usize, cfg: TrimCfg) -> usize {
    results.saturating_sub(cfg.keep_last)
}

fn note(bytes: usize) -> String {
    format!("\"[tool result cleared by the gateway to save tokens: {bytes} bytes]\"")
}

/// The body with the first `boundary` tool results cleared. Returns the
/// bytes and how many results were replaced.
pub fn clear(
    body: &Bytes,
    results: &[Range<usize>],
    boundary: usize,
    cfg: TrimCfg,
) -> (Bytes, usize) {
    let mut out = Vec::with_capacity(body.len());
    let (mut at, mut n) = (0, 0);
    for r in results.iter().take(boundary) {
        if r.len() < cfg.min_bytes.max(note(r.len()).len() + 1) {
            continue;
        }
        out.extend_from_slice(&body[at..r.start]);
        out.extend_from_slice(note(r.len()).as_bytes());
        at = r.end;
        n += 1;
    }
    if n == 0 {
        return (body.clone(), 0);
    }
    out.extend_from_slice(&body[at..]);
    (Bytes::from(out), n)
}

#[cfg(test)]
mod tests {
    use super::super::fingerprint::fingerprint;
    use super::*;
    use serde_json::Value;

    const CFG: TrimCfg = TrimCfg {
        keep_last: 1,
        min_bytes: 100,
    };

    #[test]
    fn clears_old_results_and_nothing_else() {
        let big = "x".repeat(400);
        let body = Bytes::from(format!(
            r#"{{"model":"m","messages":[
              {{"role":"user","content":[{{"type":"text","text":"go"}}]}},
              {{"role":"assistant","content":[{{"type":"tool_use","id":"a","name":"t","input":{{}}}}]}},
              {{"role":"user","content":[{{"type":"tool_result","tool_use_id":"a","content":"{big}"}}]}},
              {{"role":"assistant","content":[{{"type":"tool_use","id":"b","name":"t","input":{{}}}}]}},
              {{"role":"user","content":[{{"type":"tool_result","tool_use_id":"b","content":[{{"type":"text","text":"{big}"}}]}}]}},
              {{"role":"assistant","content":[{{"type":"tool_use","id":"c","name":"t","input":{{}}}}]}},
              {{"role":"user","content":[{{"type":"tool_result","tool_use_id":"c","content":"{big} last"}}]}}
            ]}}"#
        ));
        let fp = fingerprint(&body, true).unwrap();
        let rs = tool_results(&body, &fp);
        assert_eq!((rs.len(), limit(rs.len(), CFG)), (3, 2));
        let (out, n) = clear(&body, &rs, 2, CFG);
        assert_eq!(n, 2);
        let (v, orig): (Value, Value) = (
            serde_json::from_slice(&out).unwrap(),
            serde_json::from_slice(&body).unwrap(),
        );
        let m = v["messages"].as_array().unwrap();
        for i in [2, 4] {
            let c = &m[i]["content"][0];
            assert!(c["content"]
                .as_str()
                .unwrap()
                .starts_with("[tool result cleared"));
            assert_eq!(
                c["tool_use_id"],
                orig["messages"][i]["content"][0]["tool_use_id"]
            );
        }
        // The newest result and every other message are untouched.
        for i in [0, 1, 3, 5, 6] {
            assert_eq!(m[i], orig["messages"][i]);
        }
        assert!(out.len() < body.len() - 600);
        // Same boundary, longer conversation: the cleared part is the same bytes.
        let prefix = std::str::from_utf8(&out)
            .unwrap()
            .find(r#""id":"c""#)
            .unwrap();
        let (again, _) = clear(&body, &rs, 2, CFG);
        assert_eq!(out[..prefix], again[..prefix]);
        // Nothing to do: the very same buffer comes back.
        assert_eq!(clear(&body, &rs, 0, CFG).1, 0);
    }

    #[test]
    fn works_on_openai_tool_messages_and_skips_small_ones() {
        let big = "y".repeat(300);
        let body = Bytes::from(format!(
            r#"{{"model":"m","messages":[{{"role":"user","content":"go"}},
              {{"role":"tool","tool_call_id":"1","content":"{big}"}},
              {{"role":"tool","tool_call_id":"2","content":"tiny"}},
              {{"role":"tool","tool_call_id":"3","content":"{big}"}}]}}"#
        ));
        let fp = fingerprint(&body, false).unwrap();
        let rs = tool_results(&body, &fp);
        let (out, n) = clear(&body, &rs, 2, CFG);
        assert_eq!(n, 1);
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert!(
            v["messages"][1]["content"]
                .as_str()
                .unwrap()
                .contains("300 bytes")
                || v["messages"][1]["content"]
                    .as_str()
                    .unwrap()
                    .contains("302 bytes")
        );
        assert_eq!(v["messages"][2]["content"], "tiny");
        assert_eq!(v["messages"][3]["content"].as_str().unwrap().len(), 300);
    }
}
