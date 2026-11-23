//! OpenTelemetry GenAI semantic conventions, at the gateway's edges.
//!
//! Stored rows keep the gateway's own column names; this module maps them to
//! the convention's attribute names on the way out (span attributes, OTLP
//! export, metrics) and folds older or vendor attribute names into the
//! current ones on the way in.
//!
//! Written against the conventions in `open-telemetry/semantic-conventions-genai`
//! as of 2026-11 (status: Development). The names that changed most recently:
//! `gen_ai.system` became `gen_ai.provider.name`, cache-creation tokens are
//! `gen_ai.usage.cache_write.input_tokens`, and token metrics are the
//! `gen_ai.client.inference.usage.*` counters.

use serde_json::{json, Map, Value};

use crate::config::Kind;

/// `gen_ai.operation.name` for a request path.
pub fn operation(path: &str) -> &'static str {
    let p = path.split('?').next().unwrap_or(path);
    if p.ends_with("/embeddings") || p.contains(":embedContent") || p.contains(":batchEmbed") {
        "embeddings"
    } else if p.ends_with("/chat/completions")
        || p.ends_with("/messages")
        || p.ends_with("/responses")
    {
        "chat"
    } else if p.contains(":generateContent") || p.contains(":streamGenerateContent") {
        "generate_content"
    } else if p.ends_with("/completions") {
        "text_completion"
    } else {
        "chat"
    }
}

/// `gen_ai.provider.name`: the configured value, else a guess from the
/// upstream kind, its host and the model name.
pub fn provider(kind: Kind, configured: Option<&str>, base_url: &str, model: &str) -> String {
    if let Some(p) = configured {
        return p.to_string();
    }
    let host = base_url
        .split("://")
        .nth(1)
        .unwrap_or(base_url)
        .split('/')
        .next()
        .unwrap_or("");
    let by_host = if host.contains("anthropic.com") {
        Some("anthropic")
    } else if host.contains("openai.azure.com") {
        Some("azure.ai.openai")
    } else if host.contains("openai.com") {
        Some("openai")
    } else if host.contains("generativelanguage.googleapis.com") {
        Some("gcp.gemini")
    } else if host.contains("aiplatform.googleapis.com") {
        Some("gcp.vertex_ai")
    } else if host.contains("amazonaws.com") {
        Some("aws.bedrock")
    } else {
        None
    };
    if let Some(p) = by_host {
        return p.to_string();
    }
    // Behind a router such as LiteLLM the model name is the best hint.
    let m = model.rsplit('/').next().unwrap_or(model);
    let by_model = if m.starts_with("claude") {
        Some("anthropic")
    } else if m.starts_with("gpt")
        || m.starts_with("o1")
        || m.starts_with("o3")
        || m.starts_with("o4")
    {
        Some("openai")
    } else if m.starts_with("gemini") {
        Some("gcp.gemini")
    } else {
        None
    };
    match (by_model, kind) {
        (Some(p), _) => p.to_string(),
        (None, Kind::Anthropic) => "anthropic".into(),
        (None, Kind::Openai) => "openai".into(),
        // Routers and local servers: the convention's own escape hatch.
        (None, Kind::Litellm | Kind::None) => "_OTHER".into(),
    }
}

/// `host` and `port` of a base URL, for `server.address` / `server.port`.
pub fn server_of(base_url: &str) -> (String, Option<u16>) {
    let (scheme, rest) = base_url.split_once("://").unwrap_or(("http", base_url));
    let authority = rest.split('/').next().unwrap_or("");
    let authority = authority.rsplit('@').next().unwrap_or(authority);
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) if !h.ends_with(']') || !p.is_empty() => match p.parse::<u16>() {
            Ok(port) => (h, Some(port)),
            Err(_) => (authority, None),
        },
        _ => (authority, None),
    };
    let port = port.or(match scheme {
        "https" => Some(443),
        "http" => Some(80),
        _ => None,
    });
    (host.trim_matches(['[', ']']).to_string(), port)
}

/// Everything the root span of a model call says about it.
#[derive(Debug, Default)]
pub struct Inference<'a> {
    pub operation: &'a str,
    pub provider: &'a str,
    pub request_model: &'a str,
    pub response_model: &'a str,
    pub response_id: &'a str,
    pub finish_reason: &'a str,
    pub stream: bool,
    pub max_tokens: Option<u64>,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_write_tokens: u64,
    pub ttft_ms: Option<f64>,
    pub error_type: Option<&'a str>,
    pub server_address: &'a str,
    pub server_port: Option<u16>,
    pub conversation_id: &'a str,
    pub agent_id: &'a str,
}

/// Attributes of a `gen_ai.client.inference` span.
pub fn inference_attrs(i: &Inference) -> Map<String, Value> {
    let mut m = Map::new();
    let mut put = |k: &str, v: Value| {
        m.insert(k.to_string(), v);
    };
    put("gen_ai.operation.name", json!(i.operation));
    put("gen_ai.provider.name", json!(i.provider));
    if !i.request_model.is_empty() {
        put("gen_ai.request.model", json!(i.request_model));
    }
    if i.stream {
        put("gen_ai.request.stream", json!(true));
    }
    if let Some(n) = i.max_tokens {
        put("gen_ai.request.max_tokens", json!(n));
    }
    if !i.response_model.is_empty() {
        put("gen_ai.response.model", json!(i.response_model));
    }
    if !i.response_id.is_empty() {
        put("gen_ai.response.id", json!(i.response_id));
    }
    if !i.finish_reason.is_empty() {
        put("gen_ai.response.finish_reasons", json!([i.finish_reason]));
    }
    if let Some(ms) = i.ttft_ms {
        put("gen_ai.response.time_to_first_chunk", json!(ms / 1000.0));
    }
    if i.input_tokens + i.output_tokens > 0 {
        // Input tokens include cached ones, as the convention asks.
        put("gen_ai.usage.input_tokens", json!(i.input_tokens));
        put("gen_ai.usage.output_tokens", json!(i.output_tokens));
        if i.cache_read_tokens > 0 {
            put(
                "gen_ai.usage.cache_read.input_tokens",
                json!(i.cache_read_tokens),
            );
        }
        if i.cache_write_tokens > 0 {
            put(
                "gen_ai.usage.cache_write.input_tokens",
                json!(i.cache_write_tokens),
            );
        }
    }
    if let Some(e) = i.error_type {
        put("error.type", json!(e));
    }
    if !i.server_address.is_empty() {
        put("server.address", json!(i.server_address));
        if let Some(p) = i.server_port {
            put("server.port", json!(p));
        }
    }
    if !i.conversation_id.is_empty() {
        put("gen_ai.conversation.id", json!(i.conversation_id));
    }
    if !i.agent_id.is_empty() {
        put("gen_ai.agent.id", json!(i.agent_id));
    }
    m
}

/// Older GenAI convention names, OpenLLMetry (`llm.*` / `gen_ai.*` legacy)
/// and OpenInference names, mapped to the current convention. LiteLLM and
/// most agent SDKs still emit some of the left-hand side.
const ALIASES: &[(&str, &str)] = &[
    ("gen_ai.system", "gen_ai.provider.name"),
    ("gen_ai.usage.prompt_tokens", "gen_ai.usage.input_tokens"),
    (
        "gen_ai.usage.completion_tokens",
        "gen_ai.usage.output_tokens",
    ),
    (
        "gen_ai.usage.cache_creation_input_tokens",
        "gen_ai.usage.cache_write.input_tokens",
    ),
    (
        "gen_ai.usage.cache_creation.input_tokens",
        "gen_ai.usage.cache_write.input_tokens",
    ),
    (
        "gen_ai.usage.cache_read_input_tokens",
        "gen_ai.usage.cache_read.input_tokens",
    ),
    (
        "gen_ai.usage.reasoning_tokens",
        "gen_ai.usage.reasoning.output_tokens",
    ),
    ("gen_ai.openai.request.seed", "gen_ai.request.seed"),
    (
        "gen_ai.response.finish_reason",
        "gen_ai.response.finish_reasons",
    ),
    (
        "gen_ai.completion.finish_reason",
        "gen_ai.response.finish_reasons",
    ),
    ("llm.request.type", "gen_ai.operation.name"),
    ("llm.is_streaming", "gen_ai.request.stream"),
    (
        "llm.usage.cache_read_input_tokens",
        "gen_ai.usage.cache_read.input_tokens",
    ),
    (
        "llm.usage.cache_creation_input_tokens",
        "gen_ai.usage.cache_write.input_tokens",
    ),
    // OpenInference.
    ("llm.model_name", "gen_ai.request.model"),
    ("llm.provider", "gen_ai.provider.name"),
    ("llm.token_count.prompt", "gen_ai.usage.input_tokens"),
    ("llm.token_count.completion", "gen_ai.usage.output_tokens"),
    (
        "llm.token_count.prompt_details.cache_read",
        "gen_ai.usage.cache_read.input_tokens",
    ),
    (
        "llm.token_count.prompt_details.cache_write",
        "gen_ai.usage.cache_write.input_tokens",
    ),
    ("session.id", "gen_ai.conversation.id"),
    ("tool.name", "gen_ai.tool.name"),
];

/// Old provider names that the convention renamed.
const PROVIDER_VALUES: &[(&str, &str)] = &[
    ("vertex_ai", "gcp.vertex_ai"),
    ("vertexai", "gcp.vertex_ai"),
    ("gemini", "gcp.gemini"),
    ("google", "gcp.gemini"),
    ("bedrock", "aws.bedrock"),
    ("aws", "aws.bedrock"),
    ("azure", "azure.ai.openai"),
    ("az.ai.openai", "azure.ai.openai"),
    ("az.ai.inference", "azure.ai.inference"),
    ("mistralai", "mistral_ai"),
];

/// Rewrite received span attributes to the current convention. The original
/// key is dropped; a current-name attribute already present wins.
pub fn normalize_ingest(attrs: &mut Map<String, Value>) {
    for (old, new) in ALIASES {
        if let Some(v) = attrs.remove(*old) {
            let v = match (*new, v) {
                // A single finish reason becomes the one-element list.
                ("gen_ai.response.finish_reasons", Value::String(s)) => json!([s]),
                ("gen_ai.operation.name", Value::String(s)) if s == "completion" => {
                    json!("text_completion")
                }
                (_, v) => v,
            };
            attrs.entry(new.to_string()).or_insert(v);
        }
    }
    if let Some(Value::String(p)) = attrs.get_mut("gen_ai.provider.name") {
        let lower = p.to_ascii_lowercase();
        *p = PROVIDER_VALUES
            .iter()
            .find(|(old, _)| *old == lower)
            .map_or(lower.clone(), |(_, new)| new.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operations_from_paths() {
        assert_eq!(operation("/v1/chat/completions"), "chat");
        assert_eq!(operation("/v1/messages?beta=true"), "chat");
        assert_eq!(operation("/v1/responses"), "chat");
        assert_eq!(operation("/v1/embeddings"), "embeddings");
        assert_eq!(operation("/v1/completions"), "text_completion");
        assert_eq!(
            operation("/v1beta/models/gemini-2.5-flash:streamGenerateContent?alt=sse"),
            "generate_content"
        );
    }

    #[test]
    fn providers_from_host_then_model() {
        let p = |k, url: &str, m: &str| provider(k, None, url, m);
        assert_eq!(
            p(Kind::Anthropic, "https://api.anthropic.com", "x"),
            "anthropic"
        );
        assert_eq!(
            p(
                Kind::Openai,
                "https://generativelanguage.googleapis.com/v1beta/openai",
                "gemini-2.5-flash"
            ),
            "gcp.gemini"
        );
        assert_eq!(
            p(
                Kind::Litellm,
                "http://10.0.0.11:4000",
                "anthropic/claude-sonnet-5"
            ),
            "anthropic"
        );
        assert_eq!(
            p(Kind::Litellm, "http://10.0.0.11:4000", "team-model"),
            "_OTHER"
        );
        assert_eq!(
            provider(Kind::Litellm, Some("aws.bedrock"), "http://x", "claude"),
            "aws.bedrock"
        );
        assert_eq!(
            server_of("https://api.openai.com/v1"),
            ("api.openai.com".into(), Some(443))
        );
        assert_eq!(
            server_of("http://10.0.0.11:4000"),
            ("10.0.0.11".into(), Some(4000))
        );
    }

    #[test]
    fn inference_attributes_use_current_names() {
        let a = inference_attrs(&Inference {
            operation: "chat",
            provider: "anthropic",
            request_model: "claude-sonnet-5",
            response_model: "claude-sonnet-5-20260101",
            finish_reason: "end_turn",
            stream: true,
            input_tokens: 1200,
            output_tokens: 40,
            cache_read_tokens: 1000,
            cache_write_tokens: 100,
            ttft_ms: Some(250.0),
            ..Inference::default()
        });
        assert_eq!(a["gen_ai.provider.name"], "anthropic");
        assert_eq!(a["gen_ai.usage.input_tokens"], 1200);
        assert_eq!(a["gen_ai.usage.cache_write.input_tokens"], 100);
        assert_eq!(a["gen_ai.response.finish_reasons"], json!(["end_turn"]));
        assert_eq!(a["gen_ai.response.time_to_first_chunk"], 0.25);
        assert!(a.get("error.type").is_none());
    }

    #[test]
    fn legacy_attributes_are_folded_in() {
        let mut m = Map::new();
        m.insert("gen_ai.system".into(), json!("vertex_ai"));
        m.insert("gen_ai.usage.prompt_tokens".into(), json!(10));
        m.insert("gen_ai.usage.input_tokens".into(), json!(12));
        m.insert("llm.token_count.completion".into(), json!(5));
        m.insert("gen_ai.response.finish_reason".into(), json!("stop"));
        normalize_ingest(&mut m);
        assert_eq!(m["gen_ai.provider.name"], "gcp.vertex_ai");
        // The current name was already there and wins.
        assert_eq!(m["gen_ai.usage.input_tokens"], 12);
        assert_eq!(m["gen_ai.usage.output_tokens"], 5);
        assert_eq!(m["gen_ai.response.finish_reasons"], json!(["stop"]));
        assert!(m.get("gen_ai.system").is_none());
        assert!(m.get("gen_ai.usage.prompt_tokens").is_none());
    }
}
