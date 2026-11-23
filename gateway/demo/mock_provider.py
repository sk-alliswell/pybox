#!/usr/bin/env python3
"""OpenAI-compatible mock model server for the observability demo.

Streams chat completions with per-model latency, reports cached prompt tokens
when it has seen the same prompt prefix before (like a KV/prefix cache does),
and fails now and then so retries and errors show up.
"""
import hashlib, json, random, sys, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

MODELS = {  # first token delay s, seconds per output token
    "mock-fast": (0.12, 0.004),
    "mock-smart": (0.45, 0.012),
}
seen_prefixes = set()


class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers.get("content-length", 0))) or b"{}")
        model = body.get("model", "mock-fast")
        first, per_tok = MODELS.get(model, MODELS["mock-fast"])
        text = json.dumps(body.get("messages", []))
        prompt = max(8, len(text) // 4)
        prefix = hashlib.sha1(text[:400].encode()).hexdigest()
        cached = int(prompt * 0.8) if prefix in seen_prefixes and prompt > 200 else 0
        seen_prefixes.add(prefix)
        roll = random.random()
        if roll < 0.03:
            return self.send_json(429, {"error": {"message": "rate limited", "type": "rate_limit_error"}}, {"retry-after-ms": "150"})
        if roll < 0.045:
            return self.send_json(500, {"error": {"message": "backend exploded", "type": "server_error"}})
        out = random.randint(20, 160)
        usage = {"prompt_tokens": prompt, "completion_tokens": out, "total_tokens": prompt + out,
                 "prompt_tokens_details": {"cached_tokens": cached}}
        first *= random.uniform(0.7, 1.6) * (0.55 if cached else 1.0)
        if not body.get("stream"):
            time.sleep(first + out * per_tok)
            return self.send_json(200, {"id": "chatcmpl-mock", "object": "chat.completion", "model": model,
                                        "choices": [{"index": 0, "finish_reason": "stop", "message": {"role": "assistant", "content": "word " * out}}],
                                        "usage": usage})
        self.send_response(200)
        self.send_header("content-type", "text/event-stream")
        self.send_header("transfer-encoding", "chunked")
        self.end_headers()

        def ev(obj):
            data = ("data: " + (obj if isinstance(obj, str) else json.dumps(obj)) + "\n\n").encode()
            self.wfile.write(b"%x\r\n%s\r\n" % (len(data), data))
            self.wfile.flush()

        base = {"id": "chatcmpl-mock", "object": "chat.completion.chunk", "model": model}
        try:
            ev({**base, "choices": [{"index": 0, "delta": {"role": "assistant", "content": ""}}]})
            time.sleep(first)
            for i in range(0, out, 4):
                ev({**base, "choices": [{"index": 0, "delta": {"content": "word " * 4}}]})
                time.sleep(per_tok * 4 * random.uniform(0.6, 1.5))
            ev({**base, "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]})
            if (body.get("stream_options") or {}).get("include_usage"):
                ev({**base, "choices": [], "usage": usage})
            ev("[DONE]")
            self.wfile.write(b"0\r\n\r\n")
        except (BrokenPipeError, ConnectionResetError):
            pass

    def send_json(self, status, obj, headers=None):
        data = json.dumps(obj).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(data)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(data)


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9101
    ThreadingHTTPServer(("127.0.0.1", port), H).serve_forever()
