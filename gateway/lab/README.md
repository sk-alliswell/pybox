# Lab: many agents, small fast models, with and without the gateway

A reproducible setup for feeling what the gateway does when a lot of agents
generate tokens at once.

```
agents ──> pybox-gateway ──> LiteLLM Proxy ──┬─> qwen-0.5b      (local CPU, llama-cpp-python)
                                             ├─> smollm2-360m   (local CPU, llama-cpp-python)
                                             ├─> gemini-flash-lite
                                             ├─> gpt-4o-mini
                                             └─> claude-haiku
```

## Bring it up

```sh
python3 -m venv venv && venv/bin/pip install 'litellm[proxy]' 'llama-cpp-python[server]' aiohttp \
    opentelemetry-sdk opentelemetry-exporter-otlp-proto-http
# two small GGUF models (about 880 MB together)
curl -LO https://huggingface.co/Qwen/Qwen2.5-0.5B-Instruct-GGUF/resolve/main/qwen2.5-0.5b-instruct-q4_k_m.gguf
curl -LO https://huggingface.co/HuggingFaceTB/SmolLM2-360M-Instruct-GGUF/resolve/main/smollm2-360m-instruct-q8_0.gguf

venv/bin/python -m llama_cpp.server --model qwen2.5-0.5b-instruct-q4_k_m.gguf --model_alias qwen-0.5b \
    --port 9201 --n_ctx 2048 --n_threads 2 --interrupt_requests false &
venv/bin/python -m llama_cpp.server --model smollm2-360m-instruct-q8_0.gguf --model_alias smollm2-360m \
    --port 9202 --n_ctx 2048 --n_threads 2 --interrupt_requests false &

# GEMINI_API_KEY, OPENAI_API_KEY and GATEWAY_ANTHROPIC_KEY must be set for the hosted models
env OTEL_EXPORTER=otlp_http OTEL_ENDPOINT=http://127.0.0.1:8787/gateway/otlp/v1/traces \
    "OTEL_HEADERS=Authorization=Bearer lab-otlp" OTEL_SERVICE_NAME=litellm-lab \
    venv/bin/litellm --config lab/litellm.yaml --port 4000 --host 127.0.0.1 &

cargo build --release --locked && target/release/pybox-gateway --config lab/gateway.lab.toml &
```

Console: <http://127.0.0.1:8787/gateway/console> (admin token `lab-admin`).

## Drive it

```sh
venv/bin/python lab/drive.py --mode direct  --seconds 180 --out /tmp/direct.jsonl    # agents -> LiteLLM
venv/bin/python lab/drive.py --mode gateway --seconds 180 --out /tmp/gateway.jsonl   # agents -> gateway -> LiteLLM
venv/bin/python lab/compare.py /tmp/direct.jsonl /tmp/gateway.jsonl
```

`drive.py` runs 18 interactive agents (one request at a time each) for the whole
run and, from 20% to 85% of it, two bulk agents that keep 24 requests open on the
local models and 80 on the hosted ones. Both modes use the same schedule, and the
numbers are what the clients measured, so they include any time spent queued.

Keep the machine otherwise idle while a run is going: the local models, LiteLLM
and the load generator share the same cores, and a compile in the background is
enough to skew a comparison.

Results from one machine are in [RESULTS.md](RESULTS.md).

## A realistic day

```sh
target/release/pybox-gateway --config lab/gateway.realistic-base.toml &
venv/bin/python lab/realistic.py --seconds 480 --out /tmp/day-base.jsonl
venv/bin/python lab/analyze_day.py /tmp/day-base.jsonl          # while that gateway is still running
# then the same day with weights, a batch limit, a runaway budget and the cache on:
target/release/pybox-gateway --config lab/gateway.realistic-tuned.toml &
venv/bin/python lab/realistic.py --seconds 480 --profile tuned --out /tmp/day-tuned.jsonl
```

Multi-turn sessions, tool results, repeated questions, a batch at peak time and a runaway
agent; what it showed is in [REALISTIC.md](REALISTIC.md). Costs about $2 per run.

## Experiments, one folder each

`stack.sh up` starts the two local model servers, `mock_cloud.py` (a stand-in
for the hosted models: prompt cache, unsupported parameters and timing are
modelled, nothing is billed) and LiteLLM with `litellm.mock.yaml`.
`experiment.py` then runs one experiment against a fresh gateway and keeps
everything about it together:

```sh
export LAB=~/lab     # venv, models, logs; LiteLLM's otel callback also needs opentelemetry-sdk
lab/stack.sh up
PY=$LAB/venv/bin/python
$PY lab/experiment.py run sat-unified --kind sat --config lab/gateway.sat-unified.toml \
    --drive "$PY lab/saturate.py --mode gateway --out {dir}/requests.jsonl --priority --patient --retry" \
    --analyze "$PY lab/analyze_sat.py run={dir}/requests.jsonl" --note "why this run exists"
$PY lab/experiment.py list --kind sat
$PY lab/experiment.py compare <id-before> <id-after>
```

Each run's folder holds `manifest.json` (commit, config, command, machine,
note), the gateway configuration and log, the raw per-request records and
`summary.json`; `index.jsonl` beside the folders has one line per run with
the headline numbers. `--binary` runs an older build under the same load for
a before/after on one machine. Results are kept outside the repository
(`$GATEWAY_EXPERIMENTS`).

A day run reports completed work, fresh tokens per unit of work and
`valid`; give `--baseline <summary.json of the base run>` to get this day's
unit costs applied to the baseline's amount of work.
