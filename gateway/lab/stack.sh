#!/usr/bin/env bash
# Start (or stop) what the lab experiments talk to: two local model servers,
# the mock hosted provider and one LiteLLM in front of them. The gateway is
# not started here: experiment.py starts a fresh one per experiment.
#   lab/stack.sh up | down | status          (LAB=~/lab holds venv, models, logs)
set -u
LAB=${LAB:-$HOME/lab}
HERE=$(cd "$(dirname "$0")" && pwd)
PY=$LAB/venv/bin/python
case "${1:-status}" in
up)
  mkdir -p "$LAB/logs"
  model() { # name port file
    nohup "$PY" -m llama_cpp.server --model "$LAB/models/$3" --model_alias "$1" --host 127.0.0.1 --port "$2" \
      --n_ctx 2048 --n_threads 2 --interrupt_requests false >"$LAB/logs/$1.log" 2>&1 &
  }
  model qwen-0.5b 9201 qwen2.5-0.5b-instruct-q4_k_m.gguf
  model smollm2-360m 9202 smollm2-360m-instruct-q8_0.gguf
  nohup "$PY" "$HERE/mock_cloud.py" --port 9300 >"$LAB/logs/mock.log" 2>&1 &
  OTEL_EXPORTER=otlp_http OTEL_ENDPOINT=http://127.0.0.1:8787/gateway/otlp/v1/traces \
    OTEL_HEADERS="Authorization=Bearer lab-otlp" OTEL_SERVICE_NAME=litellm-lab \
    nohup "$LAB/venv/bin/litellm" --config "$HERE/litellm.mock.yaml" --host 127.0.0.1 --port 4000 >"$LAB/logs/litellm.log" 2>&1 &
  for _ in $(seq 120); do
    curl -sf -m 2 -H "authorization: Bearer sk-lab-master" http://127.0.0.1:4000/health/readiness >/dev/null \
      && curl -sf -m 2 http://127.0.0.1:9201/v1/models >/dev/null && curl -sf -m 2 http://127.0.0.1:9202/v1/models >/dev/null \
      && curl -sf -m 2 http://127.0.0.1:9300/stats >/dev/null && break
    sleep 1
  done
  printf '{"litellm":"litellm.mock.yaml","models":["qwen-0.5b q4_k_m","smollm2-360m q8_0"],"threads_per_model":2,"hosted":"mock_cloud.py"}\n' >"$LAB/stack.json"
  "$0" status ;;
down)
  pkill -f "[l]lama_cpp.server" ; pkill -f "[m]ock_cloud.py --port" ; pkill -f "[b]in/litellm --config" ; true ;;
status)
  for p in 9201 9202 9300 4000; do
    if curl -s -m 2 -o /dev/null "http://127.0.0.1:$p/"; then echo "$p up"; else echo "$p down"; fi
  done ;;
esac
