#!/usr/bin/env bash
# Model servers for the prefix-routing runs: three llama.cpp servers with the
# same small model, one thread each.
#   lab/prefix_stack.sh up        one cache slot per server (the last prompt)
#   lab/prefix_stack.sh up many   servers keep many prompts (--cache true)
#   lab/prefix_stack.sh slots N   slot_server.py instead: N cache slots per server,
#                                 and answers say how many prompt tokens were reused
#   lab/prefix_stack.sh litellm   also one LiteLLM proxy per server (4011-4013)
#   lab/prefix_stack.sh down
set -u
LAB=${LAB:-$HOME/lab}
HERE=$(cd "$(dirname "$0")" && pwd)
PY=$LAB/venv/bin/python
case "${1:-up}" in
up)
  extra=""; [ "${2:-}" = many ] && extra="--cache true --cache_size 1000000000"
  for p in 9211 9212 9213; do
    nohup "$PY" -m llama_cpp.server --model "$LAB/models/qwen2.5-0.5b-instruct-q4_k_m.gguf" --model_alias qwen-0.5b \
      --host 127.0.0.1 --port $p --n_ctx 4096 --n_threads 1 --interrupt_requests false $extra >"$LAB/logs/kv-$p.log" 2>&1 &
  done
  for _ in $(seq 120); do
    curl -sf -m 2 http://127.0.0.1:9211/v1/models >/dev/null && curl -sf -m 2 http://127.0.0.1:9212/v1/models >/dev/null \
      && curl -sf -m 2 http://127.0.0.1:9213/v1/models >/dev/null && break
    sleep 1
  done ;;
slots)
  for p in 9211 9212 9213; do
    nohup "$PY" "$HERE/slot_server.py" --model "$LAB/models/qwen2.5-0.5b-instruct-q4_k_m.gguf" --port $p --slots "${2:-2}" \
      >"$LAB/logs/kv-$p.log" 2>&1 &
  done
  for _ in $(seq 180); do
    curl -sf -m 2 http://127.0.0.1:9211/v1/models >/dev/null && curl -sf -m 2 http://127.0.0.1:9212/v1/models >/dev/null \
      && curl -sf -m 2 http://127.0.0.1:9213/v1/models >/dev/null && break
    sleep 1
  done ;;
litellm)
  for i in 1 2 3; do
    nohup "$LAB/venv/bin/litellm" --config "$HERE/litellm.prefix-$i.yaml" --host 127.0.0.1 --port 401$i >"$LAB/logs/litellm-401$i.log" 2>&1 &
  done
  for _ in $(seq 180); do
    ok=1; for i in 1 2 3; do curl -sf -m 2 -H "authorization: Bearer sk-lab-master" http://127.0.0.1:401$i/health/readiness >/dev/null || ok=0; done
    [ $ok = 1 ] && break; sleep 1
  done ;;
down)
  pkill -f "[l]lama_cpp.server"; pkill -f "[s]lot_server.py"; pkill -f "[b]in/litellm --config"; true ;;
esac
