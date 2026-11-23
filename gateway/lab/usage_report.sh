#!/usr/bin/env bash
# The usage report for a window of the gateway's own log: take the request
# rows out of the gateway (TDB or memory, whichever it runs with) and hand
# them to `gwsim usage`. This is the whole of what a scheduled daily report
# has to run.
#
#   lab/usage_report.sh OUT_DIR [FROM_MS TO_MS]
#
# Without a window it takes the 24 hours up to now. Settings come from the
# environment:
#   GATEWAY        gateway address               (http://127.0.0.1:8787)
#   ADMIN_TOKEN    its admin token
#   GWSIM          the gwsim binary              (sim/target/release/gwsim)
#   SLOTS          what the servers of each model run at once, model=N,...
#   TZ_HOURS       hours from UTC for the hour-of-day tables (9)
#   DAY_S          length of a day in the log, for shortened test days (86400)
#   DAY_START_MS   a timestamp at which such a shortened day starts
#   SOURCE         first line of the report
set -euo pipefail
HERE=$(cd "$(dirname "$0")/.." && pwd)
out=${1:?usage: usage_report.sh OUT_DIR [FROM_MS TO_MS]}
to=${3:-$(($(date +%s) * 1000))}
from=${2:-$((to - 86400000))}
gw=${GATEWAY:-http://127.0.0.1:8787}
gwsim=${GWSIM:-$HERE/sim/target/release/gwsim}
mkdir -p "$out"
curl -sf -H "authorization: Bearer ${ADMIN_TOKEN:?set ADMIN_TOKEN}" \
  "$gw/gateway/obs/export?from=$from&to=$to" -o "$out/rows.jsonl"
n=$(wc -l <"$out/rows.jsonl")
[ "$n" -gt 0 ] || { echo "no request rows between $from and $to" >&2; exit 1; }
args=(--tz "${TZ_HOURS:-9}" --day-s "${DAY_S:-86400}" --report "$out/report.md" --json "$out/report.json")
[ -n "${SLOTS:-}" ] && args+=(--slots "$SLOTS")
[ -n "${DAY_START_MS:-}" ] && args+=(--day-start "$DAY_START_MS")
[ -n "${SOURCE:-}" ] && args+=(--source "$SOURCE")
"$gwsim" usage "$out/rows.jsonl" "${args[@]}"
echo "$n rows -> $out/report.md"
