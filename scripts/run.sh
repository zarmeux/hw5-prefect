#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
exec 9>/run/lock/hw5-prefect.lock
flock -n 9 || { echo 'ДЗ №5 уже выполняется на этой машине.' >&2; exit 1; }
[[ -x /opt/hw5-prefect/bin/python ]] || { echo 'Сначала: sudo bash scripts/install.sh' >&2; exit 1; }
run_id=$(date -u +%Y%m%dT%H%M%SZ)-$$
evidence="$ROOT/evidence/$run_id"
mkdir -p "$evidence" /var/lib/hive/tmp
work=$(mktemp -d /var/lib/hive/tmp/hw5-prefect.XXXXXXXX)
trap 'rm -rf -- "$work"' EXIT
chown hive:hive "$work"
# A temporary local Prefect API is started automatically; no Cloud account needed.
# An explicit server can instead be selected using PREFECT_API_URL.
export PREFECT_HOME=/var/lib/hw5-prefect
export PREFECT_SERVER_ANALYTICS_ENABLED=false
export PREFECT_LOGGING_COLORS=false
mkdir -p "$PREFECT_HOME"
cd "$ROOT"
/opt/hw5-prefect/bin/python flows/events_flow.py \
  --evidence "$evidence" --work "$work" 2>&1 | tee "$evidence/prefect.log"
python3 "$ROOT/scripts/collect-evidence.py" "$evidence" --output "$ROOT/evidence/RESULTS.md"
printf '%s\n' "$evidence" >"$ROOT/evidence/latest.txt"
printf 'HW5_RUN_OK %s\n' "$evidence"
