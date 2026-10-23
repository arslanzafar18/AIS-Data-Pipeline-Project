#!/usr/bin/env bash
# Shows the Gold publish gate: inject a Bronze file that is 90% malformed -> Silver rejects it ->
# Gold refuses to publish and keeps the previous READY output. Run AFTER scripts/run_demo.sh.
set -uo pipefail
cd "$(dirname "$0")/.."
export APP_ENV=${APP_ENV:-dev} PYTHONPATH=.
python - <<'PY'
import gzip, json, pipeline_config
from pathlib import Path
from collector.collector import make_record
cfg = pipeline_config.load(); d = Path(pipeline_config.path(cfg, "bronze/raw/dt=2026-10-11/hour=12")); d.mkdir(parents=True, exist_ok=True)
good = json.dumps({"mmsi": 259000001, "msgtime": "2026-10-11T12:00:00+00:00", "latitude": 63.0, "longitude": 1.0, "speedOverGround": 10.0})
with gzip.open(d / "ais_bad00000001_20261011T120000Z.jsonl.gz", "wt") as f:
    for i in range(3000):
        line = good.replace("259000001", str(259000001 + i)) if i % 10 == 0 else '{"mmsi": 25'
        f.write(json.dumps(make_record("2026-10-11T12:00:03+00:00", line, "bad.jsonl.gz", "bad")) + "\n")
PY
python -m spark.silver_job > /dev/null 2>&1
python -m spark.gold_job 2>&1 | grep -E "\"passed\": false|\"detail\"|Gold NOT" | head
rc=${PIPESTATUS[0]}
echo "gold_job exit code: $rc"
[ "$rc" -ne 0 ] || { echo "GATE DID NOT BLOCK BAD DATA"; exit 1; }
echo "--- previous published Gold is still READY:"; python -c "import json;print(json.load(open('data/$APP_ENV/gold/_READY.json'))['status'])"
echo "--- failure manifest:"; python -c "import json;m=json.load(open('data/$APP_ENV/gold/_LAST_FAILED.json'));print(m['status'],[c for c in m['checks'] if not c['passed']])"
