import json
import sys
from pathlib import Path

evidence = Path(sys.argv[1])
rows = [line.split("HW5_METRICS=", 1)[1]
        for line in (evidence / "spark.log").read_text().splitlines()
        if line.startswith("HW5_METRICS=")]
if len(rows) != 1:
    raise RuntimeError(f"Expected one metrics record, got {len(rows)}")
(evidence / "metrics.json").write_text(
    json.dumps(json.loads(rows[0]), indent=2) + "\n", encoding="utf-8")
