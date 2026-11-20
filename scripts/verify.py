import csv
import json
import math
import re
import sys
from pathlib import Path


evidence = Path(sys.argv[1])
metrics = json.loads((evidence / "metrics.json").read_text())
assert metrics["master"] == "yarn", metrics
assert metrics["input_uri"].startswith("hdfs://"), metrics
assert metrics["output_uri"].startswith("hdfs://"), metrics
assert metrics["output_rows"] > 0, metrics
report = (evidence / "yarn-application.log").read_text()
assert re.search(r"Final-State\s*:\s*SUCCEEDED\s*$", report, re.M), report
assert re.search(r"Application-Type\s*:\s*SPARK\s*$", report, re.M), report
fields = ["events_count", "unique_users", "page_views", "signups", "purchases"]
actual = {}
for row in csv.reader((evidence / "hive-rows.tsv").read_text().splitlines(), delimiter="\t"):
    if len(row) != 7 or not row[0][:4].isdigit():
        continue
    assert row[0] not in actual, f"Повтор даты: {row}"
    actual[row[0]] = {key: int(value) for key, value in zip(fields, row[1:6])}
    actual[row[0]]["purchase_share"] = float(row[6])
expected = {row["event_date"]: {key: value for key, value in row.items() if key != "event_date"}
            for row in metrics["rows"]}
assert actual.keys() == expected.keys(), (actual, expected)
for date, wanted in expected.items():
    for key, value in wanted.items():
        assert math.isclose(actual[date][key], value, rel_tol=0, abs_tol=1e-6), (date, key, actual, expected)
assert sum(row["events_count"] for row in actual.values()) == metrics["clean_rows"]
print(json.dumps({"hive_matches_spark": True, "partitions": sorted(actual), "rows": actual}, indent=2))
