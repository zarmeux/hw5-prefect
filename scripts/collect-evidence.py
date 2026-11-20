"""Create a short submission report only from a complete successful run."""
import argparse
import json
import subprocess
import sys
from pathlib import Path


def generate_report(evidence: Path) -> str:
    required = ["prefect.log", "spark.log", "metrics.json", "yarn-application.log",
                "executors.json", "hive-register.log", "hive-rows.tsv",
                "verification.json", "flow-result.json"]
    for name in required:
        if not (evidence / name).is_file():
            raise RuntimeError(f"Missing evidence: {name}")
    subprocess.run([sys.executable, str(Path(__file__).with_name("verify.py")),
                    str(evidence)], check=True, capture_output=True, text=True)
    metrics = json.loads((evidence / "metrics.json").read_text(encoding="utf-8"))
    result = json.loads((evidence / "flow-result.json").read_text(encoding="utf-8"))
    verification = json.loads((evidence / "verification.json").read_text(encoding="utf-8"))
    executors = json.loads((evidence / "executors.json").read_text(encoding="utf-8"))
    if result["application_id"] != metrics["application_id"]:
        raise RuntimeError("Flow and Spark application IDs differ")
    if not verification.get("hive_matches_spark") or result["verification"] != verification:
        raise RuntimeError("Verification did not succeed or reports differ")
    if not executors.get("executors") or not executors.get("successful_tasks_by_host"):
        raise RuntimeError("No executor/task evidence")
    fields = ["event_date", "events_count", "unique_users", "page_views", "signups",
              "purchases", "purchase_share"]
    lines = ["# Результат запуска ДЗ №5", "",
             f"Запуск: `{evidence.name}`.", "",
             f"Приложение YARN: `{metrics['application_id']}`; тип `SPARK`, итог `SUCCEEDED`.",
             "Prefect выполнил проверку кластера, Spark-обработку, регистрацию Hive и сверку результата.",
             f"Spark master: `{metrics['master']}`; таблица: `hw5.events_daily`.",
             f"Вход: `{metrics['input_uri']}`.", f"Результат: `{metrics['output_uri']}`.", "",
             "## Проверенные показатели", "",
             f"Прочитано строк: {metrics['input_rows']}; корректных: {metrics['valid_rows']}; "
             f"после удаления дублей: {metrics['clean_rows']}; дневных строк: {metrics['output_rows']}.",
             "Hive совпал с результатом Spark по всем датам и показателям.", "",
             "| " + " | ".join(fields) + " |", "| " + " | ".join(["---"] * len(fields)) + " |"]
    for row in sorted(metrics["rows"], key=lambda row: row["event_date"]):
        lines.append("| " + " | ".join(str(row[field]) for field in fields) + " |")
    lines += ["", "## Подтверждения", "",
              f"В Spark event log зарегистрировано исполнителей: {len(executors['executors'])}.",
              "Подтверждены успешно выполненные задачи Spark на исполнителях.",
              "Полные журналы находятся в локальном каталоге этого запуска `evidence/`.",
              ""]
    return "\n".join(lines)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("evidence", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = generate_report(args.evidence.resolve())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(report, encoding="utf-8")
    print(f"REPORT_OK {args.output}")
