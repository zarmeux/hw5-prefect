"""Prefect orchestrates a single distributed Spark job and Hive registration."""
import argparse
import json
import subprocess
from pathlib import Path

from prefect import flow, task
from prefect.cache_policies import NO_CACHE

ROOT = Path(__file__).resolve().parents[1]


def run_stage(stage: str, evidence: str, work: str):
    # Stream output to the parent, which records the complete Prefect run log.
    subprocess.run(
        ["bash", str(ROOT / "scripts/stage.sh"), stage, evidence, work],
        check=True,
    )


@task(name="01-check-hdfs-yarn", cache_policy=NO_CACHE)
def check_cluster(evidence: str, work: str):
    run_stage("preflight", evidence, work)


@task(name="02-spark-yarn-read-transform-write", cache_policy=NO_CACHE)
def process_events(evidence: str, work: str):
    run_stage("spark", evidence, work)
    return json.loads((Path(evidence) / "metrics.json").read_text())["application_id"]


@task(name="03-register-hive-table", cache_policy=NO_CACHE)
def register_table(evidence: str, work: str):
    run_stage("register", evidence, work)


@task(name="04-verify-yarn-spark-hive", cache_policy=NO_CACHE)
def verify_result(evidence: str, work: str):
    run_stage("verify", evidence, work)
    return json.loads((Path(evidence) / "verification.json").read_text())


@flow(name="hw5-events-daily", log_prints=True, retries=0)
def events_flow(evidence: str, work: str):
    # Direct task calls wait for completion; errors stop all following stages.
    # SparkSession/DataFrames remain inside one Spark process, never serialized
    # between Prefect tasks. Every invocation processes the current HDFS input.
    check_cluster(evidence, work)
    application_id = process_events(evidence, work)
    register_table(evidence, work)
    verification = verify_result(evidence, work)
    result = {"application_id": application_id, "verification": verification}
    (Path(evidence) / "flow-result.json").write_text(
        json.dumps(result, indent=2) + "\n", encoding="utf-8"
    )
    print(f"HW5_OK application_id={application_id}")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence", required=True)
    parser.add_argument("--work", required=True)
    args = parser.parse_args()
    events_flow(str(Path(args.evidence).resolve()), str(Path(args.work).resolve()))
