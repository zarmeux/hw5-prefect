#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
stage=${1:?stage required}
evidence=${2:?evidence directory required}
work=${3:?working directory required}
case "$stage" in
  preflight)
    [[ -x $SPARK_HOME/bin/spark-submit ]]
    hdfs_cmd dfs -test -d "$INPUT_PATH"
    fs_uri=$(hdfs_cmd getconf -confKey fs.defaultFS)
    [[ $fs_uri == hdfs://* ]] || { echo 'fs.defaultFS must be HDFS' >&2; exit 1; }
    printf '%s\n' "${fs_uri%/}" >"$evidence/fs-uri.txt"
    hdfs_cmd dfs -mkdir -p /user/hive/warehouse-external/hw5 "$EVENT_LOG_DIR"
    as_hive "$HADOOP_HOME/bin/yarn" node -list -all >"$evidence/nodes.log" 2>&1
    hdfs_cmd dfs -ls -R "$INPUT_PATH" >"$evidence/input-hdfs.log" 2>&1
    install -o hive -g hive -m 0400 "$ROOT/jobs/events_daily.py" "$work/events_daily.py"
    install -o hive -g hive -m 0400 "$ROOT/conf/spark-defaults.conf" "$work/spark-defaults.conf"
    install -o hive -g hive -m 0400 "$ROOT/sql/register-table.sql" "$work/register-table.sql"
    install -o hive -g hive -m 0400 "$ROOT/sql/check-table.sql" "$work/check-table.sql"
    ;;
  spark)
    fs_uri=$(cat "$evidence/fs-uri.txt")
    cd "$work"
    as_hive "$SPARK_HOME/bin/spark-submit" \
      --master yarn --deploy-mode client --name hw5-events-daily \
      --properties-file "$work/spark-defaults.conf" \
      --driver-memory 512m --num-executors 2 --executor-memory 512m --executor-cores 1 \
      --conf "spark.driver.host=$(hostname -f)" \
      --conf "spark.eventLog.dir=$fs_uri$EVENT_LOG_DIR" \
      "$work/events_daily.py" --input "$fs_uri$INPUT_PATH" --output "$fs_uri$OUTPUT_PATH" \
      2>&1 | tee "$evidence/spark.log"
    python3 "$ROOT/scripts/extract-metrics.py" "$evidence"
    ;;
  register)
    beeline_cmd -f "$work/register-table.sql" >"$evidence/hive-register.log" 2>&1
    ;;
  verify)
    application_id=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["application_id"])' "$evidence/metrics.json")
    [[ $application_id =~ ^application_[0-9]+_[0-9]+$ ]]
    as_hive "$HADOOP_HOME/bin/yarn" application -status "$application_id" >"$evidence/yarn-application.log" 2>&1
    hdfs_cmd dfs -cat "$EVENT_LOG_DIR/$application_id" \
      | python3 "$ROOT/scripts/inspect-event-log.py" >"$evidence/executors.json"
    beeline_cmd -f "$work/check-table.sql" >"$evidence/hive-rows.tsv" 2>"$evidence/hive-client.log"
    hdfs_cmd dfs -ls -R "$OUTPUT_PATH" >"$evidence/output-hdfs.log" 2>&1
    python3 "$ROOT/scripts/verify.py" "$evidence" | tee "$evidence/verification.json"
    ;;
  *) echo "Unknown stage: $stage" >&2; exit 2 ;;
esac
