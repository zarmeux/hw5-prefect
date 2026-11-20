#!/usr/bin/env bash
set -euo pipefail
[[ $EUID -eq 0 ]] || { echo 'Запускаем через sudo bash.' >&2; exit 1; }
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/etc/hadoop-yarn
export SPARK_HOME=/opt/spark-3.5.7-bin-hadoop3
INPUT_PATH=/user/hive/warehouse-external/hw3/events
OUTPUT_PATH=/user/hive/warehouse-external/hw5/events_daily
EVENT_LOG_DIR=/user/hive/spark-events
JDBC_URL=jdbc:hive2://127.0.0.1:10000/default
as_hive() {
  runuser -u hive -- env JAVA_HOME="$JAVA_HOME" HADOOP_HOME="$HADOOP_HOME" \
    HADOOP_CONF_DIR="$HADOOP_CONF_DIR" HADOOP_HEAPSIZE=256 "$@"
}
hdfs_cmd() { as_hive "$HADOOP_HOME/bin/hdfs" "$@"; }
beeline_cmd() {
  runuser -u hive -- env JAVA_HOME="$JAVA_HOME" HADOOP_HOME="$HADOOP_HOME" \
    HADOOP_CONF_DIR=/etc/hive/hadoop HIVE_HOME=/opt/hive HIVE_CONF_DIR=/etc/hive \
    HADOOP_HEAPSIZE=256 /opt/hive/bin/beeline -u "$JDBC_URL" -n hive \
    --force=false --silent=true --showHeader=false --outputformat=tsv2 "$@"
}
