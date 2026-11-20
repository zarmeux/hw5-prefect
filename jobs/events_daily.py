import argparse
import json

from pyspark.sql import SparkSession, functions as F, types as T


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("hw5-events-daily").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        if spark.sparkContext.master != "yarn":
            raise RuntimeError("Эту работу запускаем через --master yarn")
        conf = spark.sparkContext._jsc.hadoopConfiguration()
        source = spark._jvm.org.apache.hadoop.fs.Path(args.input)
        target = spark._jvm.org.apache.hadoop.fs.Path(args.output)
        source_fs = source.getFileSystem(conf)
        target_fs = target.getFileSystem(conf)
        if str(source_fs.getUri().getScheme()) != "hdfs":
            raise RuntimeError("Источник должен находиться в HDFS")
        if str(target_fs.getUri().getScheme()) != "hdfs":
            raise RuntimeError("Результат должен находиться в HDFS")

        schema = T.StructType([
            T.StructField("event_id", T.StringType()),
            T.StructField("user_id", T.StringType()),
            T.StructField("event_type", T.StringType()),
        ])
        raw = (spark.read.schema(schema).option("header", "false")
               .option("mode", "FAILFAST").option("basePath", args.input)
               .csv(args.input).cache())
        if "event_date" not in raw.columns:
            raise RuntimeError("Нет партиций event_date=YYYY-MM-DD во входном каталоге")
        input_count = raw.count()
        if input_count == 0:
            raise RuntimeError("Входные данные пусты")
        normalized = raw.select(
            F.trim("event_id").alias("event_id_text"),
            F.trim("user_id").alias("user_id_text"),
            F.lower(F.trim("event_type")).alias("event_type"),
            F.col("event_date").alias("event_date_text"),
            F.to_date("event_date", "yyyy-MM-dd").alias("event_day"),
        ).withColumn("event_id", F.col("event_id_text").cast("long")) \
         .withColumn("user_id", F.col("user_id_text").cast("long"))
        valid = normalized.filter(
            F.col("event_id_text").rlike("^[0-9]+$")
            & F.col("user_id_text").rlike("^[0-9]+$")
            & F.col("event_id").isNotNull()
            & F.col("user_id").isNotNull()
            & F.col("event_type").rlike("^[a-z][a-z0-9_]*$")
            & F.col("event_day").isNotNull()
            & (F.date_format("event_day", "yyyy-MM-dd") == F.col("event_date_text"))
        ).select("event_id", "user_id", "event_type", "event_day")
        valid_count = valid.count()
        if valid_count != input_count:
            raise RuntimeError(f"Некорректных записей: {input_count - valid_count}; запись отменена")
        clean = valid.dropDuplicates().cache()
        clean_count = clean.count()
        daily = (clean.groupBy("event_day").agg(
            F.count("*").alias("events_count"),
            F.countDistinct("user_id").alias("unique_users"),
            F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("page_views"),
            F.sum(F.when(F.col("event_type") == "signup", 1).otherwise(0)).alias("signups"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
        ).withColumn("purchase_share", F.round(F.col("purchases") / F.col("events_count"), 6))
          .withColumn("event_date", F.date_format("event_day", "yyyy-MM-dd"))
          .drop("event_day").cache())
        rows = [row.asDict() for row in daily.orderBy("event_date").collect()]
        daily.explain(mode="formatted")
        daily.write.mode("overwrite").partitionBy("event_date").parquet(args.output)
        saved = spark.read.parquet(args.output)
        if saved.count() != len(rows):
            raise RuntimeError("Число записанных строк не совпало с расчётом")
        metrics = {
            "application_id": spark.sparkContext.applicationId,
            "master": spark.sparkContext.master,
            "spark_version": spark.version,
            "input_uri": str(source_fs.makeQualified(source)),
            "output_uri": str(target_fs.makeQualified(target)),
            "input_rows": input_count,
            "valid_rows": valid_count,
            "clean_rows": clean_count,
            "duplicates_removed": valid_count - clean_count,
            "output_rows": len(rows),
            "rows": rows,
        }
        print("HW5_METRICS=" + json.dumps(metrics, sort_keys=True), flush=True)
        daily.unpersist()
        clean.unpersist()
        raw.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
