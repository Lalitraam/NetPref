import argparse
import shutil
from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

from config import (
    ALERT_PROB,
    CHECKPOINTS_DIR,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    MODEL_DIR,
    SCORED_FLOWS_DIR,
    STREAMING_SCHEMA,
    get_spark_session,
)

parser = argparse.ArgumentParser()
parser.add_argument("--seconds", type=int, default=0,
                    help="auto-stop after N seconds (0 = run until Ctrl+C)")
parser.add_argument("--bootstrap-server", default=KAFKA_BOOTSTRAP_SERVERS,
                    help="Kafka bootstrap servers (default from config/env)")
parser.add_argument("--topic", default=KAFKA_TOPIC,
                    help="Kafka topic to subscribe to")
args = parser.parse_args()

# clean slate on every run (demo mode)
shutil.rmtree(CHECKPOINTS_DIR, ignore_errors=True)
shutil.rmtree(SCORED_FLOWS_DIR, ignore_errors=True)
SCORED_FLOWS_DIR.parent.mkdir(exist_ok=True)

spark = get_spark_session(
    "netperf-stream",
    packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"],
)

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", args.bootstrap_server)
    .option("subscribe", args.topic)
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", 500)   # ~500 events per micro-batch
    .load()
)

events = (
    raw.select(F.from_json(F.col("value").cast("string"), STREAMING_SCHEMA).alias("e"))
    .select("e.*")
    .withColumn("event_time", (F.col("event_ts_ms") / 1000).cast("timestamp"))
)

model = PipelineModel.load(MODEL_DIR.as_uri())
scored = (
    model.transform(events)
    .withColumn("degraded_prob", vector_to_array("probability")[1])
    .select(
        "flow_id", "link_id", "event_time", "proto", "service", "state",
        "total_pkts", "total_bytes", "loss_ratio", "avg_jitter",
        "true_degraded", "degraded_prob", "prediction",
    )
)


def handle_flows(df, batch_id):
    df = df.cache()
    n = df.count()
    if n == 0:
        df.unpersist()
        return
    alerts = df.filter(
        (F.col("prediction") == 1.0) & (F.col("degraded_prob") >= ALERT_PROB)
    ).count()
    correct = df.filter(F.col("prediction") == F.col("true_degraded")).count()
    df.write.mode("append").parquet(SCORED_FLOWS_DIR.as_uri())
    print(f"[flows] batch {batch_id}: {n} scored | {alerts} alerts "
          f"(prob >= {ALERT_PROB}) | agrees with rule label: {correct / n:.1%}",
          flush=True)
    df.unpersist()


metrics = (
    scored.withWatermark("event_time", "30 seconds")
    .groupBy(F.window("event_time", "10 seconds").alias("w"), "link_id")
    .agg(
        F.count("*").alias("flows"),
        F.round(F.sum("total_bytes") / 1e6, 3).alias("mb"),
        F.round(F.avg("loss_ratio"), 3).alias("avg_loss"),
        F.round(F.avg("avg_jitter"), 1).alias("mean_jitter"),
        F.sum(F.col("prediction").cast("int")).alias("pred_degraded"),
        F.round(F.avg("prediction"), 3).alias("degraded_rate"),
    )
)


def show_windows(df, batch_id):
    n = df.count()
    if n == 0:
        return
    print(f"\n[windows] batch {batch_id}: {n} link-windows updated. Worst 5:",
          flush=True)
    (
        df.select(
            F.col("w.start").alias("window_start"), "link_id", "flows", "mb",
            "avg_loss", "mean_jitter", "pred_degraded", "degraded_rate",
        )
        .orderBy(F.desc("degraded_rate"), F.desc("flows"))
        .show(5, truncate=False)
    )


q_flows = (
    scored.writeStream.foreachBatch(handle_flows)
    .option("checkpointLocation", (CHECKPOINTS_DIR / "flows").as_uri())
    .trigger(processingTime="5 seconds")
    .start()
)
q_windows = (
    metrics.writeStream.outputMode("update").foreachBatch(show_windows)
    .option("checkpointLocation", (CHECKPOINTS_DIR / "windows").as_uri())
    .trigger(processingTime="5 seconds")
    .start()
)

print("Streaming started. Waiting for Kafka events (Ctrl+C to stop)...",
      flush=True)
try:
    if args.seconds > 0:
        spark.streams.awaitAnyTermination(args.seconds)
    else:
        spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\nStopping...")
finally:
    for q in (q_flows, q_windows):
        try:
            q.stop()
        except Exception:
            pass
    spark.stop()
    print("Stopped.")