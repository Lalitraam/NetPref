import argparse
import os
import shutil
import sys
from pathlib import Path

# ---- Windows + Hadoop setup (same as train_model.py) ----
HADOOP_HOME_WIN = r"C:\hadoop"
HADOOP_HOME_FWD = "C:/hadoop"
HADOOP_BIN_WIN = HADOOP_HOME_WIN + r"\bin"
HADOOP_BIN_FWD = HADOOP_HOME_FWD + "/bin"
if not os.path.exists(HADOOP_BIN_WIN + r"\hadoop.dll"):
    sys.exit(r"hadoop.dll not found in C:\hadoop\bin - add it, then run again")
os.environ["HADOOP_HOME"] = HADOOP_HOME_WIN
os.environ["hadoop.home.dir"] = HADOOP_HOME_WIN
os.environ["PATH"] = HADOOP_BIN_WIN + os.pathsep + os.environ["PATH"]
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--seconds", type=int, default=0,
                    help="auto-stop after N seconds (0 = run until Ctrl+C)")
args = parser.parse_args()

ROOT = Path(__file__).resolve().parent
MODELS = ROOT / "models"
CHECKPOINTS = ROOT / "checkpoints"
OUT_FLOWS = ROOT / "output" / "scored_flows"
ALERT_PROB = 0.8   # a flow counts as an "alert" at or above this probability

# clean slate on every run (demo mode)
shutil.rmtree(CHECKPOINTS, ignore_errors=True)
shutil.rmtree(OUT_FLOWS, ignore_errors=True)
OUT_FLOWS.parent.mkdir(exist_ok=True)

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("netperf-stream")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.ui.showConsoleProgress", "false")
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
    .config(
        "spark.driver.extraJavaOptions",
        f"-Dhadoop.home.dir={HADOOP_HOME_FWD} -Djava.library.path={HADOOP_BIN_FWD}",
    )
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

SCHEMA = """
  flow_id long, proto string, service string, state string,
  dur double, spkts long, dpkts long, sbytes long, dbytes long,
  rate double, sload double, dload double, sinpkt double, dinpkt double,
  smean long, dmean long, swin long, dwin long,
  tcprtt double, synack double, ackdat double,
  ct_src_dport_ltm long, ct_dst_sport_ltm long,
  sloss long, dloss long, sjit double, djit double,
  total_pkts long, total_bytes long, loss_ratio double, avg_jitter double,
  attack_cat string, true_degraded int, link_id string, event_ts_ms long
"""

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "flows")
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", 500)   # ~500 events per micro-batch
    .load()
)

events = (
    raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e"))
    .select("e.*")
    .withColumn("event_time", (F.col("event_ts_ms") / 1000).cast("timestamp"))
)

model = PipelineModel.load((MODELS / "rf_congestion").as_uri())
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
    df.write.mode("append").parquet(OUT_FLOWS.as_uri())
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
    .option("checkpointLocation", (CHECKPOINTS / "flows").as_uri())
    .trigger(processingTime="5 seconds")
    .start()
)
q_windows = (
    metrics.writeStream.outputMode("update").foreachBatch(show_windows)
    .option("checkpointLocation", (CHECKPOINTS / "windows").as_uri())
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