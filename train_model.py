import json
import os
import sys
from pathlib import Path

# ---- Windows + Hadoop setup (must run before Spark starts) ----
HADOOP_HOME_WIN = r"C:\hadoop"      # for environment variables
HADOOP_HOME_FWD = "C:/hadoop"       # for Java options (backslashes get stripped)
HADOOP_BIN_WIN = HADOOP_HOME_WIN + r"\bin"
HADOOP_BIN_FWD = HADOOP_HOME_FWD + "/bin"
if not os.path.exists(HADOOP_BIN_WIN + r"\hadoop.dll"):
    sys.exit(r"hadoop.dll not found in C:\hadoop\bin - add it, then run again")
os.environ["HADOOP_HOME"] = HADOOP_HOME_WIN
os.environ["hadoop.home.dir"] = HADOOP_HOME_WIN
os.environ["PATH"] = HADOOP_BIN_WIN + os.pathsep + os.environ["PATH"]

# make Spark use this venv's Python
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
MODELS = ROOT / "models"
MODELS.mkdir(exist_ok=True)

CAT_COLS = ["proto", "service", "state"]
NUM_COLS = [
    "dur", "spkts", "dpkts", "sbytes", "dbytes", "rate", "sload", "dload",
    "sinpkt", "dinpkt", "smean", "dmean", "swin", "dwin",
    "tcprtt", "synack", "ackdat", "ct_src_dport_ltm", "ct_dst_sport_ltm",
]

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("netperf-train")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.ui.showConsoleProgress", "false")
    .config(
        "spark.driver.extraJavaOptions",
        f"-Dhadoop.home.dir={HADOOP_HOME_FWD} -Djava.library.path={HADOOP_BIN_FWD}",
    )
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

print("Loading data...", flush=True)
train = (
    spark.read.parquet((DATA / "clean_train.parquet").as_uri())
    .withColumn("label", F.col("degraded").cast("double"))
    .repartition(8)
    .cache()
)
test = (
    spark.read.parquet((DATA / "clean_test.parquet").as_uri())
    .withColumn("label", F.col("degraded").cast("double"))
)
print("train rows:", train.count(), "| test rows:", test.count(), flush=True)

indexers = [
    StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
    for c in CAT_COLS
]
encoder = OneHotEncoder(
    inputCols=[f"{c}_idx" for c in CAT_COLS],
    outputCols=[f"{c}_ohe" for c in CAT_COLS],
    handleInvalid="keep",
)
assembler = VectorAssembler(
    inputCols=NUM_COLS + [f"{c}_ohe" for c in CAT_COLS],
    outputCol="features",
    handleInvalid="keep",
)
rf = RandomForestClassifier(
    labelCol="label", featuresCol="features",
    numTrees=50, maxDepth=8, seed=42,
)

pipeline = Pipeline(stages=indexers + [encoder, assembler, rf])
print("Training Random Forest (can take a few minutes)...", flush=True)
model = pipeline.fit(train)

print("Training done. Saving model...", flush=True)
model.write().overwrite().save((MODELS / "rf_congestion").as_uri())
print("Model saved. Evaluating...", flush=True)

preds = model.transform(test)

auc_roc = BinaryClassificationEvaluator(
    labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
).evaluate(preds)
auc_pr = BinaryClassificationEvaluator(
    labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderPR"
).evaluate(preds)

# Confusion matrix once, then compute everything else in plain Python
cm = (
    preds.groupBy("label", "prediction").count().toPandas()
    .set_index(["label", "prediction"])["count"].to_dict()
)
tp = cm.get((1.0, 1.0), 0)
fp = cm.get((0.0, 1.0), 0)
fn = cm.get((1.0, 0.0), 0)
tn = cm.get((0.0, 0.0), 0)
total = tp + fp + fn + tn
precision = tp / (tp + fp) if (tp + fp) else 0.0
recall = tp / (tp + fn) if (tp + fn) else 0.0
f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0

metrics = {
    "auc_roc": auc_roc,
    "auc_pr": auc_pr,
    "accuracy": (tp + tn) / total,
    "precision_degraded": precision,
    "recall_degraded": recall,
    "f1_degraded": f1,
    "confusion": {"tn": tn, "fp": fp, "fn": fn, "tp": tp},
}

print("\n=== Test-set results ===")
for k, v in metrics.items():
    if k != "confusion":
        print(f"{k:20s} {v:.4f}")
print(f"confusion  TN={tn}  FP={fp}  FN={fn}  TP={tp}")

# feature importances with readable names
attrs = preds.schema["features"].metadata["ml_attr"]["attrs"]
names = {}
for group in attrs.values():
    for a in group:
        names[a["idx"]] = a["name"]
imp = model.stages[-1].featureImportances.toArray()
top = sorted(
    ((names.get(i, f"f{i}"), float(v)) for i, v in enumerate(imp)),
    key=lambda x: -x[1],
)[:10]
print("\nTop 10 features:")
for n, v in top:
    print(f"  {n:25s} {v:.4f}")
metrics["top_features"] = top

with open(MODELS / "metrics.json", "w") as f:
    json.dump(metrics, f, indent=2)
print("\nSaved metrics to models/metrics.json", flush=True)

spark.stop()