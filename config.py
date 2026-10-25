import os
import sys
from pathlib import Path
from typing import List, Optional

# Base Project Paths
ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"
MODELS_DIR = ROOT_DIR / "models"
OUTPUT_DIR = ROOT_DIR / "output"
CHARTS_DIR = OUTPUT_DIR / "charts"
REPORTS_DIR = OUTPUT_DIR / "reports"
CHECKPOINTS_DIR = ROOT_DIR / "checkpoints"
SCORED_FLOWS_DIR = OUTPUT_DIR / "scored_flows"

# Persistent Database & Artifact Paths
WAREHOUSE_DB = DATA_DIR / "warehouse.duckdb"
HBASE_DB = DATA_DIR / "hbase_lite.duckdb"
METRICS_PATH = MODELS_DIR / "metrics.json"
THRESHOLDS_PATH = DATA_DIR / "thresholds.json"
MODEL_DIR = MODELS_DIR / "rf_congestion"

# Kafka Settings (configurable via environment variables)
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "flows")
N_LINKS = 8

# Alert & Label Thresholds
ALERT_PROB = 0.80
JITTER_QUANTILE = 0.90
LOSS_QUANTILE = 0.85

# Column Definitions
CAT_COLS = ["proto", "service", "state"]
NUM_COLS = [
    "dur", "spkts", "dpkts", "sbytes", "dbytes", "rate", "sload", "dload",
    "sinpkt", "dinpkt", "smean", "dmean", "swin", "dwin",
    "tcprtt", "synack", "ackdat", "ct_src_dport_ltm", "ct_dst_sport_ltm",
]
RAW_METRIC_COLS = ["sloss", "dloss", "sjit", "djit"]

# Streaming Schema
STREAMING_SCHEMA = """
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


def get_kafka_logs_dir() -> Path:
    """Resolve Kafka logs directory across different OS platforms."""
    env_dir = os.environ.get("KAFKA_LOGS_DIR")
    if env_dir:
        return Path(env_dir)

    if sys.platform == "win32":
        candidates = [Path(r"D:\kafka\kafka-logs"), Path(r"C:\kafka\kafka-logs")]
    else:
        # macOS / Linux candidates
        candidates = [
            Path("/tmp/kafka-logs"),
            Path("/var/log/kafka"),
            Path.home() / "kafka" / "kafka-logs",
            Path.home() / ".kafka" / "kafka-logs",
        ]

    for p in candidates:
        if p.exists():
            return p
    return candidates[0]


def setup_spark_environment() -> None:
    """Configures Python runtime and Hadoop environment across Windows, macOS, and Linux."""
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # Windows-specific Hadoop/winutils setup
    if sys.platform == "win32":
        hadoop_home = os.environ.get("HADOOP_HOME", r"C:\hadoop")
        hadoop_bin = os.path.join(hadoop_home, "bin")
        dll_path = os.path.join(hadoop_bin, "hadoop.dll")

        if os.path.exists(dll_path):
            os.environ["HADOOP_HOME"] = hadoop_home
            os.environ["hadoop.home.dir"] = hadoop_home
            current_path = os.environ.get("PATH", "")
            if hadoop_bin not in current_path:
                os.environ["PATH"] = hadoop_bin + os.pathsep + current_path
        else:
            print(
                f"[Notice] hadoop.dll not found in {hadoop_bin}. "
                "If Spark fails on Windows, install winutils.exe and hadoop.dll into C:\\hadoop\\bin."
            )


def get_spark_session(
    app_name: str,
    packages: Optional[List[str]] = None,
    master: str = "local[*]",
    memory: str = "4g",
):
    """Factory creating an optimized, cross-platform PySpark SparkSession."""
    setup_spark_environment()

    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder
        .master(master)
        .appName(app_name)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.memory", memory)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
    )

    if packages:
        builder = builder.config("spark.jars.packages", ",".join(packages))

    if sys.platform == "win32":
        hadoop_home = os.environ.get("HADOOP_HOME", r"C:\hadoop").replace("\\", "/")
        hadoop_bin = f"{hadoop_home}/bin"
        builder = builder.config(
            "spark.driver.extraJavaOptions",
            f"-Dhadoop.home.dir={hadoop_home} -Djava.library.path={hadoop_bin}",
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
