from config import get_spark_session

spark = get_spark_session("netperf-test-spark", memory="1g")
spark.range(5).show()
spark.stop()
print("PySpark smoke test completed successfully.")