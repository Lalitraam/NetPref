# BIG DATA AND MODERN DATABASES LAB (23XW56)

## NetPref - A Real-Time Network Performance and Congestion Predictor with Distributed Stream Analytics

**Developed By:** Lalit (24PW21), Praneeth (24PW40)

---

### Abstract
In high-speed modern networks, network congestion, bufferbloat, and degraded link conditions—characterized by excessive packet loss and high jitter—severely disrupt application performance and user quality of experience. Traditional network monitoring systems operate reactively by calculating packet loss after connections terminate or by sending active probe traffic that adds network overhead. NetPref introduces a proactive, real-time distributed stream-processing architecture designed to predict impending network degradation at the individual flow level from baseline transport-layer behaviors before catastrophic buffer overflow occurs.

The ingestion pipeline streams network flow events (derived from the UNSW-NB15 benchmark dataset) through an Apache Kafka distributed message broker partitioned by network link topologies. An Apache Spark Structured Streaming engine consumes events in micro-batches and executes real-time distributed inference using a trained Spark MLlib Random Forest classifier (50 decision trees, max depth 8). Crucially, the model strictly excludes ground-truth packet loss and jitter metrics during training to prevent target leakage, predicting degradation purely from 22 behavioral transport attributes—including connection duration, inter-packet arrival times, byte transfer rates, TCP window advertisements, and round-trip handshake timings—achieving 97.0% classification accuracy and an ROC AUC of 0.994.

Scored flows and stateful 10-second tumbling window link aggregations are persisted across a dual-store architecture: an operational HBase-style wide-column store using compound row keys with reversed timestamps (`link_id#reversed_ts#flow_id`) for sub-millisecond reverse-chronological prefix scans, and an embedded DuckDB columnar data warehouse for analytical SQL slicing. A reactive Streamlit NOC dashboard visualizes real-time per-link health, automatically flags bottleneck links, triggers high-priority alerts ($P(\text{degraded}) \ge 0.80$), and provides an in-browser SQL query runner, demonstrating an end-to-end implementation of enterprise big data engineering, distributed streaming, and predictive network intelligence.

---

### Key Features
- **Real-Time Event Streaming:** High-throughput event ingestion using Apache Kafka 3.9 (KRaft mode) partitioned by link topology.
- **Distributed Stream Processing:** Micro-batch stream processing and stateful 10-second windowed aggregation with Apache Spark 3.5.
- **In-Stream Machine Learning:** Parallel inference using Spark MLlib Random Forest classifier achieving 97.0% accuracy and 0.994 AUC.
- **Zero-Target-Leakage Prediction:** Predicts packet loss and jitter degradation purely from behavioral flow and TCP handshake dynamics.
- **Operational Wide-Column Storage:** HBase-style store implementing compound row keys with reversed timestamps for chronological prefix scans.
- **Columnar Analytical Warehousing:** Embedded DuckDB analytical store hosting fact tables (`fact_flows`) and alert views (`v_alerts`).
- **Interactive NOC Monitoring Dashboard:** Streamlit-based operations center with live metric cards, link health bars, and alert feeds.

---

### Tech Stack
- **Ingestion Layer:** Apache Kafka 3.9 (KRaft consensus mode, topic: `flows`), Python producer
- **Stream Processing:** Apache Spark 3.5 (PySpark) Structured Streaming with event-time watermarking
- **Machine Learning:** Spark MLlib (Random Forest Classifier, StringIndexer, OneHotEncoder, VectorAssembler)
- **Storage Layer:** Operational Wide-Column Store (HBaseLite over DuckDB), Analytical Warehouse (DuckDB + SQL)
- **Visualization & Monitoring:** Streamlit, Altair, Matplotlib
- **Platform:** Cross-platform support across macOS (Apple Silicon & Intel), Linux (Ubuntu/Debian), and Windows
