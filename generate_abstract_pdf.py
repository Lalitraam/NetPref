import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent

html_content = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>NetPref - Abstract</title>
<style>
  @page {
    size: A4 portrait;
    margin: 11mm 16mm 10mm 16mm;
  }
  *, *::before, *::after {
    box-sizing: border-box;
  }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    color: #111827;
    background: #ffffff;
    line-height: 1.32;
    font-size: 8.5pt;
    margin: 0;
    padding: 0;
  }
  ul:last-of-type { margin-bottom: 0; }

  .header {
    text-align: left;
    margin-bottom: 8pt;
    border-bottom: 1.5pt solid #1e3a8a;
    padding-bottom: 5pt;
  }
  .course-title {
    font-size: 10pt;
    font-weight: 700;
    color: #1e3a8a;
    letter-spacing: 0.5px;
    text-transform: uppercase;
    margin-bottom: 2pt;
  }
  .project-title {
    font-size: 14pt;
    font-weight: 800;
    color: #0f172a;
    line-height: 1.2;
    margin-bottom: 3pt;
  }
  .authors {
    font-size: 9.5pt;
    font-weight: 600;
    color: #4b5563;
  }
  .authors span {
    color: #111827;
  }

  h2 {
    font-size: 10pt;
    font-weight: 700;
    color: #1e3a8a;
    margin-top: 7pt;
    margin-bottom: 3pt;
    text-transform: uppercase;
    letter-spacing: 0.3px;
    border-bottom: 0.5pt solid #e5e7eb;
    padding-bottom: 1pt;
  }

  p {
    margin-top: 0;
    margin-bottom: 4.5pt;
    text-align: justify;
    line-height: 1.32;
  }

  ul {
    margin-top: 0;
    margin-bottom: 6pt;
    padding-left: 13pt;
  }
  li {
    margin-bottom: 2pt;
    line-height: 1.3;
    font-size: 8.5pt;
  }
  li strong {
    color: #0f172a;
  }

  .tech-stack-list {
    list-style-type: square;
  }
  .key-features-list {
    list-style-type: disc;
  }
</style>
</head>
<body>

<div class="header">
  <div class="course-title">Big Data and Modern Databases Lab (23XW56)</div>
  <div class="project-title">NetPref: A Real-Time Network Performance & Congestion Predictor with Distributed Stream Analytics</div>
  <div class="authors">Developed By: <span>Lalit (24PW21), Praneeth (24PW40)</span></div>
</div>

<h2>Abstract</h2>
<p>
  In modern high-speed communication networks, dynamic congestion and bufferbloat—characterized by severe packet loss and volatile jitter—critically degrade application responsiveness and Quality of Service (QoS). Traditional network management systems operate reactively by computing packet drop ratios post-mortem or relying on active probe traffic that imposes auxiliary network overhead. NetPref introduces a proactive, real-time distributed stream-processing architecture designed to predict network degradation at the individual flow level from baseline transport-layer behaviors before catastrophic buffer overflow occurs.
</p>
<p>
  The ingestion pipeline streams network flow events (derived from the UNSW-NB15 benchmark dataset) through an Apache Kafka distributed message broker partitioned by network link topologies. An Apache Spark Structured Streaming engine consumes events in micro-batches and executes real-time distributed inference using a trained Spark MLlib Random Forest classifier (50 decision trees, max depth 8). Crucially, the model strictly excludes ground-truth packet loss and jitter metrics during training to prevent target leakage, predicting degradation purely from 22 behavioral transport attributes—including connection duration, inter-packet arrival times, byte transfer rates, TCP window advertisements, and round-trip handshake timings—achieving 97.0% classification accuracy and an ROC AUC of 0.994.
</p>
<p>
  Scored flows and stateful 10-second tumbling window link aggregations are persisted across a dual-store architecture: an operational HBase-style wide-column store using compound row keys with reversed timestamps (<code>link_id#reversed_ts#flow_id</code>) for sub-millisecond reverse-chronological prefix scans, and an embedded DuckDB columnar data warehouse for analytical SQL slicing. A reactive Streamlit NOC dashboard visualizes real-time per-link health, automatically flags bottleneck links, triggers high-priority alerts (P &ge; 0.80), and provides an in-browser SQL query runner, demonstrating an end-to-end implementation of enterprise big data engineering, distributed streaming, and predictive network intelligence.
</p>

<h2>Key Features</h2>
<ul class="key-features-list">
  <li><strong>Real-Time Event Streaming:</strong> High-throughput event ingestion using Apache Kafka 3.9 (KRaft mode) partitioned by link topology.</li>
  <li><strong>Distributed Stream Processing:</strong> Micro-batch stream processing and stateful 10-second windowed aggregation with Apache Spark 3.5.</li>
  <li><strong>In-Stream Machine Learning:</strong> Parallel inference using Spark MLlib Random Forest classifier achieving 97.0% accuracy and 0.994 AUC.</li>
  <li><strong>Zero-Target-Leakage Prediction:</strong> Predicts packet loss and jitter degradation purely from behavioral flow and TCP handshake dynamics.</li>
  <li><strong>Operational Wide-Column Storage:</strong> HBase-style store implementing compound row keys with reversed timestamps for chronological prefix scans.</li>
  <li><strong>Columnar Analytical Warehousing:</strong> Embedded DuckDB analytical store hosting fact tables (<code>fact_flows</code>) and alert views (<code>v_alerts</code>).</li>
  <li><strong>Interactive NOC Monitoring Dashboard:</strong> Streamlit-based operations center with live metric cards, link health bars, and alert feeds.</li>
</ul>

<h2>Tech Stack</h2>
<ul class="tech-stack-list">
  <li><strong>Ingestion Layer:</strong> Apache Kafka 3.9 (KRaft consensus mode, topic: <code>flows</code>), Python producer</li>
  <li><strong>Stream Processing:</strong> Apache Spark 3.5 (PySpark) Structured Streaming with event-time watermarking</li>
  <li><strong>Machine Learning:</strong> Spark MLlib (Random Forest Classifier, StringIndexer, OneHotEncoder, VectorAssembler)</li>
  <li><strong>Storage Layer:</strong> Operational Wide-Column Store (HBaseLite over DuckDB), Analytical Warehouse (DuckDB + SQL)</li>
  <li><strong>Visualization & Monitoring:</strong> Streamlit, Altair, Matplotlib</li>
  <li><strong>Platform:</strong> Cross-platform support across macOS (Apple Silicon & Intel), Linux (Ubuntu/Debian), and Windows</li>
</ul>

</body>
</html>
"""

html_path = ROOT / "output" / "NetPref_Abstract.html"
pdf_path = ROOT / "NetPref_Abstract.pdf"

html_path.write_text(html_content, encoding="utf-8")
print(f"Saved HTML to {html_path}")

cmd = [
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "--headless",
    "--disable-gpu",
    "--run-all-compositor-stages-before-draw",
    f"--print-to-pdf={pdf_path}",
    "--no-pdf-header-footer",
    str(html_path.as_uri()),
]

print("Rendering NetPref_Abstract.pdf via Google Chrome headless...")
res = subprocess.run(cmd, capture_output=True, text=True)
if res.returncode == 0 and pdf_path.exists():
    size_kb = pdf_path.stat().st_size / 1024
    print(f"SUCCESS: Generated {pdf_path} ({size_kb:.1f} KB)")
else:
    print(f"ERROR: {res.stderr}")
