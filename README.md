# VNStockPulse

VNStockPulse is a Vietnamese stock data pipeline and research dashboard.

## Architecture

```text
Data Sources
→ Ingestion Layer
→ Data Contract
→ Bronze Layer
→ Silver Layer
→ Gold Layer
→ Serving API
→ Streamlit Dashboard