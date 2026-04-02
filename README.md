# databricks-online-retail-pipeline
End-to-End Medallion Pipeline on Databricks (PySpark + Delta Lake)
# End-to-End Data Engineering Pipeline (Databricks)

## Overview
This project demonstrates a fully scalable end-to-end data engineering pipeline built in Databricks using Medallion Architecture (Bronze, Silver, Gold).
The pipeline processes retail transactional data and transforms it into an analytics-ready data model optimized for BI tools.

---

## Architecture
- Bronze Layer: Raw data ingestion using Auto Loader (cloudFiles)
- Silver Layer: Data cleaning, validation, deduplication
- Gold Layer: Star schema (fact + dimension tables)

---

## Tech Stack
- Databricks (Lakehouse)
- PySpark
- Delta Lake
- Unity Catalog
- Auto Loader (cloudFiles)
- SQL

---

## Data Source
- Online Retail II dataset (Kaggle)

---

## Pipeline Steps

### Bronze Layer
- Ingested raw CSV data using Auto Loader
- Added metadata (load timestamp, file path, ingestion date)
- Stored data in Delta Lake

### Silver Layer
- Removed null values and invalid records
- Filtered negative quantities and prices
- Applied schema standardization
- Deduplicated records using window functions
- Added derived columns (TotalAmount, date breakdown)

### Gold Layer
- Built Star Schema:
  - fact_sales
  - dim_customer
  - dim_product
  - dim_date
- Created surrogate keys
- Optimized data for analytical queries

---

## Results
- Processed ~500k+ rows
- Built scalable and reproducible pipeline
- Created analytics-ready data model

---

## How to Run
1. Upload data to Volume (raw_data)
2. Run notebooks in order:
   - 01_Bronze_Ingestion
   - 02_Silver_Transformation
   - 03_Gold_Modeling

## Future Improvements
- Add Databricks Workflows (scheduling)
- Add data quality monitoring
- Integrate Power BI dashboard
