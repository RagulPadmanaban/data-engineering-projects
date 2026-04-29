# 🚀 AWS Glue Incremental Data Pipeline

## 📌 Overview

This project demonstrates an end-to-end incremental data pipeline built using AWS Glue Visual ETL.

The pipeline ingests CSV data from Amazon S3, processes only newly added files using Glue Job Bookmarks, performs transformations using Spark SQL, and stores the output in partitioned Parquet format for efficient querying in Athena.

---

## 🧠 Key Concepts Implemented

* Incremental data loading using Glue Job Bookmark
* S3-based data lake architecture
* Schema transformation and column selection
* SQL-based data transformation
* Partitioned Parquet storage
* Data quality validation
* Athena query integration

---

## ⚙️ Architecture

S3 (Raw CSV Files)
↓
AWS Glue (Visual ETL + Job Bookmark)
↓
S3 (Parquet - Partitioned)
↓
Athena (Query Layer)

---

## 📂 Project Structure

```
aws/
└── glue-incremental-pipeline/
    ├── scripts/
    │   └── glue_job.py
    ├── screenshots/
    │   ├── s3_raw.png
    │   ├── glue_pipeline.png
    │   ├── s3_processed.png
    │   ├── athena_query.png
    │   └── incremental_test.png
    └── README.md
```

---

## 📥 Source Data

* Format: CSV
* Location:

```
s3://your-bucket/raw/customer_orders/
```

---

## 🔄 Incremental Processing

* AWS Glue Job Bookmark is enabled
* Tracks previously processed files
* Ensures only new files are processed

### Behavior:

| Run             | Description                      |
| --------------- | -------------------------------- |
| First run       | Processes all files              |
| Subsequent runs | Processes only newly added files |

---

## 🔧 Transformation Logic

* Selected relevant columns
* Applied schema mapping
* Used SQL transformation:

```sql
SELECT *,
       CAST(price AS INT) * CAST(quantity AS INT) AS total_amount
FROM myDataSource
```

---

## 📤 Output

* Format: Parquet
* Location:

```
s3://your-bucket/silver/customer_orders/
```

* Partitioned by:

```
order_date
```

---

## 📊 Data Quality

* Applied basic validation using AWS Glue Data Quality
* Ensured dataset is not empty
* Maintained schema consistency

---

## 🔍 Query Layer (Athena)

* Queried processed data using Amazon Athena
* Verified incremental data loads
* Ensured partition-based query optimization

---

## 📷 Screenshots

### 🔹 Raw Data (S3)

![S3 Raw](screenshots/s3_raw.png)

### 🔹 Glue Visual ETL Pipeline

![Glue Pipeline](screenshots/glue_pipeline.png)

### 🔹 Processed Data (Partitioned)

![S3 Processed](screenshots/s3_processed.png)

### 🔹 Athena Query Output

![Athena Query](screenshots/athena_query.png)

### 🔹 Incremental Load Validation

![Incremental](screenshots/incremental_test.png)

---

## 🚀 How to Run

1. Upload CSV files to S3 (raw layer)
2. Create Glue Visual ETL job
3. Enable Job Bookmark
4. Run job (initial load)
5. Add new CSV file
6. Run job again (incremental load)
7. Query data using Athena

---

## 💬 Interview Explanation

“I built an incremental data pipeline using AWS Glue Visual ETL and Job Bookmarks to process only newly added files from S3. The data was transformed using Spark SQL, stored in partitioned Parquet format, and queried using Athena.”

---

## 🔥 Key Highlights

* End-to-end cloud data pipeline
* Incremental processing using Job Bookmark
* Partition optimization for performance
* Real-world ETL implementation using AWS services
