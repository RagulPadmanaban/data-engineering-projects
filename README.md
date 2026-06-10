# 🚀 Data Engineering Projects

This repository contains end-to-end data engineering projects built using modern cloud platforms, big data technologies, and analytics solutions.

---

## 👨‍💻 About Me

Data Engineer with hands-on experience in designing and building scalable ETL/ELT pipelines using Azure, Databricks, Microsoft Fabric, and AWS technologies.

Experienced in implementing metadata-driven ingestion frameworks, Medallion Architecture, incremental processing, data warehousing, and business analytics solutions.

---

## 📂 Projects

### 🟣 Microsoft Fabric

#### Microsoft Fabric Multicloud Analytics Platform

**Technologies:** Microsoft Fabric, OneLake, Lakehouse, Warehouse, Data Factory, Shortcuts, Power BI, SQL

**Key Features:**

* Built an end-to-end analytics platform using Microsoft Fabric Medallion Architecture (Bronze → Silver → Gold).
* Leveraged OneLake Shortcuts to integrate data from multiple cloud storage locations without physical data duplication.
* Developed metadata-driven ingestion pipelines using Lookup, ForEach, and Copy Data activities.
* Implemented SCD Type 2 processing and business transformations across Silver Layer dimension and fact datasets.
* Created Gold Layer analytical tables for Sales, Customer, Product, Inventory, Payment, and Trend Analytics.
* Developed interactive Power BI dashboards for KPI monitoring and business reporting.
* Implemented dependency-based notebook orchestration for dimension and fact processing workflows.

---

### 🔷 Databricks

#### Incremental Lakehouse Pipeline

**Technologies:** Azure Databricks, Delta Lake, PySpark, Azure Synapse

**Key Features:**

* Metadata-driven ingestion framework.
* Delta Lake MERGE operations.
* Incremental loading using watermark columns.
* Medallion Architecture (Bronze → Silver → Gold).
* SCD Type 2 implementation using PySpark.

---

### ☁️ AWS

#### AWS Glue Incremental Data Pipeline

**Technologies:** AWS Glue, S3, Athena

**Key Features:**

* S3 → Glue → Parquet → Athena architecture.
* Job Bookmark-based incremental processing.
* Partitioned data design for optimized querying.
* Automated ETL workflows using AWS Glue.

---

### 🔵 Azure

#### Azure Synapse Data Warehouse Migration

**Technologies:** Azure Synapse Analytics, Azure Data Factory, SQL Server

**Key Features:**

* Metadata-driven ingestion pipelines.
* Data warehouse migration from on-premises systems.
* PolyBase and Copy Activity-based ingestion.
* Incremental and historical load processing.
* Performance optimization using external tables.

---

## 🧠 Skills Demonstrated

* ETL / ELT Pipeline Development
* Metadata-Driven Ingestion Frameworks
* Incremental Data Processing
* SCD Type 2 Implementation
* Data Warehousing
* Medallion Architecture
* Data Modeling
* Lakehouse Architecture
* Pipeline Orchestration
* Power BI Analytics
* Cloud Data Engineering (Microsoft Fabric, Azure, AWS, Databricks)

---

## ⚙️ Architecture Patterns Used

* Batch Processing Pipelines
* Incremental Load (Watermark / Bookmark)
* Metadata-Driven Ingestion
* Medallion Architecture
* Star Schema Modeling
* Dependency-Based Orchestration
* Lakehouse Architecture

---

## 📌 Repository Structure

```text
data-engineering-projects/
│
├── Fabric/
├── Databricks/
├── AWS/
├── Azure/
└── README.md
```

---

## 📎 Repository Contents

Each project folder contains:

* Source Code / Notebooks
* Architecture Diagrams
* Pipeline Screenshots
* Dashboard Screenshots
* Documentation
* SQL Scripts

---

## 🚀 Goal

To design scalable, efficient, and production-ready data engineering solutions across Microsoft Fabric, Azure, Databricks, and AWS cloud platforms.
