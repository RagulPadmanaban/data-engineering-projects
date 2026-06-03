# Microsoft Fabric Multicloud Data Engineering Project

## Overview

This project demonstrates an end-to-end Data Engineering solution using Microsoft Fabric and Medallion Architecture.

The solution ingests retail sales data, processes it through Bronze, Silver, and Gold layers, implements Incremental Loading and SCD Type 2, orchestrates pipelines dynamically, and delivers business insights through Power BI dashboards.

---

# Architecture

## Medallion Architecture

- Bronze Layer - Raw Data Ingestion
- Silver Layer - Data Cleansing and Transformation
- Gold Layer - Business Analytics and Reporting

---

# Technologies Used

- Microsoft Fabric
- Lakehouse
- Warehouse
- Data Pipelines
- PySpark
- Spark SQL
- Power BI
- Incremental Loading
- SCD Type 2
- Medallion Architecture

---

# Project Components

## Bronze Layer

- Metadata-driven ingestion
- Dynamic pipeline execution
- Parallel data loading
- Raw data storage

## Silver Layer

- Data cleansing
- Business transformations
- Incremental loading
- SCD Type 2 implementation

## Gold Layer

- Sales Analytics
- Customer Analytics
- Product Analytics
- Inventory Analytics
- Payment Analytics

---

# Data Model

### Dimension Tables

- DimCustomer
- DimProduct
- DimCategory
- DimSupplier
- DimInventory

### Fact Tables

- FactOrderItems
- FactPayment

---

# Pipeline Orchestration

Implemented loosely coupled orchestration using:

- Metadata-driven pipelines
- Parallel notebook execution
- Dependency-based execution flow

### Execution Flow

```text
Bronze Ingestion
       |
       v
DimCategory   DimSupplier   DimCustomer
       |           |             |
       +-----------+-------------+
                   |
             DimProduct
                   |
              DimInventory
                   |
               DimOrder
                   |
            FactOrderItems
                   |
              FactPayment
```

---

# Dashboard Features

- KPI Cards
- Total Sales Analysis
- Product Sales Analysis
- Sales Trend Analysis
- Sales Category Distribution
- Interactive Filters

---

# Business Logic

- Incremental Loading using Watermark Columns
- Slowly Changing Dimension (SCD Type 2)
- Metadata-Driven Ingestion Framework
- Dynamic Pipeline Execution
- Truncate and Load Gold Layer Strategy
- Sales Category Classification

---

# Power BI Dashboard

The dashboard includes:

- Total Sales KPI
- Total Orders KPI
- Product-wise Sales Analysis
- Sales Trend Analysis
- Sales Category Distribution
- Interactive Slicer Filters

---

# Future Enhancements

- Real-Time Streaming Ingestion
- CI/CD Integration
- Monitoring and Alerting
- Data Quality Framework
- Advanced Power BI Reporting

---

# Project Status

✅ Bronze Layer Completed

✅ Silver Layer Completed

✅ Gold Layer Completed

✅ Incremental Loading Implemented

✅ SCD Type 2 Implemented

✅ Dynamic Pipeline Developed

✅ Power BI Dashboard Created

✅ End-to-End Data Engineering Workflow Completed

---

# Author

**Ragul P**

Data Engineer

GitHub: https://github.com/RagulPadmanaban
