# STEDI Human Balance Analytics (Spark & AWS Glue)

## Project Overview
This project implements a **data lakehouse solution on AWS** for the STEDI Step Trainer team. The objective is to curate sensor and mobile application data so that **data scientists can train a machine learning model** to accurately detect steps, while strictly enforcing **customer privacy requirements**.

The solution leverages **Amazon S3, AWS Glue, Apache Spark, and Amazon Athena** to ingest raw JSON data, apply transformations, and produce analytics- and ML-ready datasets.

---

## Data Sources
Raw data is stored in Amazon S3 as JSON files:

- **Customer data** (mobile application user information)  
  `s3://stedi-mila-lake-house/customer/landing/`

- **Accelerometer data** (mobile phone motion data: X, Y, Z axes)

- **Step Trainer data** (distance sensor readings from the STEDI device)

Only customers who have consented to share their data for research purposes are included in downstream datasets.

---

## Architecture & Data Layers

### 1. Landing (Raw)
- Original JSON files stored in Amazon S3
- Tables created using **AWS Glue Crawlers**
- No transformations applied
- Schema-on-read approach

**Tables:**
- `landing_customer`
- `landing_accelerometer`
- `landing_step_trainer`

---

### 2. Trusted
- Created using **AWS Glue Spark jobs**
- Privacy rules enforced
- Data converted to **Parquet** format

**Tables:**
- `customer_trusted`  
  - Contains only customers who have consented to research usage
- `accelerometer_trusted`  
  - Accelerometer data joined with `customer_trusted`
- `step_trainer_trusted`  
  - Step Trainer data joined with `customer_trusted`

---

### 3. Curated (Machine Learning)
- Final dataset prepared for machine learning model training
- Joins trusted accelerometer and step trainer data
- Time-aligned sensor readings
- Stored in **Parquet** format

**Table:**
- `machine_learning_curated`

---

## ETL Process
1. Raw JSON data is ingested into Amazon S3 (Landing layer)
2. AWS Glue Crawlers create Data Catalog tables for landing data
3. AWS Glue Spark jobs:
   - Filter customers based on research consent
   - Join sensor data with consented customers
   - Write trusted and curated datasets back to S3
4. Output datasets are registered in the AWS Glue Data Catalog
5. Data is validated and queried using Amazon Athena

---

## Privacy Considerations
- Only customers who have explicitly opted in to research are included
- All sensor data is filtered using the trusted customer dataset
- Non-consented customer data is excluded from machine learning training datasets

---

## Technologies Used
- Amazon S3  
- AWS Glue (Crawlers and Spark Jobs)  
- AWS Glue Data Catalog  
- Apache Spark  
- Amazon Athena  

---

## Outcome
The final curated dataset enables the STEDI data science team to:
- Train step-detection machine learning models
- Query sensor data efficiently
- Maintain strong privacy and data governance standards
