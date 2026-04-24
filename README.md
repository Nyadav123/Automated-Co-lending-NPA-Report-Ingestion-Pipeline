# 📊 Automated Co-lending & NPA Report Ingestion Pipeline

This project implements an **end-to-end automated data pipeline** for Co-lending and NPA report ingestion using AWS services. It is designed to securely transfer, process, and store reports between internal systems and external SFTP servers with proper monitoring and alerting.

---

# 🚀 Architecture Overview

The solution is divided into **two phases**:

* **Phase 1** → Upload reports from DWH → S3 → SFTP
* **Phase 2** → Fetch reports from SFTP → Process → Store in DB & S3

---

# ⚙️ Phase 1: Co-lending Report Ingestion (Outbound Flow)

## 🔄 Operational Workflow

1. Reports are generated from the **DWH Server**
2. Files are uploaded to a designated **S3 bucket path**
3. **S3 Event Trigger** invokes a Lambda function
4. Lambda:

   * Fetches credentials from **AWS Secrets Manager**
   * Downloads file using **S3 VPC Endpoint**
   * Connects to external **SFTP (DCB Server)** via **NAT Gateway**
5. File is uploaded to the target SFTP path
6. **SNS Notification** is triggered for:

   * Success ✅
   * Failure ❌

---

## 💻 Code Workflow

1. Lambda receives S3 event
2. Extracts S3 object key
3. Downloads file into `/tmp` storage
4. Fetches SFTP credentials from Secrets Manager
5. Establishes SFTP connection
6. Uploads file to remote server
7. Closes connection
8. Sends notification via SNS

### ❗ Error Handling

* Lambda timeout exceeded
* Storage limit exceeded
* SFTP connection failure
* Invalid destination path
* Upload timeout

Each failure triggers **email alerts via SNS**

---

# ⚙️ Phase 2: NPA Report Ingestion (Inbound Flow)

## 🔄 Operational Workflow

1. **EventBridge Scheduler** triggers Lambda
2. Lambda connects to **SFTP Server**
3. Downloads report file (CSV/Excel)
4. Processes data using in-memory dataframe
5. Performs transformation & filtering
6. Uploads processed file to **S3 bucket**
7. Inserts filtered data into **Database**
8. Sends notification for success/failure

---

## 💻 Code Workflow

1. Fetch credentials (SFTP + DB) from Secrets Manager
2. Read configuration from environment variables:

   * File prefix
   * Queries
   * Table name
3. Connect to SFTP server
4. Download file to Lambda temp storage
5. Upload raw file to S3
6. Read file into dataframe
7. Fetch reference data from DB
8. Apply transformation logic
9. Create final dataset
10. Truncate target table
11. Insert processed data
12. Generate final Excel report
13. Upload final report to S3 (partitioned by date)
14. Close all connections

---

## ❗ Error Handling

* Secret fetch failure
* SFTP connection/download error
* DB connection/query failure
* Data processing issues
* S3 upload failure

All failures trigger **SNS email alerts**

---

# 🧰 Tech Stack

* **AWS Lambda**
* **Amazon S3**
* **Amazon SNS**
* **AWS Secrets Manager**
* **Amazon EventBridge**
* **VPC + NAT Gateway + VPC Endpoints**
* **SFTP (External Server Integration)**
* **Python (Pandas for data processing)**
* **SQL Database**

---

# 🔐 Security Best Practices

* Credentials stored in **AWS Secrets Manager**
* Private subnet execution using **VPC**
* Secure outbound connectivity via **NAT Gateway**
* Restricted S3 access via **VPC Endpoint**
* IAM roles with least privilege access

---

# 📬 Notifications

* Integrated with **Amazon SNS**
* Email alerts for:

  * Success
  * Failures
  * Timeout scenarios
  * Data issues

---

# 📁 S3 Structure (Example)

s3://bucket-name/
│
├── raw/
│   └── reports/
│
├── processed/
│   └── year=YYYY/month=MM/day=DD/
│
└── logs/

