This post is part of a series where I document my learnings from the “Data Engineering Zoomcamp” course, created by DataTalksClub. The course material can be found on GitHub here: [DataTalksClub/data-engineering-zoomcamp: Free Data Engineering course!](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main)

# NYC Taxi Pipeline with Bruin

This project demonstrates a **data pipeline for NYC Taxi trip data** using **Bruin** a unified CLI tool for data ingestion, transformation, orchestration, and governance. The pipeline ingests taxi trip data, processes it through staging transformations, and produces analytical reports.

---

## 📊 Architecture Overview

The pipeline follows a **3-layer data flow**:

```
Raw Data → Ingestion → Staging → Reporting
```

1. **Ingestion Layer**
    - Collects taxi trip data
    - Loads raw data into the data warehouse
2. **Staging Layer**
    - Cleans and standardizes the ingested data
    - Prepares data for analytics
3. **Reporting Layer**
    - Aggregates data
    - Produces analytical reports

---

# #🛠 Tech Stack

This project uses the following tools:

- **Python** – data ingestion logic
- **SQL** – data transformations and reporting
- **Bruin** – pipeline orchestration
- **Virtual Environment (venv)** – dependency isolation
- **DuckDB / Warehouse (depending on configuration)**

---

## 📂 Project Structure

```
nyc-taxi-pipeline-with-bruin
│
├── README.md
│
└── my-taxi-pipeline
    ├── README.md
    │
    ├── pipeline
    │   ├── assets
    │   │
    │   │── ingestion
    │   │   ├── payment_lookup.asset.yml
    │   │   ├── requirements.txt
    │   │   └── trips.py
    │   │
    │   │── staging
    │   │   └── trips.sql
    │   │
    │   │── reports
    │   │   └── trips_report.sql
    │   │
    │   └── pipeline.yml
    │
    └── venv
```

---

## 📦 Pipeline Components

#### 1️⃣ Ingestion

**Location**

```
pipeline/assets/ingestion
```

Files:

- `trips.py`
    
    Python script that ingests NYC taxi trip data.
    
- `payment_lookup.asset.yml`
    
    Bruin asset configuration for the ingestion step.
    
- `requirements.txt`
    
    Python dependencies for the ingestion pipeline.
    

---

#### 2️⃣ Staging

**Location**

```
pipeline/assets/staging
```

File:

- `trips.sql`

This SQL script performs:

- Data cleaning
- Column standardization
- Preparing the dataset for analytics

---

#### 3️⃣ Reporting

**Location**

```
pipeline/assets/reports
```

File:

- `trips_report.sql`

This query generates analytical reports such as:

- trip statistics
- payment type summaries
- aggregated metrics

---

## ⚙️ Pipeline Configuration

The main pipeline configuration is defined in:

```
pipeline/pipeline.yml
```

This file defines:

- pipeline name
- assets
- execution order
- dependencies between ingestion, staging, and reporting.

---

## 🚀 How to Run the Pipeline

#### 1️⃣ Clone the Repository

```
git clone https://github.com/yourusername/nyc-taxi-pipeline-with-bruin.git
cd nyc-taxi-pipeline-with-bruin/my-taxi-pipeline
```

---

#### 2️⃣ Create Virtual Environment

```
python3-m venv venv
```

Activate it:

Linux / WSL

```
source venv/bin/activate
```

Windows

```
venv\Scripts\activate
```

---

#### 3️⃣ Install Dependencies

```
pip install-r pipeline/assets/ingestion/requirements.txt
```

---

#### 4️⃣ Run the Pipeline

Example run:

```
bruin run ./pipeline/pipeline.yml \
--start-date2022-01-01 \
--end-date2022-01-31
```

Bruin will:

1. Run ingestion assets
2. Execute staging transformations
3. Generate reports

---

## 📈 Example Output

After the pipeline runs successfully, you will have:

- cleaned taxi trip data
- transformed staging tables
- analytical report tables

These outputs can be used for:

- dashboards
- analytics
- further data modeling

---

## 🎯 Learning Objectives

This project demonstrates:

- building a **modular data pipeline**
- orchestrating pipelines with **Bruin**
- combining **Python ingestion + SQL transformation**
- structuring a **data engineering project**