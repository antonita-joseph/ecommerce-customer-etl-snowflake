# E-Commerce Customer ETL Pipeline with Snowflake

## Project Overview

This project demonstrates a complete **Extract, Transform, Load (ETL)** pipeline for e-commerce customer analytics.

The pipeline reads raw customer, order, and payment CSV files, applies business transformations, creates customer-level metrics, anonymises customer identifiers, segments customers by value, and loads the final `DIM_CUSTOMERS` table into Snowflake.

This project is designed as a portfolio project for a **Data Engineer internship role**, showing practical skills in Python, pandas, SQL-style transformations, Snowflake loading, data modelling, and production-style project organisation.

---

## Business Use Case

A retail analytics team needs a clean customer dimension table to support reporting, customer segmentation, and revenue analysis.

The final Snowflake table can be used by analysts and BI tools to answer questions such as:

- Which customers are VIP, loyal, new, or inactive?
- What is the total revenue per customer?
- What is the average order value?
- How long does delivery take on average?
- When did each customer first and last purchase?

---

## Tech Stack

- Python
- pandas
- NumPy
- Snowflake
- Snowflake Connector for Python
- SQL concepts
- Git and GitHub

---

## Project Structure

```text
ecommerce-customer-etl-snowflake/
│
├── docs/
│   └── etl_guide.html
│
├── etl_pipeline.py
├── generate_sample_data.py
├── requirements.txt
├── README.md
└── .env.example
```

---

## ETL Flow

### 1. Extract

The pipeline reads three raw CSV files:

- `olist_customers_dataset.csv`
- `olist_orders_dataset.csv`
- `olist_order_payments_dataset.csv`

The project also includes `generate_sample_data.py`, which creates realistic sample CSV files so the pipeline can be tested without downloading the full Kaggle dataset.

### 2. Transform

The transformation layer performs the following steps:

- Standardises column names and text formatting
- Removes duplicate customer records
- Joins orders and payments
- Filters delivered orders
- Calculates customer-level metrics
- Creates customer value segments
- Anonymises customer IDs using SHA-256 hashing
- Adds ETL audit columns for lineage

### 3. Load

The final transformed DataFrame is loaded into Snowflake as:

```sql
DIM_CUSTOMERS
```

The pipeline creates the target table if it does not already exist, truncates the table for a full refresh, and bulk loads the data using Snowflake's `write_pandas`.

---

## Customer Segmentation Logic

Customers are grouped into four segments:

| Segment | Logic |
|---|---|
| VIP | At least 3 orders and revenue of 500 or more |
| LOYAL | At least 2 orders or revenue of 200 or more |
| NEW | Delivered order activity but below LOYAL threshold |
| NO_ORDER | No delivered orders |

---

## Target Table: DIM_CUSTOMERS

| Column | Description |
|---|---|
| `customer_key` | Anonymised customer identifier |
| `customer_city` | Customer city |
| `customer_state` | Customer state |
| `zip_code` | Customer ZIP code prefix |
| `total_orders` | Delivered orders per customer |
| `total_revenue` | Total customer revenue |
| `avg_order_value` | Average order value |
| `avg_days_deliver` | Average delivery time |
| `first_order_date` | First purchase date |
| `last_order_date` | Last purchase date |
| `customer_segment` | VIP, LOYAL, NEW, or NO_ORDER |
| `etl_loaded_at` | ETL load timestamp |
| `etl_pipeline_ver` | Pipeline version |

---

## How to Run

### 1. Clone the repository

```bash
git clone https://github.com/YOUR_USERNAME/ecommerce-customer-etl-snowflake.git
cd ecommerce-customer-etl-snowflake
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Generate sample data

```bash
python generate_sample_data.py
```

This creates sample CSV files in the `data/` folder.

### 4. Configure Snowflake credentials

Create a `.env` file locally using `.env.example` as a template.

```bash
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_WH=COMPUTE_WH
SNOWFLAKE_DB=ANALYTICS
SNOWFLAKE_SCHEMA=ECOMMERCE
```

Do not commit the real `.env` file to GitHub.

### 5. Run the ETL pipeline

```bash
python etl_pipeline.py
```

---

## Snowflake Output

The pipeline loads data into:

```text
Database: ANALYTICS
Schema: ECOMMERCE
Table: DIM_CUSTOMERS
```

---

## Portfolio Highlights

This project demonstrates:

- End-to-end ETL pipeline development
- Data cleaning and transformation using pandas
- Customer analytics and segmentation
- Snowflake data warehouse loading
- Data privacy using hashed customer keys
- Environment variable-based configuration
- Reproducible sample data generation
- Production-style project documentation

---

## Resume Bullet

Built an end-to-end Python ETL pipeline for e-commerce customer analytics, transforming raw customer, order, and payment data into a Snowflake customer dimension table with customer segmentation, delivery metrics, revenue KPIs, audit columns, and anonymised customer keys.

---

## Documentation

A visual ETL guide is available in:

```text
docs/etl_guide.html
```

Open this file in a browser to view the project flow and architecture.
