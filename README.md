# EAS-550 Fintech Analytics Dashboard

> **Live App:** [https://eas-550-fintech-analytics-dashboard.onrender.com/](https://eas-550-fintech-analytics-dashboard.onrender.com/)
>
> **GitHub Repo:** [https://github.com/atom2binghamton/EAS-550-Fintech-Analytics-Dashboard](https://github.com/atom2binghamton/EAS-550-Fintech-Analytics-Dashboard)

A financial analytics platform built on PostgreSQL with dbt star schema transformations and an interactive Streamlit dashboard deployed to the cloud and backed by a CI/CD pipeline.

---

## Table of Contents

- [Live Demo]
- [Architecture Overview]
- [Tech Stack]
- [Project Structure]
- [Database Design]
- [dbt Transformation Layer]
- [Analytics Queries]
- [CI/CD Pipeline]
- [Local Setup Instructions]
- [Environment Variables]
- [Screenshots]
- [Demo Video]
- [Performance Tuning]
- [Security Model]

---

## Live Demo

The dashboard is publicly deployed on **Render** and connects live to a **Neon** serverless PostgreSQL database.

**[Open Live Dashboard →](https://eas-550-fintech-analytics-dashboard.onrender.com/)**

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        SOURCE DATA LAYER                            │
│                                                                     │
│   CSV Files: DimCustomer, DimAccount, DimProduct,                   │
│              DimProductCategory, DimProductSubCategory,             │
│              FactTransaction                                        │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  ingest_data.py (SQLAlchemy + psycopg2)
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    OLTP DATABASE LAYER  (Neon PostgreSQL)           │
│                                                                     │
│   product_categories → product_subcategories → products             │
│   customers → accounts → transactions                               │
│                                                                     │
│   Normalized to 3NF  |  RBAC via security.sql  |  Indexed           │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  dbt build (staging → marts)
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   ANALYTICS LAYER  (dbt Star Schema)                │
│                                                                     │
│   Staging Views: stg_customers, stg_accounts, stg_transactions,     │
│                  stg_products, stg_product_categories,              │
│                  stg_product_subcategories                          │
│                                                                     │
│   Mart Tables:   dim_customer  dim_account  dim_product             │
│                          ↘         ↓        ↙                      │
│                         fact_transactions                           │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  Streamlit + Plotly
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  PRESENTATION LAYER  (Render)                       │
│                                                                     │
│   Interactive Dashboard: KPI Cards, Date/Category/Channel Filters   │
│   Charts: Monthly Trend (Line), Category Breakdown (Bar),           │
│            Channel Mix (Pie), Recent Transactions (Table)           │
└─────────────────────────────────────────────────────────────────────┘
```

### CI/CD Flow

```
Pull Request Opened / Updated
        │
        ├─► GitHub Actions: ci.yml
        │       ├── SQLFluff: lint all SQL files
        │       └── dbt test: run data quality checks
        │
        └─► GitHub Actions: neon_workflow.yml
                ├── Create Neon branch
                ├── Apply schema.sql + security.sql + indexes.sql
                ├── Load sample data via ingest_data.py
                └── dbt build (staging views + mart tables)

Pull Request Closed
        └─► Delete Neon branch automatically
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Database | [Neon](https://neon.tech/) — Serverless PostgreSQL |
| Transformation | [dbt](https://www.getdbt.com/) (dbt-postgres) |
| Data Ingestion | Python, SQLAlchemy, psycopg2, Pandas |
| Dashboard | [Streamlit](https://streamlit.io/), [Plotly](https://plotly.com/) |
| Deployment | [Render](https://render.com/) |
| CI/CD | GitHub Actions |
| SQL Linting | SQLFluff |

---

## Project Structure

```
EAS-550-Fintech-Analytics-Dashboard/
│
├── app.py                          # Streamlit dashboard (entry point)
├── ingest_data.py                  # ETL: CSV → Neon PostgreSQL
├── schema.sql                      # OLTP table definitions (3NF)
├── security.sql                    # RBAC roles and permissions
├── indexes.sql                     # Performance indexes
├── requirements.txt                # Python dependencies
├── .env.example                    # Environment variable template
│
├── data/                           # Source CSV files
│   ├── DimCustomer.csv
│   ├── DimAccount.csv
│   ├── DimProduct.csv
│   ├── DimProductCategory.csv
│   ├── DimProductSubCategory.csv
│   └── FactTransaction.csv
│
├── fintech_dbt/                    # dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/                # Views: rename/clean raw columns
│   │   │   ├── stg_accounts.sql
│   │   │   ├── stg_customers.sql
│   │   │   ├── stg_transactions.sql
│   │   │   ├── stg_products.sql
│   │   │   ├── stg_product_categories.sql
│   │   │   └── stg_product_subcategories.sql
│   │   └── marts/                  # Tables: star schema
│   │       ├── dim_account.sql
│   │       ├── dim_customer.sql
│   │       ├── dim_product.sql
│   │       ├── fact_transactions.sql
│   │       └── schema.yml          # dbt tests (uniqueness, not-null, FK)
│   └── analyses/                   # Standalone analytical SQL
│       ├── query1.sql              # Monthly regional revenue + MoM growth
│       ├── query2.sql              # Product category ranking + percentiles
│       └── query3.sql              # Cohort retention analysis
│
└── .github/workflows/
    ├── ci.yml                      # SQLFluff linting + dbt tests on PRs
    └── neon_workflow.yml           # Neon branch lifecycle management
```

---

## Database Design

### Entity-Relationship Diagram

![ERD Crow's Foot Notation](screenshots/erd_diagram.png)

### OLTP Schema (3NF)

The source database is normalized to **Third Normal Form (3NF)**, eliminating transitive dependencies across all six tables:

```
product_categories (CategoryID PK, CategoryName)
        │
        └── product_subcategories (SubCategoryID PK, SubCategoryName, CategoryID FK)
                │
                └── products (ProductID PK, ProductName, SubCategoryID FK)

customers (CustomerID PK, FirstName, LastName, DateOfBirth, Email, ...)
        │
        └── accounts (AccountID PK, CustomerID FK, AccountType, OpenDate, Balance, ...)
                │
                └── transactions (TransactionID PK, AccountID FK, ProductID FK,
                                  Amount, TransactionDate, Status, Channel, ...)
```

All tables use integer primary keys, foreign key constraints with `ON DELETE CASCADE`, and `CHECK` constraints for validation (e.g., `Amount > 0`).

---

## dbt Transformation Layer

The dbt project (`fintech_dbt/`) transforms the normalized OLTP schema into a star schema.

### Staging Models (materialized as Views)

Each staging model performs a single responsibility: rename columns to a consistent `snake_case` convention and expose only the fields needed downstream.

| Model | Source Table | Purpose |
|---|---|---|
| `stg_customers` | `customers` | Standardize customer column names |
| `stg_accounts` | `accounts` | Standardize account column names |
| `stg_transactions` | `transactions` | Standardize transaction column names |
| `stg_products` | `products` | Standardize product column names |
| `stg_product_categories` | `product_categories` | Standardize category column names |
| `stg_product_subcategories` | `product_subcategories` | Standardize subcategory column names |

### Mart Models (materialized as Tables)

| Model | Grain | Key Joins |
|---|---|---|
| `dim_customer` | One row per customer | Adds computed `age` from `DateOfBirth` |
| `dim_account` | One row per account | Joins `stg_accounts` + `stg_customers` |
| `dim_product` | One row per product | Flattens product → subcategory → category hierarchy |
| `fact_transactions` | One row per transaction | FK references to all three dimensions |

### Data Quality Tests

dbt tests defined in `schema.yml` enforce:
- **Uniqueness** on all primary keys
- **Not-null** on all required fields
- **Referential integrity** between fact and dimension tables

---

## Analytics Queries

Three analytical SQL queries are included in `fintech_dbt/analyses/`:

| Query | Description |
|---|---|
| `query1.sql` | Monthly transaction revenue by region with running totals and month-over-month growth rate |
| `query2.sql` | Product category revenue ranking with NTILE percentile buckets |
| `query3.sql` | Customer retention analysis that tracks what percentage of customers return each month after their first transaction |

---

## CI/CD Pipeline

### `ci.yml` — Quality Gate on Every PR

1. Installs Python dependencies and dbt
2. Runs **SQLFluff** to lint all `.sql` files against PostgreSQL rules
3. Runs **`dbt test`** to validate uniqueness, not-null, and referential integrity checks

### `neon_workflow.yml` — Preview Branch Lifecycle

| Trigger | Action |
|---|---|
| PR opened or updated | Create a Neon database branch scoped to the PR |
| — | Apply `schema.sql`, `security.sql`, `indexes.sql` |
| — | Run `ingest_data.py` to load sample CSV data |
| — | Run `dbt build` to create staging views and mart tables |
| PR closed | Delete the Neon branch automatically |

---

## Local Setup Instructions

### Prerequisites

- Python 3.10+
- A [Neon](https://neon.tech/) account and project
- `pip` package manager

### 1. Clone the Repository

```bash
git clone https://github.com/atom2binghamton/EAS-550-Fintech-Analytics-Dashboard.git
cd EAS-550-Fintech-Analytics-Dashboard
```

### 2. Create a Virtual Environment

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# macOS / Linux
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables

Copy the example file and fill in your Neon connection string:

```bash
cp .env.example .env
```

Edit `.env`:

```env
DATABASE_URL=postgresql://USER:PASSWORD@HOST/DATABASE?sslmode=require
```

> Find your connection string in the Neon console under **Connection Details**.

### 5. Initialize the Database Schema

```bash
psql $DATABASE_URL -f schema.sql
psql $DATABASE_URL -f security.sql
psql $DATABASE_URL -f indexes.sql
```

### 6. Ingest Sample Data

```bash
python ingest_data.py
```

This loads the six CSV files from `data/` into the OLTP tables. The script is idempotent. (re-running it will not insert duplicate rows)

### 7. Build dbt Models

```bash
cd fintech_dbt
dbt deps
dbt build
```

This creates all staging views and mart tables in your database.

### 8. Run the Dashboard Locally

```bash
cd ..
streamlit run app.py
```

The dashboard opens at `http://localhost:8501`.

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `DATABASE_URL` | Yes | PostgreSQL connection string (Neon format with `sslmode=require`) |
| `DBT_HOST` | dbt only | Neon database hostname |
| `DBT_USER` | dbt only | Database username |
| `DBT_PASSWORD` | dbt only | Database password |
| `DBT_PORT` | dbt only | Port (default: `5432`) |
| `DBT_DBNAME` | dbt only | Database name (typically `neondb`) |
| `DBT_SCHEMA` | dbt only | Target schema (typically `public`) |
| `NEON_API_KEY` | CI/CD only | Neon API key for branch management |
| `NEON_PROJECT_ID` | CI/CD only | Neon project ID for branch management |

For Render deployment, set `DATABASE_URL` in the **Environment** tab of your Render service settings.

---

## Screenshots

> Screenshots below, see [Demo Video] for a walkthrough.

![Live App Overview](screenshots/live_app_1.png)

![Live App Query](screenshots/live_app_2.png)

![GitHub Actions History](screenshots/actions_history.png)

![Neon Monitoring](screenshots/neon_monitoring.png)

---

## Demo Video



## Performance Tuning

Seven indexes were added to optimize common query patterns. Benchmarked with `EXPLAIN ANALYZE`:

| Index | Columns | Benefit |
|---|---|---|
| `idx_transactions_status_date` | `(Status, TransactionDate DESC)` | Covers filtered + sorted transaction queries |
| `idx_transactions_account_id` | `(AccountID)` | Streamlines FK join to accounts |
| `idx_transactions_product_id` | `(ProductID)` | Streamlines FK join to products |
| `idx_accounts_customer_id` | `(CustomerID)` | Speeds up customer → account traversal |
| `idx_products_subcategory_id` | `(SubCategoryID)` | Streamlines product hierarchy join |
| `idx_product_subcategories_category_id` | `(CategoryID)` | Expedites category rollup |

The cohort retention query (Query 3) showed a **2.5× improvement** in execution time (from ~210ms to ~84ms) after applying the composite index on `(Status, TransactionDate DESC)`. See `performance_tuning_report.md` for full `EXPLAIN ANALYZE` output.

---

## Security Model

Role-based access control is enforced via `security.sql`:

| Role | Permissions | Use Case |
|---|---|---|
| `Analyst` | `SELECT` only on all tables | Read-only data analysis |
| `App_User` | `SELECT`, `INSERT`, `UPDATE` on all tables | Application runtime access |
| `PUBLIC` | Revoked from `public` schema | Prevents unauthenticated access |

Default privileges are set so any future tables automatically inherit these roles.
