# Phase 2.1 - dbt Star Schema

## Setup

1. install dbt
```
pip install dbt-postgres
```

2. go into this folder
```
cd fintech_dbt
```

3. load the credentials (do this every time you open a new terminal)
```
source run_dbt.sh
```

## Run

test the connection first
```
dbt debug
```

build all the models
```
dbt run
```

run all the data quality tests
```
dbt test
```

generate the data catalog
```
dbt docs generate
dbt docs serve
```
docs will open at http://localhost:8080

## What this does

transforms the 6 OLTP tables from phase 1 into a star schema:

- staging layer: just renames columns from the raw tables
- marts layer: the actual star schema

```
fact_transactions  <-- center fact table
    |-- dim_customer
    |-- dim_account
    |-- dim_product  (category + subcategory + product all in one)
```

## Models

| model | type | description |
|-------|------|-------------|
| stg_customers | staging | renamed columns from customers |
| stg_accounts | staging | renamed columns from accounts |
| stg_transactions | staging | renamed columns from transactions |
| stg_products | staging | renamed columns from products |
| stg_product_subcategories | staging | renamed columns from product_subcategories |
| stg_product_categories | staging | renamed columns from product_categories |
| dim_customer | mart | customer dimension with age calculated |
| dim_account | mart | account dimension with customer info joined in |
| dim_product | mart | product with category hierarchy flattened |
| fact_transactions | mart | main fact table, center of star schema |
