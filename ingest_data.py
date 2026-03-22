import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

def clean_dim_product_category(df):
    df = df.drop_duplicates()
    df = df.dropna(subset=['ProductCategoryID'])
    df['ProductCategoryID'] = df['ProductCategoryID'].astype(int)
    df['ProductCategoryName'] = df['ProductCategoryName'].astype(str).str.strip()
    return df

def clean_dim_product_subcategory(df):
    df = df.drop_duplicates()
    df = df.dropna(subset=['ProductSubCategoryID', 'ProductCategoryID'])
    df['ProductSubCategoryID'] = df['ProductSubCategoryID'].astype(int)
    df['ProductCategoryID'] = df['ProductCategoryID'].astype(int)
    df['ProductSubCategoryName'] = df['ProductSubCategoryName'].astype(str).str.strip()
    return df

def clean_dim_customer(df):
    df = df.drop_duplicates()
    df = df.dropna(subset=['CustomerID'])
    df['CustomerID'] = df['CustomerID'].astype(int)
    df['FullName'] = df['FullName'].astype(str).str.strip()
    df['Email'] = df['Email'].astype(str).str.strip().str.lower()
    df['Status'] = df['Status'].astype(str).str.strip()
    df['Gender'] = df['Gender'].astype(str).str.strip()
    df['Region'] = df['Region'].astype(str).str.strip()
    return df

def clean_dim_customer_usa(df):
    df = df.drop_duplicates()
    df = df.dropna(subset=['CustomerID'])
    df['CustomerID'] = df['CustomerID'].astype(int)
    df['FullName'] = df['FullName'].astype(str).str.strip()
    df['Email'] = df['Email'].astype(str).str.strip().str.lower()
    df['Status'] = df['Status'].astype(str).str.strip()
    df['Gender'] = df['Gender'].astype(str).str.strip()
    df['Region'] = df['Region'].astype(str).str.strip()
    return df

def clean_dim_account(df):
    df = df.drop_duplicates()
    df = df.dropna(subset=['AccountID'])
    df['AccountID'] = df['AccountID'].astype(int)
    df['CustomerID'] = df['CustomerID'].astype(int)
    df['Balance'] = pd.to_numeric(df['Balance'], errors='coerce').fillna(0)
    df['AccountType'] = df['AccountType'].astype(str).str.strip()
    df['Status'] = df['Status'].astype(str).str.strip()
    df['OpenDate'] = pd.to_datetime(df['OpenDate'], errors='coerce')
    df['ClosedDate'] = pd.to_datetime(df['ClosedDate'], errors='coerce')
    return df

def clean_fact_transaction(df):
    df = df.drop_duplicates()
    df = df.dropna(subset=['TransactionID'])
    df['TransactionID'] = df['TransactionID'].astype(int)
    df['AccountID'] = df['AccountID'].astype(int)
    df['ProductID'] = pd.to_numeric(df['ProductID'], errors='coerce').fillna(0).astype(int)
    df['TransactionAmount'] = pd.to_numeric(df['TransactionAmount'], errors='coerce').fillna(0)
    df['TransactionDate'] = pd.to_datetime(df['TransactionDate'], errors='coerce')
    df['TransactionType'] = df['TransactionType'].astype(str).str.strip()
    df['TransactionChannel'] = df['TransactionChannel'].astype(str).str.strip()
    df['Status'] = df['Status'].astype(str).str.strip()
    return df

def load_table(df, table_name, id_col):
    with engine.connect() as conn:
        try:
            existing_ids = pd.read_sql(
                text(f"SELECT {id_col} FROM {table_name}"), conn
            )[id_col].tolist()
        except Exception:
            existing_ids = []

    new_rows = df[~df[id_col].isin(existing_ids)]
    if len(new_rows) > 0:
        new_rows.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"  Inserted {len(new_rows)} rows into {table_name}")
    else:
        print(f"  No new rows for {table_name} (already loaded)")

def main():
    print("\n Loading DimProductCategory...")
    df = pd.read_csv("data/DimProductCategory.csv")
    df = clean_dim_product_category(df)
    load_table(df, "DimProductCategory", "ProductCategoryID")

    print("\n Loading DimProductSubCategory...")
    df = pd.read_csv("data/DimProductSubCategory.csv")
    df = clean_dim_product_subcategory(df)
    load_table(df, "DimProductSubCategory", "ProductSubCategoryID")

    print("\n Loading DimCustomer...")
    df = pd.read_csv("data/DimCustomer.csv")
    df = clean_dim_customer(df)
    load_table(df, "DimCustomer", "CustomerID")

    print("\n Loading DimCustomerUSA...")
    df = pd.read_csv("data/DimCustomerUSA.csv")
    df = clean_dim_customer_usa(df)
    load_table(df, "DimCustomerUSA", "CustomerID")

    print("\n Loading DimAccount...")
    df = pd.read_csv("data/DimAccount.csv")
    df = clean_dim_account(df)
    load_table(df, "DimAccount", "AccountID")

    print("\n Loading FactTransaction...")
    df = pd.read_csv("data/FactTransaction.csv")
    df = clean_fact_transaction(df)
    load_table(df, "FactTransaction", "TransactionID")

    print("\n Done! All tables loaded successfully.")

if __name__ == "__main__":
    main()