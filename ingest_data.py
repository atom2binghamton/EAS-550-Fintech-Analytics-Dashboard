import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()

raw_url = os.getenv("DATABASE_URL")
if raw_url and raw_url.startswith("postgres://"):
    DATABASE_URL = raw_url.replace("postgres://", "postgresql://", 1)
else:
    DATABASE_URL = raw_url

engine = create_engine(DATABASE_URL)

def clean_product_categories(df):
    df = df.drop_duplicates().dropna(subset=['ProductCategoryID'])
    df['ProductCategoryID'] = df['ProductCategoryID'].astype(int)
    df['ProductCategoryName'] = df['ProductCategoryName'].astype(str).str.strip()
    return df

def clean_product_subcategories(df):
    df = df.drop_duplicates().dropna(subset=['ProductSubCategoryID', 'ProductCategoryID'])
    df['ProductSubCategoryID'] = df['ProductSubCategoryID'].astype(int)
    df['ProductCategoryID'] = df['ProductCategoryID'].astype(int)
    df['ProductSubCategoryName'] = df['ProductSubCategoryName'].astype(str).str.strip()
    return df

def clean_customers(df):
    df = df.drop_duplicates().dropna(subset=['CustomerID'])
    df['CustomerID'] = df['CustomerID'].astype(int)
    df['FullName'] = df['FullName'].astype(str).str.strip()
    df['Email'] = df['Email'].astype(str).str.strip().str.lower()
    df['Gender'] = df['Gender'].astype(str).str.strip()
    df['Region'] = df['Region'].astype(str).str.strip()
    df['Status'] = df['Status'].astype(str).str.strip()
    df['DOB'] = pd.to_datetime(df['DOB'], dayfirst=True, errors='coerce')
    df['JoinDate'] = pd.to_datetime(df['JoinDate'], dayfirst=True, errors='coerce')
    return df

def clean_accounts(df):
    df = df.drop_duplicates().dropna(subset=['AccountID'])
    df['AccountID'] = df['AccountID'].astype(int)
    df['CustomerID'] = df['CustomerID'].astype(int)
    df['Balance'] = pd.to_numeric(df['Balance'], errors='coerce').fillna(0)
    df['AccountType'] = df['AccountType'].astype(str).str.strip()
    df['Status'] = df['Status'].astype(str).str.strip()
    df['OpenDate'] = pd.to_datetime(df['OpenDate'], dayfirst=True, errors='coerce')
    df['ClosedDate'] = pd.to_datetime(df['ClosedDate'], dayfirst=True, errors='coerce')
    mask = df['ClosedDate'] < df['OpenDate']
    df.loc[mask, ['OpenDate', 'ClosedDate']] = df.loc[mask, ['ClosedDate', 'OpenDate']].values
    return df

def clean_transactions(df):
    df = df.drop_duplicates().dropna(subset=['TransactionID'])
    df['TransactionID'] = df['TransactionID'].astype(int)
    df['AccountID'] = df['AccountID'].astype(int)
    df['ProductID'] = pd.to_numeric(df['ProductID'], errors='coerce').fillna(0).astype(int)
    df['TransactionAmount'] = pd.to_numeric(df['TransactionAmount'], errors='coerce').fillna(0)
    df['TransactionDate'] = pd.to_datetime(df['TransactionDate'], dayfirst=True, errors='coerce')
    df['TransactionType'] = df['TransactionType'].astype(str).str.strip()
    df['TransactionChannel'] = df['TransactionChannel'].astype(str).str.strip()
    df['Status'] = df['Status'].astype(str).str.strip()
    return df

def load_table(df, table_name, id_col):
    """Inserts rows that do not already exist in the target table."""
    try:
        with engine.connect() as conn:
            query = text(f"SELECT {id_col} FROM {table_name}")
            existing_ids = pd.read_sql(query, conn)[id_col.lower()].tolist()
    except Exception:
        existing_ids = []

    new_rows = df[~df[id_col].isin(existing_ids)]

    if len(new_rows) > 0:
        new_rows.columns = [c.lower() for c in new_rows.columns]
        new_rows.to_sql(table_name, engine, if_exists='append', index=False, method='multi')
        print(f"  {table_name}: Inserted {len(new_rows)} rows.")
    else:
        print(f"  {table_name}: No new rows to add.")

def main():
    tasks = [
        ("data/DimProductCategory.csv", "product_categories", "ProductCategoryID", clean_product_categories),
        ("data/DimProductSubCategory.csv", "product_subcategories", "ProductSubCategoryID", clean_product_subcategories),
        ("data/DimCustomer.csv", "customers", "CustomerID", clean_customers),
        ("data/DimCustomerUSA.csv", "customers_usa", "CustomerID", clean_customers),
        ("data/DimAccount.csv", "accounts", "AccountID", clean_accounts),
        ("data/FactTransaction.csv", "transactions", "TransactionID", clean_transactions)
    ]

    for file_path, table, pk, clean_func in tasks:
        if os.path.exists(file_path):
            print(f"Processing {file_path} -> {table}...")
            df = pd.read_csv(file_path)
            df = clean_func(df)
            load_table(df, table, pk)
        else:
            print(f"⚠️ Warning: {file_path} not found. Skipping {table}.")

if __name__ == "__main__":
    main()
    #testing
