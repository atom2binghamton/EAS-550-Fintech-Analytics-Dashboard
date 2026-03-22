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

def generate_products_from_transactions():
    """
    No DimProduct.csv exists in the dataset.
    We generate a products table from the unique ProductIDs in FactTransaction.
    Each product is assigned to SubCategoryID 100 (first available subcategory).
    """
    df_trans = pd.read_csv("data/FactTransaction.csv")
    df_sub = pd.read_csv("data/DimProductSubCategory.csv")
    
    first_sub_id = int(df_sub['ProductSubCategoryID'].min())
    
    unique_ids = df_trans['ProductID'].dropna().unique()
    products_df = pd.DataFrame({
        'ProductID': unique_ids,
        'ProductSubcategoryID': first_sub_id,
        'ProductName': ['Product_' + str(int(pid)) for pid in unique_ids]
    })
    products_df['ProductID'] = products_df['ProductID'].astype(int)
    products_df = products_df.drop_duplicates(subset=['ProductID'])
    return products_df

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
    mask = df['ClosedDate'].notna() & (df['ClosedDate'] < df['OpenDate'])
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
    print("\n Processing product_categories...")
    df = pd.read_csv("data/DimProductCategory.csv")
    df = clean_product_categories(df)
    load_table(df, "product_categories", "ProductCategoryID")

    print("\n Processing product_subcategories...")
    df = pd.read_csv("data/DimProductSubCategory.csv")
    df = clean_product_subcategories(df)
    load_table(df, "product_subcategories", "ProductSubCategoryID")

    print("\n Generating products from FactTransaction (no DimProduct.csv)...")
    df = generate_products_from_transactions()
    load_table(df, "products", "ProductID")

    print("\n Processing customers...")
    df = pd.read_csv("data/DimCustomer.csv")
    df = clean_customers(df)
    load_table(df, "customers", "CustomerID")

    print("\n Processing customers_usa...")
    if os.path.exists("data/DimCustomerUSA.csv"):
        df = pd.read_csv("data/DimCustomerUSA.csv")
        df = clean_customers(df)
        load_table(df, "customers", "CustomerID")

    print("\n Processing accounts...")
    df = pd.read_csv("data/DimAccount.csv")
    df = clean_accounts(df)
    load_table(df, "accounts", "AccountID")

    print("\n Processing transactions...")
    df = pd.read_csv("data/FactTransaction.csv")
    df = clean_transactions(df)
    load_table(df, "transactions", "TransactionID")

    print("\n Done! All tables loaded successfully.")

if __name__ == "__main__":
    main()