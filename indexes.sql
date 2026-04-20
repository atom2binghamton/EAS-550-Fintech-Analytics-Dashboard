-- indexes.sql

-- 1. Transaction indexes on FK columns for account and product joins
CREATE INDEX IF NOT EXISTS idx_transactions_account_id
    ON transactions (AccountID);

CREATE INDEX IF NOT EXISTS idx_transactions_product_id
    ON transactions (ProductID);

-- 2. Transaction index on date for time-based filtering and sorting
CREATE INDEX IF NOT EXISTS idx_transactions_date
    ON transactions (TransactionDate DESC);

-- 3. Composite index on status and date for the common completed-transaction filter
CREATE INDEX IF NOT EXISTS idx_transactions_status_date
    ON transactions (Status, TransactionDate DESC);

-- 4. Account index on customer FK for the transactions -> accounts -> customers join path
CREATE INDEX IF NOT EXISTS idx_accounts_customer_id
    ON accounts (CustomerID);

-- 5. Product hierarchy indexes for the subcategory and category joins in dim_product
CREATE INDEX IF NOT EXISTS idx_products_subcategory_id
    ON products (ProductSubcategoryID);

CREATE INDEX IF NOT EXISTS idx_product_subcategories_category_id
    ON product_subcategories (ProductCategoryID);
