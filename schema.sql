-- schema.sql

-- 1. Product_categories table
CREATE TABLE product_categories (
    ProductCategoryID INT PRIMARY KEY,
    ProductCategoryName VARCHAR(100) NOT NULL UNIQUE
);

-- 2. Product_subcategories table
CREATE TABLE product_subcategories (
    ProductSubCategoryID INT PRIMARY KEY,
    ProductCategoryID INT NOT NULL,
    ProductSubCategoryName VARCHAR(100) NOT NULL,
    CONSTRAINT fk_category
        FOREIGN KEY (ProductCategoryID) 
        REFERENCES product_categories(ProductCategoryID)
        ON DELETE CASCADE
);

-- 3. Products table
CREATE TABLE products (
    ProductID INT PRIMARY KEY,
    ProductSubcategoryID INT NOT NULL,
    ProductName VARCHAR(100) NOT NULL,
    CONSTRAINT fk_subcategory
        FOREIGN KEY (ProductSubcategoryID) 
        REFERENCES product_subcategories(ProductSubCategoryID)
        ON DELETE CASCADE
);

-- 4. Customers table
CREATE TABLE customers (
    CustomerID INT PRIMARY KEY,
    FullName VARCHAR(255) NOT NULL,
    DOB DATE,
    Gender VARCHAR(20) CHECK (Gender IN ('Male', 'Female', 'Other')),
    Region VARCHAR(100),
    Email VARCHAR(255) NOT NULL UNIQUE,
    Status VARCHAR(50) NOT NULL,
    JoinDate DATE NOT NULL
);

-- 5. Accounts table
CREATE TABLE accounts (
    AccountID INT PRIMARY KEY,
    CustomerID INT NOT NULL,
    AccountType VARCHAR(50) NOT NULL,
    OpenDate DATE NOT NULL,
    ClosedDate DATE,
    Status VARCHAR(50) NOT NULL,
    RegistrationID INT,
    Balance DECIMAL(15, 2) NOT NULL DEFAULT 0.00,
    CONSTRAINT fk_customer
        FOREIGN KEY (CustomerID) 
        REFERENCES customers(CustomerID)
        ON DELETE CASCADE,
    CONSTRAINT chk_dates 
        CHECK (ClosedDate IS NULL OR ClosedDate >= OpenDate)
);

-- 6. Transactions table
CREATE TABLE transactions (
    TransactionID INT PRIMARY KEY,
    AccountID INT NOT NULL,
    TransactionDate TIMESTAMPTZ NOT NULL,
    TransactionAmount DECIMAL(15, 2) NOT NULL,
    TransactionType VARCHAR(50) NOT NULL,
    TransactionChannel VARCHAR(50) NOT NULL,
    ProductID INT NOT NULL,
    Status VARCHAR(50) NOT NULL,
    CONSTRAINT fk_account
        FOREIGN KEY (AccountID) 
        REFERENCES accounts(AccountID)
        ON DELETE CASCADE,
    CONSTRAINT fk_product
        FOREIGN KEY (ProductID) 
        REFERENCES products(ProductID)
        ON DELETE CASCADE
);