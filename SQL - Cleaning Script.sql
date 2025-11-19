
---------------------------------------------
-- 0. CLEAN-UP EXISTING OBJECTS
---------------------------------------------
DROP VIEW IF EXISTS Cleaned_Data;
DROP TABLE IF EXISTS Transaction_Data_Staging2;
DROP TABLE IF EXISTS Transaction_Data_Staging;
DROP TABLE IF EXISTS Transaction_Data;
DROP TABLE IF EXISTS merchant_dictionary;
DROP TABLE IF EXISTS cleaning_log;

---------------------------------------------
-- 1. RAW TABLE (SOURCE STRUCTURE)
---------------------------------------------
CREATE TABLE Transaction_Data(
    AccountId VARCHAR(4),
    TransactionId VARCHAR(7),
    TransactionId_OBIE VARCHAR(8),
    BookingDateTime TIMESTAMP,
    ValueDateTime TIMESTAMP,
    RawDateString VARCHAR(100),
    InstructedAmount DECIMAL(12,2),
    TransactionAmount DECIMAL(12,2),
    Currency VARCHAR(3),
    OriginalCurrency VARCHAR(3),
    ExchangeRate DECIMAL(16,2),
    TransactionType VARCHAR(15),
    MerchantCategoryCode INT,
    BankTransactionCode VARCHAR(6),
    ProprietaryBankTransactionCode VARCHAR(6),
    Reference VARCHAR(50),
    Balance DECIMAL(12,2),
    CounterpartyName VARCHAR(50),
    CounterpartyAccount VARCHAR(50),
    Status VARCHAR(50),
    merchant_name VARCHAR(50),
    category VARCHAR(50)
);

---------------------------------------------
-- 2. INGEST RAW DATA
---------------------------------------------
COPY Transaction_Data 
FROM 'C:/Users/geoff/Downloads/Data Cleaning in SQL/Transaction Data.csv'
DELIMITER ',' CSV HEADER;

---------------------------------------------
-- 3. CREATE STAGING TABLE (LIKE RAW)
---------------------------------------------
CREATE TABLE Transaction_Data_Staging (LIKE Transaction_Data INCLUDING ALL);

INSERT INTO Transaction_Data_Staging
SELECT * FROM Transaction_Data;

---------------------------------------------
-- 4. STANDARDISATION
---------------------------------------------
-- Fix numeric precision
ALTER TABLE Transaction_Data_Staging
    ALTER COLUMN instructedamount TYPE DECIMAL(12,2),
    ALTER COLUMN transactionamount TYPE DECIMAL(12,2),
    ALTER COLUMN balance TYPE DECIMAL(12,2);

-- Trim whitespace in merchant and category
UPDATE Transaction_Data_Staging
SET merchant_name = TRIM(merchant_name),
    category      = TRIM(category);

-- Normalise merchant name capitalisation
UPDATE Transaction_Data_Staging
SET merchant_name = INITCAP(merchant_name);

---------------------------------------------
-- 5. CATEGORY CLEANING & STANDARDISATION
---------------------------------------------
UPDATE Transaction_Data_Staging
SET category = CASE 
    WHEN LOWER(category) LIKE '%bank%'       OR LOWER(category) LIKE '%fees%'        THEN 'Bank Fees'
    WHEN LOWER(category) LIKE '%clothing%'                                     THEN 'Clothing'
    WHEN LOWER(category) LIKE '%fit%'                                          THEN 'Fitness'
    WHEN LOWER(category) LIKE '%food%'      OR LOWER(category) LIKE '%drink%'  THEN 'Food & Drink'
    WHEN LOWER(category) LIKE '%store%'     OR LOWER(category) LIKE '%grocer%' 
                                           OR LOWER(category) LIKE '%supermarket%'  THEN 'Groceries'
    WHEN LOWER(category) LIKE '%fuel%'                                         THEN 'Fuel'
    WHEN LOWER(category) LIKE '%corr%'      OR LOWER(category) LIKE '%pted%'   THEN 'Corrupted'
    WHEN LOWER(category) LIKE '%streaming%' OR LOWER(category) LIKE '%software%' THEN 'Subscriptions'
    WHEN LOWER(category) LIKE '%income%'                                       THEN 'Income'
    WHEN LOWER(category) LIKE '%insur%'                                        THEN 'Insurance'
    WHEN LOWER(category) LIKE '%mobile%'    OR LOWER(category) LIKE '%telecom%' THEN 'Mobile / Telecom'
    WHEN LOWER(category) LIKE '%shopping%'  OR LOWER(category) LIKE '%retail%' THEN 'Online Shopping'
    WHEN LOWER(category) LIKE '%care%'                                         THEN 'Personal Care'
    WHEN LOWER(category) LIKE '%rent%'      OR LOWER(category) LIKE '%mortgages%' THEN 'Rent / Mortgages'
    WHEN LOWER(category) LIKE '%tran%'      OR LOWER(category) LIKE '%travel%' THEN 'Transport'
    WHEN LOWER(category) LIKE '%utilities%'                                    THEN 'Utilities'
    WHEN LOWER(category) LIKE '%home%'                                         THEN 'Home Furnishing'
    ELSE category
END;

---------------------------------------------
-- 6. MERCHANT DICTIONARY & FUZZY MATCHING
---------------------------------------------
CREATE TABLE merchant_dictionary (
    id SERIAL PRIMARY KEY,
    clean_name VARCHAR(255) NOT NULL,
    dirty_name VARCHAR(255) NOT NULL
);

-- Your full INSERT list from earlier goes here, unchanged:
-- INSERT INTO merchant_dictionary (clean_name, dirty_name) VALUES
-- ('Adobe', 'adobe'),
-- ...
-- ('Zara', 'Zaar');

CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;

---------------------------------------------
-- 7. CHANGE LOG TABLE
---------------------------------------------
CREATE TABLE cleaning_log (
    log_id SERIAL PRIMARY KEY,
    transactionid VARCHAR(7),
    column_changed VARCHAR(50),
    old_value TEXT,
    new_value TEXT,
    changed_at TIMESTAMP DEFAULT NOW(),
    changed_by TEXT DEFAULT CURRENT_USER
);

---------------------------------------------
-- 8. MERCHANT NORMALISATION WITH SAFER LEVENSHTEIN
---------------------------------------------
-- Log merchant name changes
INSERT INTO cleaning_log (transactionid, column_changed, old_value, new_value)
SELECT t.transactionid, 'merchant_name', t.merchant_name, d.clean_name
FROM Transaction_Data_Staging t
JOIN merchant_dictionary d
    ON levenshtein(LOWER(t.merchant_name), LOWER(d.dirty_name)) <= 1
   AND ABS(LENGTH(t.merchant_name) - LENGTH(d.dirty_name)) <= 2;

-- Apply corrections
UPDATE Transaction_Data_Staging t
SET merchant_name = d.clean_name
FROM merchant_dictionary d
WHERE levenshtein(LOWER(t.merchant_name), LOWER(d.dirty_name)) <= 1
  AND ABS(LENGTH(t.merchant_name) - LENGTH(d.dirty_name)) <= 2;

---------------------------------------------
-- 9. DUPLICATE HANDLING WITH WINDOW FUNCTION
---------------------------------------------
-- Create second staging table with an explicit row_num to identify duplicates
DROP TABLE IF EXISTS Transaction_Data_Staging2;

CREATE TABLE Transaction_Data_Staging2(
    AccountId VARCHAR(4),
    TransactionId VARCHAR(7),
    TransactionId_OBIE VARCHAR(8),
    BookingDateTime TIMESTAMP,
    ValueDateTime TIMESTAMP,
    RawDateString VARCHAR(100),
    InstructedAmount DECIMAL(12,2),
    TransactionAmount DECIMAL(12,2),
    Currency VARCHAR(3),
    OriginalCurrency VARCHAR(3),
    ExchangeRate DECIMAL(16,2),
    TransactionType VARCHAR(15),
    MerchantCategoryCode INT,
    BankTransactionCode VARCHAR(6),
    ProprietaryBankTransactionCode VARCHAR(6),
    Reference VARCHAR(50),
    Balance DECIMAL(12,2),
    CounterpartyName VARCHAR(50),
    CounterpartyAccount VARCHAR(50),
    Status VARCHAR(50),
    merchant_name VARCHAR(50),
    category VARCHAR(50),
    row_num INT
);

WITH duplicatescte AS (
SELECT *,ROW_NUMBER()
OVER (PARTITION BY
AccountId,TransactionId,TransactionId_OBIE,BookingDateTime,ValueDateTime,
RawDateString,InstructedAmount,TransactionAmount,Currency,OriginalCurrency,
ExchangeRate,TransactionType,MerchantCategoryCode,BankTransactionCode,
ProprietaryBankTransactionCode,Reference,Balance,CounterpartyName,
CounterpartyAccount,Status
ORDER BY transactionid) AS row_num
FROM Transaction_Data_Staging)

SELECT * FROM duplicatescte WHERE row_num>1;
SELECT * FROM Transaction_Data_Staging;

--- We have identified 49 duplicates which are the same across every column in our table
--- We can create a new table that only coatains unique rows

DROP TABLE IF EXISTS Transaction_Data_Staging2;
CREATE TABLE Transaction_Data_Staging2(
AccountId VARCHAR(4),
TransactionId VARCHAR(7),
TransactionId_OBIE VARCHAR(8),
BookingDateTime TIMESTAMP,
ValueDateTime TIMESTAMP,
RawDateString VARCHAR(100),
InstructedAmount DECIMAL(12,2),
TransactionAmount DECIMAL(12,2),
Currency VARCHAR(3),
OriginalCurrency VARCHAR(3),
ExchangeRate DECIMAL(16,2),
TransactionType VARCHAR(15),
MerchantCategoryCode INT,
BankTransactionCode VARCHAR(6),
ProprietaryBankTransactionCode VARCHAR(6),
Reference VARCHAR(50),
Balance DECIMAL(12,2),
CounterpartyName VARCHAR(50),
CounterpartyAccount VARCHAR(50),
Status VARCHAR(50),
merchant_name VARCHAR(50),
category VARCHAR(50),
row_num INT);

Insert into Transaction_Data_Staging2 
SELECT *,ROW_NUMBER()
OVER (PARTITION BY
AccountId,TransactionId,TransactionId_OBIE,BookingDateTime,ValueDateTime,
RawDateString,InstructedAmount,TransactionAmount,Currency,OriginalCurrency,
ExchangeRate,TransactionType,MerchantCategoryCode,BankTransactionCode,
ProprietaryBankTransactionCode,Reference,Balance,CounterpartyName,
CounterpartyAccount,Status
ORDER BY transactionid) AS row_num
FROM Transaction_Data_Staging;

SELECT count(*) FROM Transaction_Data_Staging2;

-- -- Delete the second instance of duplicates and check they are deleted.
DELETE FROM Transaction_Data_Staging2 WHERE row_num > 1;
SELECT * FROM Transaction_Data_Staging2 WHERE row_num > 1;

-- -- Delete the row_num column from table
ALTER TABLE Transaction_Data_Staging2
DROP COLUMN row_num;

Select * FROM Transaction_Data_Staging2;

---------------------------------------------
-- 10. REMOVE REDUNDANT COLUMNS
---------------------------------------------
ALTER TABLE Transaction_Data_Staging2
DROP COLUMN RawDateString;

---------------------------------------------
-- 11. NULL HANDLING
---------------------------------------------
UPDATE Transaction_Data_Staging2 
SET transactionamount = COALESCE(transactionamount, instructedamount);

---------------------------------------------
-- 12. CATEGORY FIXES FOR NEGATIVE INCOME
---------------------------------------------
UPDATE Transaction_Data_Staging2
SET category = CASE 
    WHEN instructedamount < 0 AND category = 'Income' THEN 'Income Reversal'
    ELSE category
END;

---------------------------------------------
-- 13. VALIDATION CONSTRAINTS ON CLEANED DATA
---------------------------------------------
-- Primary key on TransactionId (assuming uniqueness after de-duplication)
-- ALTER TABLE Transaction_Data_Staging2
-- ADD CONSTRAINT pk_transaction PRIMARY KEY (TransactionId);

-- Date checks: must not be on/after 1st Jan 2026
ALTER TABLE Transaction_Data_Staging2
ADD CONSTRAINT chk_valid_booking_date 
CHECK (BookingDateTime < '2026-01-01');

ALTER TABLE Transaction_Data_Staging2
ADD CONSTRAINT chk_valid_value_date 
CHECK (ValueDateTime < '2026-01-01');

-- Instructed amount should not be zero
ALTER TABLE Transaction_Data_Staging2
ADD CONSTRAINT chk_nonzero_amount
CHECK (InstructedAmount <> 0);

-- Exchange rate logic: only present when currencies differ
ALTER TABLE Transaction_Data_Staging2
ADD CONSTRAINT chk_exchange_rate_logic
CHECK (
    (Currency = OriginalCurrency AND ExchangeRate IS NULL)
    OR
    (Currency <> OriginalCurrency AND ExchangeRate IS NOT NULL)
);

---------------------------------------------
-- 14. FINAL VIEW
---------------------------------------------
CREATE VIEW Cleaned_Data AS
SELECT * FROM Transaction_Data_Staging2;

-- Optional sanity check
SELECT * FROM Cleaned_Data;

