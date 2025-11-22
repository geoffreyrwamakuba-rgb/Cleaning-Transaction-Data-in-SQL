---------------------------------------------
-- 0. CLEAN-UP EXISTING OBJECTS
---------------------------------------------
DROP VIEW IF EXISTS Cleaned_Data;
DROP TABLE IF EXISTS Staging_Table2;
DROP TABLE IF EXISTS Staging_Table;
DROP TABLE IF EXISTS Transaction_Data;
DROP TABLE IF EXISTS merchant_dictionary;
DROP TABLE IF EXISTS cleaning_log;

---------------------------------------------
-- 1. CREATE RAW TABLE
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
-- 2. IMPORT RAW DATA
---------------------------------------------
COPY Transaction_Data 
FROM 'C:/Users/geoff/Downloads/Data Cleaning in SQL/Transaction Data.csv'
DELIMITER ',' CSV HEADER;

---------------------------------------------
-- 3. CREATE STAGING TABLE
---------------------------------------------
CREATE TABLE Staging_Table (LIKE Transaction_Data INCLUDING ALL);

INSERT INTO Staging_Table
SELECT * FROM Transaction_Data;

---------------------------------------------
-- 4. STANDARDISATION
---------------------------------------------
-- Fix numeric precision
ALTER TABLE Staging_Table
    ALTER COLUMN instructedamount TYPE DECIMAL(12,2),
    ALTER COLUMN transactionamount TYPE DECIMAL(12,2),
    ALTER COLUMN balance TYPE DECIMAL(12,2);

-- Trim whitespace in merchant and category
UPDATE Staging_Table
SET merchant_name = TRIM(merchant_name),
    category = TRIM(category);

-- Normalise merchant name capitalisation
UPDATE Staging_Table
SET merchant_name = INITCAP(merchant_name);

---------------------------------------------
-- 5. CATEGORY CLEANING & STANDARDISATION
---------------------------------------------
UPDATE Staging_Table
SET category = CASE 
    WHEN LOWER(category) LIKE '%bank%' OR LOWER(category) LIKE '%fees%' THEN 'Bank Fees'
    WHEN LOWER(category) LIKE '%clothing%' THEN 'Clothing'
    WHEN LOWER(category) LIKE '%fit%' THEN 'Fitness'
    WHEN LOWER(category) LIKE '%food%' OR LOWER(category) LIKE '%drink%'  THEN 'Food & Drink'
    WHEN LOWER(category) LIKE '%store%' OR LOWER(category) LIKE '%grocer%' OR LOWER(category) LIKE '%supermarket%'  THEN 'Groceries'
    WHEN LOWER(category) LIKE '%fuel%' THEN 'Fuel'
    WHEN LOWER(category) LIKE '%corr%' OR LOWER(category) LIKE '%pted%' THEN 'Corrupted'
    WHEN LOWER(category) LIKE '%streaming%' OR LOWER(category) LIKE '%software%' THEN 'Subscriptions'
    WHEN LOWER(category) LIKE '%income%' THEN 'Income'
    WHEN LOWER(category) LIKE '%insur%' THEN 'Insurance'
    WHEN LOWER(category) LIKE '%mobile%' OR LOWER(category) LIKE '%telecom%' THEN 'Mobile / Telecom'
    WHEN LOWER(category) LIKE '%shopping%' OR LOWER(category) LIKE '%retail%' THEN 'Online Shopping'
    WHEN LOWER(category) LIKE '%care%' THEN 'Personal Care'
    WHEN LOWER(category) LIKE '%rent%' OR LOWER(category) LIKE '%mortgages%' THEN 'Rent / Mortgages'
    WHEN LOWER(category) LIKE '%tran%' OR LOWER(category) LIKE '%travel%' THEN 'Transport'
    WHEN LOWER(category) LIKE '%utilities%' THEN 'Utilities'
    WHEN LOWER(category) LIKE '%home%' THEN 'Home Furnishing'
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

INSERT INTO merchant_dictionary (clean_name, dirty_name) VALUES
('****', '****'),

('Adobe', 'adobe'),
('Adobe', 'Adboe'),
('Adobe', 'Abode'),
('Adobe', 'Adoeb'),
('Adobe', 'adboe'),
('Adobe', 'Aodbe'),
('Adobe', 'Daobe'),

('Amazon', 'amazon'),
('Amazon', 'Amzon'),
('Amazon', 'Amaon'),
('Amazon', 'Amazn'),
('Amazon', 'Amzan'),
('Amazon', 'amason'),
('Amazon', 'Maazon'),

('Electricity Co', 'electricity co'),
('Electricity Co', 'Electricty Co'),
('Electricity Co', 'ElectricityCo'),
('Electricity Co', 'electrcity co'),
('Electricity Co', 'electicity co'),
('Electricity Co', 'Elcetricity Co'),
('Electricity Co', 'Leectricity Co'),

('Gymbox', 'Gmybox'),
('Gymbox', 'Gymbx'),
('Gymbox', 'Gym Box'),
('Gymbox', 'gymbox'),
('Gymbox', 'Gynbox'),
('Gymbox', 'Gybmox'),

('Hair Salon', 'Hair Slaon'),
('Hair Salon', 'Hiar Salon'),
('Hair Salon', 'Hair Salno'),
('Hair Salon', 'hair salon'),
('Hair Salon', 'Hair Salon '),
('Hair Salon', 'Hair Aslon'),
('Hair Salon', 'Hari Salon'),

('Health Insurance', 'Health Inusrance'),
('Health Insurance', 'Helath Insurance'),
('Health Insurance', 'Health Insuranc'),
('Health Insurance', 'health insurance'),
('Health Insurance', 'Health Insurnace'),
('Health Insurance', 'Ehalth Insurance'),
('Health Insurance', 'Healht Insurance'),
('Health Insurance', 'Health Nisurance'),

('IKEA', 'IEKA'),
('IKEA', 'ikea'),
('IKEA', 'IKEA '),
('IKEA', 'IKAE'),
('IKEA', 'ieka'),
('IKEA', 'Kiea'),

('LANDLORD LTD', 'landlord ltd'),
('LANDLORD LTD', 'Landlord Ltd'),
('LANDLORD LTD', 'Lnadlord Ltd'),
('LANDLORD LTD', 'landord ltd'),
('LANDLORD LTD', 'landlordltd'),
('LANDLORD LTD', 'Alndlord Ltd'),
('LANDLORD LTD', 'Ladnlord Ltd'),
('LANDLORD LTD', 'Landlodr Ltd'),
('LANDLORD LTD', 'Landlrod Ltd'),

('Netflix', 'Netflxi'),
('Netflix', 'Nteflix'),
('Netflix', 'Netflx'),
('Netflix', 'netflix'),
('Netflix', 'Netlfix'),
('Netflix', 'Entflix'),
('Netflix', 'Neftlix'),

('Overdraft Fee', 'overdraft fee'),
('Overdraft Fee', 'OverdraftFee'),
('Overdraft Fee', 'Overdarft Fee'),
('Overdraft Fee', 'overdraft  fee'),
('Overdraft Fee', 'ovrdraft fee'),
('Overdraft Fee', 'Overdraft Efe'),
('Overdraft Fee', 'Voerdraft Fee'),

('PAYROLL LTD', 'payroll ltd'),
('PAYROLL LTD', 'Payroll Ltd'),
('PAYROLL LTD', 'payrol ltd'),
('PAYROLL LTD', 'payro ll ltd'),
('PAYROLL LTD', 'pyaroll ltd'),
('PAYROLL LTD', 'Apyroll Ltd'),
('PAYROLL LTD', 'Paryoll Ltd'),
('PAYROLL LTD', 'Payorll Ltd'),
('PAYROLL LTD', 'Payroll Ldt'),
('PAYROLL LTD', 'Payroll Tld'),

('Pharmacy', 'pharmacy'),
('Pharmacy', 'Phamracy'),
('Pharmacy', 'Pharmcy'),
('Pharmacy', 'Pharmacy '),
('Pharmacy', 'phrarmacy'),
('Pharmacy', 'Pahrmacy'),

('Shell Fuel', 'Shell Fuel'),
('Shell Fuel', 'shell fuel'),
('Shell Fuel', 'ShellFuel'),
('Shell Fuel', 'She ll Fuel'),
('Shell Fuel', 'Shello Fuel'),
('Shell Fuel', 'Sehll Fuel'),
('Shell Fuel', 'Shell Feul'),

('Starbucks', 'starbucks'),
('Starbucks', 'Starbuks'),
('Starbucks', 'Starbuck'),
('Starbucks', 'Strabucks'),
('Starbucks', 'Starb ucks'),
('Starbucks', 'Satrbucks'),
('Starbucks', 'Stabrucks'),
('Starbucks', 'Starubcks'),
('Starbucks', 'Tsarbucks'),

('Target', 'Targte'),
('Target', 'Target '),
('Target', 'Traget'),
('Target', 'tagret'),
('Target', 'Tar get'),
('Target', 'Atrget'),


('Tesco', 'Tecso'),
('Tesco', 'Tes co'),
('Tesco', 'Tesco'),
('Tesco', 'Tseco'),
('Tesco', 'Tescco'),
('Tesco', 'Etsco'),
('Tesco', 'Tesoc'),

('Trainline', 'Trailnine'),
('Trainline', 'Trainlne'),
('Trainline', 'Train line'),
('Trainline', 'trainline'),
('Trainline', 'Trianline'),
('Trainline', 'Traniline'),

('Uber', 'Ubre'),
('Uber', 'uber'),
('Uber', 'Uber '),
('Uber', 'Ub er'),
('Uber', 'Uebr'),
('Uber', 'Buer'),

('Verizon', 'verizon'),
('Verizon', 'Verzon'),
('Verizon', 'Verrizon'),
('Verizon', 'Verion'),
('Verizon', 'Verizn'),
('Verizon', 'Evrizon'),
('Verizon', 'Vreizon'),

('Zara', 'zara'),
('Zara', 'Zra'),
('Zara', 'Za ra'),
('Zara', 'Zarra'),
('Zara', 'Zar a'),
('Zara', 'Zaar');


CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;

-- Apply corrections
UPDATE Staging_Table t
SET merchant_name = d.clean_name
FROM merchant_dictionary d
WHERE levenshtein(LOWER(t.merchant_name), LOWER(d.dirty_name)) <= 1
  AND ABS(LENGTH(t.merchant_name) - LENGTH(d.dirty_name)) <= 2;

---------------------------------------------
-- 7. DUPLICATE HANDLING WITH WINDOW FUNCTION
---------------------------------------------

WITH duplicate_cte AS (
SELECT *,ROW_NUMBER()
OVER (PARTITION BY
AccountId,TransactionId,TransactionId_OBIE,BookingDateTime,ValueDateTime,
RawDateString,InstructedAmount,TransactionAmount,Currency,OriginalCurrency,
ExchangeRate,TransactionType,MerchantCategoryCode,BankTransactionCode,
ProprietaryBankTransactionCode,Reference,Balance,CounterpartyName,
CounterpartyAccount,Status
ORDER BY transactionid) AS row_num
FROM Staging_Table)

SELECT * FROM duplicate_cte WHERE row_num>1;
SELECT * FROM Staging_Table;

--- We have identified 50 duplicates
--- We can create a new table that only coatains unique rows

DROP TABLE IF EXISTS Staging_Table2;
CREATE TABLE Staging_Table2(
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

Insert into Staging_Table2 
SELECT *,ROW_NUMBER()
OVER (PARTITION BY
AccountId,TransactionId,TransactionId_OBIE,BookingDateTime,ValueDateTime,
RawDateString,InstructedAmount,TransactionAmount,Currency,OriginalCurrency,
ExchangeRate,TransactionType,MerchantCategoryCode,BankTransactionCode,
ProprietaryBankTransactionCode,Reference,Balance,CounterpartyName,
CounterpartyAccount,Status
ORDER BY transactionid) AS row_num
FROM Staging_Table;

SELECT count(*) FROM Staging_Table2;

-- Delete the second instance of duplicates and check they are deleted.
DELETE FROM Staging_Table2 WHERE row_num > 1;
SELECT * FROM Staging_Table2 WHERE row_num > 1;

-- Delete the row_num column from table
ALTER TABLE Staging_Table2
DROP COLUMN row_num;

Select * FROM Staging_Table2;

---------------------------------------------
-- 8. REMOVE REDUNDANT COLUMNS
---------------------------------------------
ALTER TABLE Staging_Table2
DROP COLUMN RawDateString;

---------------------------------------------
-- 9. NULL HANDLING
---------------------------------------------
UPDATE Staging_Table2 
SET transactionamount = COALESCE(transactionamount, instructedamount);

---------------------------------------------
-- 10. CATEGORY FIXES FOR NEGATIVE INCOME
---------------------------------------------
UPDATE Staging_Table2
SET category = CASE 
    WHEN instructedamount < 0 AND category = 'Income' THEN 'Income Reversal'
    ELSE category
END;

---------------------------------------------
-- 11. VALIDATION CONSTRAINTS ON CLEANED DATA
---------------------------------------------
-- Primary key on TransactionId (assuming uniqueness after de-duplication)
ALTER TABLE Staging_Table2
ADD CONSTRAINT pk PRIMARY KEY (TransactionId);

-- Date checks: must not be on/after 1st Jan 2026
ALTER TABLE Staging_Table2
ADD CONSTRAINT chk_valid_booking_date 
CHECK (BookingDateTime < '2026-01-01');

ALTER TABLE Staging_Table2
ADD CONSTRAINT chk_valid_value_date 
CHECK (ValueDateTime < '2026-01-01');

-- Instructed amount should not be zero
ALTER TABLE Staging_Table2
ADD CONSTRAINT chk_nonzero_amount
CHECK (InstructedAmount <> 0);

-- Exchange rate logic: only present when currencies differ
ALTER TABLE Staging_Table2
ADD CONSTRAINT chk_exchange_rate_logic
CHECK (
    (Currency = OriginalCurrency AND ExchangeRate IS NULL)
    OR
    (Currency <> OriginalCurrency AND ExchangeRate IS NOT NULL)
);

---------------------------------------------
-- 12. FINAL VIEW
---------------------------------------------
CREATE VIEW Cleaned_Data AS
SELECT * FROM Staging_Table2;

SELECT * FROM Cleaned_Data;
