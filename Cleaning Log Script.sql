/**********************************************************************************************
    DATA CLEANING CHANGE LOG GENERATOR
    This script compares:
        - Raw data (Transaction_Data)
        - First-stage cleaned data (Staging_Table)
        - Final cleaned table (Staging_Table2)

    It produces:
        1. A master audit log of all changes
        2. A summary table showing counts of each type of correction
        3. A report for presentation
**********************************************************************************************/

---------------------------------------------
-- 0. DROP PREVIOUS LOG TABLES
---------------------------------------------
DROP VIEW IF EXISTS Cleaning_Report;
DROP TABLE IF EXISTS cleaning_audit_log;
DROP TABLE IF EXISTS cleaning_summary;

---------------------------------------------
-- 1. CREATE AUDIT LOG TABLE
---------------------------------------------
CREATE TABLE cleaning_audit_log (
    log_id BIGSERIAL PRIMARY KEY,
    transactionid VARCHAR(20),
    column_changed VARCHAR(50),
    old_value TEXT,
    new_value TEXT,
    change_stage VARCHAR(50),
    change_reason VARCHAR(255),
    changed_at TIMESTAMP DEFAULT NOW()
);

---------------------------------------------
-- 2. MERCHANT NAME CHANGES
---------------------------------------------
INSERT INTO cleaning_audit_log
(transactionid, column_changed, old_value, new_value, change_stage, change_reason)
SELECT 
    t_raw.TransactionId,
    'merchant_name',
    t_raw.merchant_name AS old_value,
    t_stage.merchant_name AS new_value,
    'Merchant Standardisation' AS change_stage,
    'Trim, initcap, dictionary match or fuzzy match' AS change_reason
FROM Transaction_Data t_raw
JOIN Staging_Table t_stage
    ON t_raw.TransactionId = t_stage.TransactionId
WHERE t_raw.merchant_name IS DISTINCT FROM t_stage.merchant_name;

---------------------------------------------
-- 3. CATEGORY CHANGES
---------------------------------------------
INSERT INTO cleaning_audit_log
(transactionid, column_changed, old_value, new_value, change_stage, change_reason)
SELECT 
    t_raw.TransactionId,
    'category',
    t_raw.category,
    t_stage.category,
    'Category Normalisation',
    'Keyword-based standardisation rules'
FROM Transaction_Data t_raw
JOIN Staging_Table t_stage
    ON t_raw.TransactionId = t_stage.TransactionId
WHERE t_raw.category IS DISTINCT FROM t_stage.category;

---------------------------------------------
-- 4. NULL FIXES (transactionamount replaced with instructedamount)
---------------------------------------------
INSERT INTO cleaning_audit_log
(transactionid, column_changed, old_value, new_value, change_stage, change_reason)
SELECT 
    t_stage.TransactionId,
    'transactionamount',
    t_stage.transactionamount AS old_value,
    t_final.transactionamount AS new_value,
    'Missing Value Fix',
    'Filled NULL using instructedamount'
FROM Staging_Table t_stage
JOIN Staging_Table2 t_final
    ON t_stage.TransactionId = t_final.TransactionId
WHERE t_stage.transactionamount IS NULL
  AND t_final.transactionamount IS NOT NULL;

---------------------------------------------
-- 5. DUPLICATE REMOVAL LOGGING
---------------------------------------------
INSERT INTO cleaning_audit_log
(transactionid, column_changed, old_value, new_value, change_stage, change_reason)
SELECT
    d.TransactionId,
    'duplicate_row',
    'Duplicate identified',
    'Deleted',
    'Duplicate Removal',
    'Window function (ROW_NUMBER partition) removed duplicate'
FROM (
    SELECT *, 
           ROW_NUMBER() OVER (
               PARTITION BY
                    AccountId, TransactionId, TransactionId_OBIE, BookingDateTime, ValueDateTime,
                    RawDateString, InstructedAmount, TransactionAmount, Currency, OriginalCurrency,
                    ExchangeRate, TransactionType, MerchantCategoryCode, BankTransactionCode,
                    ProprietaryBankTransactionCode, Reference, Balance, CounterpartyName,
                    CounterpartyAccount,Status
               ORDER BY TransactionId
           ) AS rn
    FROM Staging_Table
) d
WHERE d.rn > 1;

---------------------------------------------
-- 6. EXCHANGE RATE LOG ISSUES (logical fixes)
---------------------------------------------
INSERT INTO cleaning_audit_log
(transactionid, column_changed, old_value, new_value, change_stage, change_reason)
SELECT
    t.TransactionId,
    'exchangerate',
    NULL,
    t.exchangerate,
    'Logical Constraint Fix',
    'Enforced rule: exchange rate only allowed when currency != originalcurrency'
FROM Staging_Table2 t
WHERE (currency = originalcurrency AND exchangerate IS NULL)
   OR (currency <> originalcurrency AND exchangerate IS NOT NULL);

---------------------------------------------
-- 7. DATE VALIDATION LOGGING
---------------------------------------------
INSERT INTO cleaning_audit_log
(transactionid, column_changed, old_value, new_value, change_stage, change_reason)
SELECT 
    t.TransactionId,
    'BookingDateTime',
    t.BookingDateTime::TEXT,
    NULL,
    'Date Validation',
    'Removed or flagged invalid future dates'
FROM Staging_Table2 t
WHERE BookingDateTime >= '2026-01-01';

---------------------------------------------
-- 8. SUMMARY TABLE
---------------------------------------------
CREATE TABLE cleaning_summary AS
SELECT 
    column_changed,
    change_stage,
    change_reason,
    COUNT(*) AS number_of_changes
FROM cleaning_audit_log
GROUP BY 1,2,3
ORDER BY number_of_changes DESC;

---------------------------------------------
-- 9. VIEW FOR PRESENTATION
---------------------------------------------
CREATE VIEW Cleaning_Report AS
SELECT
    change_stage AS "Cleaning Stage",
    change_reason AS "Reason For Change",
    number_of_changes AS "Total Rows Changed"
FROM cleaning_summary
ORDER BY "Total Rows Changed" DESC;

---------------------------------------------
-- 10. OUTPUT
---------------------------------------------
SELECT * FROM Cleaning_Report;