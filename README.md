# Cleaning Open Banking Transaction Data in SQL 
# Executive Summary

- Financial transaction data is essential for analytics and insight generation, but raw data is often messy, inconsistent, or incomplete.
- In this project, I cleaned a year's worth of consumer Transaction Data.
- Financial data is legally restricted, so all datasets were generated using the UK Open Banking API standards to simulate realistic transaction behaviour.

This project demonstrates a robust SQL-based pipeline to:
- Clean and standardise merchant names
- Apply rule-based transaction categorisation
- Detect and remove duplicates using window functions
- Validate dates, currencies, and exchange rates
- Enforce logical constraints
- Log all changes for auditability
- Produce a final clean view for BI and reporting

**The result is a high-quality, analysis-ready dataset suitable for dashboards, spend insights & anomaly detection.**

# Problem
Financial transaction data is valuable but often unreliable in raw form due to:
- Manual input errors
- Incorrect categories or merchant names
- Missing or inconsistent amounts
- Duplicate transactions
- Inconsistent date formats
- Currency/exchange rate mismatches

Without proper cleaning, downstream analytics become inaccurate.
This project shows how raw transactions are converted into trustworthy, structured data.

# Methodology
### Data Source
- Real bank data cannot be used for compliance reasons.
- Therefore, this dataset was synthetically generated in Python using: 🔗 UK Open Banking API Standards (https://openbankinguk.github.io/read-write-api-site3/v4.0/resources-and-data-models/aisp/Transactions.html)
- The generated data reflects:
  - Merchant details / MCC codes
  - Booked/value dates
  - Currencies & exchange rates
  - Transaction types
  - Category labels
  - Customer spending behaviour

## Cleaning Workflow (SQL-Driven)
1. Key Steps
  - Generate random data in Python
  - Load raw CSV into PostgreSQL
  - Create a staging table
  - Transform data
  - Introduce the final cleaned table and view
2. Data Cleaning Best Practices Applied
  - Trim whitespace
  - Standardise string casing (INITCAP)
  - Fix merchant names using:
      - Dictionary mapping
      - Fuzzy matching (Levenshtein + length-distance check)
  - Normalise categories using rule-based text matching
  - Handle missing values with COALESCE
  - Deduplicate transactions using window functions
  - Validate:
      - No dates ≥ 1 Jan 2026
      - Currency codes follow ISO format
      - Exchange rate logic (rate only when currencies differ)
      - Enforce constraints so future loads remain clean
3. Change Logging
  - All significant corrections are written to a cleaning log table, including:
      - Merchant name fixes
      - Category corrections
      - Duplicate removals
      - Missing value repairs
      - Logical validation flags
  
# Skills Demonstrated
- SQL (CTEs, Views, Window Functions)
- Python (Data generation, Open Banking API modelling)
- Data Engineering best practices
- Database Management

# Business Uses & Next Steps
### Business Uses
- Customer spend insights
- Budgeting & financial planning
- Merchant analytics
### Next Steps
- Connecting to live Open Banking APIs
- Enhancing the merchant dictionary and fuzzy matching rules
- Enriching data (customer profiles, subscription detection)
