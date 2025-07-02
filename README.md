# 🧹 SQL Data Cleaning Project – Layoffs Dataset

This repository contains a complete SQL-based data cleaning pipeline applied to a real-world dataset on corporate layoffs. The goal of the project is to transform raw, messy tabular data into a clean, structured format ready for analysis or reporting.

---

## 📁 Repository Structure

```
sql-data-cleaning-project/
├── README.md                  # Project documentation
├── layoffs.csv                # Raw dataset containing company layoff information
└── data_cleaning_project.sql  # SQL script for the entire data cleaning process
```

---

## 📌 Project Overview

The dataset `layoffs.csv` includes information about layoffs across various companies and industries, including:
- Company Name
- Location
- Industry
- Total Laid Off
- Percentage Laid Off
- Date
- Company Stage
- Country
- Funds Raised (in millions)

This project performs a structured cleaning process using SQL, targeting real-world data quality issues such as:
- Duplicate rows
- Inconsistent formatting
- Missing or blank values
- Non-standard date formats
- Unnecessary data

---

## 🧰 Technologies Used

- **Database:** MySQL / SQL-compatible RDBMS
- **Language:** SQL (Standard + MySQL Functions)
- **Environment:** Any SQL engine that supports CTEs, window functions, and date/string manipulation

---

## 🧹 Data Cleaning Steps

### 1. 🗃️ Create a Staging Table
To preserve the integrity of the original dataset, all transformations are done in a **staging table**:
```sql
CREATE TABLE layoffs_stagging LIKE layoffs;
INSERT INTO layoffs_stagging SELECT * FROM layoffs;
```

---

### 2. 🔁 Remove Duplicates
- Applied `ROW_NUMBER()` using a `PARTITION BY` clause to detect duplicates based on key fields.
- Deleted rows with `row_num > 1`.

---

### 3. 🧽 Standardize Text Data
- Removed leading/trailing whitespaces from columns like `company` and `country`.
- Unified inconsistent industry entries (e.g., `"CryptoCurrency"` → `"Crypto"`).
- Removed trailing punctuation (e.g., `"United States."` → `"United States"`).

---

### 4. 🗓️ Standardize Date Format
- Converted date from text (e.g., `'1/12/2023'`) to proper SQL `DATE` format using:
```sql
STR_TO_DATE(date, '%m/%d/%Y')
```
- Updated the column type to `DATE`.

---

### 5. 🔍 Handle NULLs and Blank Values
- Replaced blank entries in the `industry` column with `NULL`.
- Used self-joins to fill in missing `industry` values based on other entries from the same company.
- Deleted rows where both `total_laid_off` and `percentage_laid_off` were `NULL`, indicating non-informative records.

---

### 6. 🧹 Final Cleanup
- Dropped the helper column `row_num` used for deduplication.
- Verified the final structure with cleaned records.

---

## ✅ Final Output

The final cleaned dataset (`layoff_stagging2`) is:
- Duplicate-free
- Properly formatted
- Filled with relevant values wherever possible
- Stripped of meaningless records

Ready for:
- Visualization
- Statistical analysis
- Predictive modeling

---

## 📈 Potential Use Cases

- Layoff trend analysis by industry or country
- Funding vs layoff correlation studies
- Time-series modeling of economic cycles

---

## 💡 Key SQL Concepts Demonstrated

- `ROW_NUMBER()` and window functions
- `TRIM()`, `STR_TO_DATE()`, and `TRAILING` functions
- `JOIN` for column imputation
- CTEs (`WITH` clause) for temporary result sets
- Schema design using staging layers

---

## 📬 Contact

Feel free to connect if you have questions or suggestions:

📧 Email: shad.datascience@gmail.com  
🔗 LinkedIn: [Your LinkedIn Profile](https://www.linkedin.com/in/shadjamil)

---

## ⭐ Acknowledgements

- Dataset inspired by publicly available layoff datasets from tech news sources and job analytics.
- SQL logic structured to reflect real-world ETL patterns.

---

### 🙌 Show some ❤️ by starring this repo if you found it useful!
