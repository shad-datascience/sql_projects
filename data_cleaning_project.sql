-- DATA CLEANING PROJECT 

SELECT * FROM layoffs;

-- STEPS:
-- 1 Remove Duplicates
-- 2 Standardize the Data
-- 3 NULL Values or Blank Values
-- 4 Remove Any (unnecessary) Columnns or Rows

-- creating a stagging databse
CREATE TABLE layoffs_stagging 
LIKE layoffs;

INSERT layoffs_stagging
SELECT * FROM layoffs;

SELECT * FROM layoffs_stagging;
-- WE will be working with the stagging databse and will not alter the original raw data:
-- 1. Removing Duplicates:
-- adding row number:
SELECT *,
ROW_NUMBER() OVER
( PARTITION BY company,location, industry, total_laid_off, percentage_laid_off, `date`,stage, country, funds_raised_millions) AS row_num
FROM layoffs_stagging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER
( PARTITION BY company,location, industry, total_laid_off, percentage_laid_off, `date`,stage, country, funds_raised_millions) AS row_num
FROM layoffs_stagging
)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1;

SELECT * FROM layoffs_stagging WHERE company = 'Casper'; -- seeing if the queerry works well
-- there is no direct way to delete the duplicates with row number>1 using a CTE, therefore we need to create a
-- stagging datanbase , this feature is available in microsoft sql server and others.
CREATE TABLE layoff_stagging2(
company TEXT,
location TEXT,
industry TEXT,
total_laid_off INT DEFAULT NULL,
percentage_laid_off text,
`date` text,
stage text,
country text,
funds_raised_millions INT DEFAULT NULL,
row_num INT
);

SELECT * FROM layoff_stagging2;
INSERT layoff_stagging2(
SELECT *,
ROW_NUMBER() OVER
( PARTITION BY company,location, industry, total_laid_off, percentage_laid_off, `date`,stage, country, funds_raised_millions) AS row_num
FROM layoffs_stagging
);

SELECT * FROM layoff_stagging2 WHERE row_num>1;

DELETE FROM layoff_stagging2 WHERE row_num>1;

SELECT * FROM layoff_stagging2;

-- 2. Standardising Data:
SELECT company, TRIM(company) -- TRIM removes whitespaces from the ends
FROM layoff_stagging2;

UPDATE layoff_stagging2
SET company = TRIM(company);

SELECT DISTINCT(industry) FROM layoff_stagging2 ORDER BY 1;

SELECT * FROM layoff_stagging2 WHERE industry LIKE 'Crypto%'; 

UPDATE layoff_stagging2 SET
industry = 'Crypto'
WHERE industry LIKE 'Crypto%';
 
 SELECT DISTINCT industry FROM layoff_stagging2 ORDER BY 1;
 
 SELECT DISTINCT location FROM layoff_stagging2 ORDER BY 1;
 
 SELECT DISTINCT country FROM layoff_stagging2 ORDER BY 1;
 -- WE HAVE United States as well as United States. so we need to fix this
 SELECT DISTINCT country, TRIM(TRAILING '.' FROM country) -- some advanced stuff :)
 FROM layoff_stagging2
 ORDER BY 1;
 
 UPDATE layoff_stagging2 
 SET country = TRIM(TRAILING '.' FROM country)
 WHERE country LIKE 'United States%';
 
 SELECT * FROM layoffs_stagging2;
 
 SELECT `date`, STR_TO_DATE(`date`, '%m/%d/%Y')
 FROM layoff_stagging2;
 
 UPDATE layoff_stagging2
 SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');
 
 SELECT `date` FROM layoff_stagging2;
 
-- Since the date still is a text so changing it to date type now, it can be done now because it has proper formatting
ALTER TABLE  layoff_stagging2
MODIFY COLUMN `date` DATE;

SELECT * FROM layoff_stagging2 WHERE 
total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- 3. Handling NULL or BLANK values
-- we will populate the missing inudtry value on the basis of other industry value given for the same company
SELECT * FROM layoff_stagging2
WHERE industry IS NULL OR
industry = '';

SELECT * FROM layoff_stagging2
WHERE company ='Airbnb';


SELECT t1.company,t1.industry,t2.industry
FROM layoff_stagging2 t1 JOIN layoff_stagging2 t2
WHERE t1.company = t2.company
AND (t1.industry IS NULL OR t1.industry='')
AND t2.industry IS NOT NULL;
 
UPDATE layoff_stagging2
SET industry = NULL WHERE
industry = '';

UPDATE layoff_stagging2 t1 JOIN layoff_stagging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL and t2.industry IS NOT NULL;

SELECT * FROM layoff_stagging2 WHERE company LIKE 'Ball%';

-- 4. Deleting unnecessary rows:
-- Deleting the rows where total_laid_off and percentage_laid_off are NULL

SELECT  * FROM layoff_stagging2 WHERE 
total_laid_off IS NULL AND percentage_laid_off IS NULL; 

DELETE  FROM layoff_stagging2 WHERE 
total_laid_off IS NULL AND percentage_laid_off IS NULL;

SELECT * FROM layoff_stagging2;

-- Deleting the row_num column:
ALTER TABLE layoff_stagging2
DROP column row_num;