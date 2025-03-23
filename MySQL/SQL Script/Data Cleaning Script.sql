-- Data Cleaning

CREATE DATABASE world_layoffs;

-- 1.Remove Duplicates
-- 2. Standardize the Data
-- 3. Null or Blankd Values
-- 4. Remove any columns

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs;

SELECT  *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;


WITH duplicate_cte AS
(
	SELECT *,
    ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, country, funds_raised_millions) AS row_num
    FROM layoffs_staging
)

SELECT *
FROM duplicate_cte
WHERE row_num > 1 ;

SELECT  *
FROM layoffs_staging;




CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


INSERT INTO layoffs_staging2
SELECT *,
    ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, country, funds_raised_millions) AS row_num
    FROM layoffs_staging;


SELECT  *
FROM layoffs_staging2;


DELETE
FROM layoffs_staging2
WHERE row_num > 1 ;


SELECT *
FROM layoffs_staging2
WHERE row_num > 1 ;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


-- Standardize Data

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT(industry)
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';	


SELECT *
FROM layoffs_staging2;


SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;


SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;


UPDATE layoffs_staging2
SET country = 'United States'
WHERE country LIKE 'United States%';


SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2
;


UPDATE layoffs_staging2
SET `date` =  STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT *
FROM layoffs_staging2;


ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Null And Blanks

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = ''
;



SELECT *
FROM layoffs_staging2
WHERE company =  'Airbnb'
AND location = 'SF Bay Area';


UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';



SELECT *
FROM layoffs_staging2
WHERE company LIKE  'Bally%'
;


UPDATE layoffs_staging2
SET industry = 'Entertainment Providers'
WHERE company LIKE 'Bally%';

SELECT *
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;



SELECT * FROM layoffs_staging2  
INTO OUTFILE 'C:/Users/singh/sql_practice/layoffs_data.csv'  
FIELDS TERMINATED BY ','  
ENCLOSED BY '"'  
LINES TERMINATED BY '\n';


SHOW VARIABLES LIKE 'secure_file_priv';

SET GLOBAL local_infile = 1;


