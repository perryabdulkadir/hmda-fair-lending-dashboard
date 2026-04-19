-- Just need a RAW schema to land the data
USE DATABASE BADGER_DB;

CREATE OR REPLACE SCHEMA BADGER_DB.RAW;

-- From https://ffiec.cfpb.gov/data-publication/snapshot-national-loan-level-dataset/2024: Loan/Application Records (LAR)
CREATE OR REPLACE TABLE BADGER_DB.RAW.HMDA_LAR_2024 (
    ACTIVITY_YEAR                            VARCHAR(4),
    LEI                                      VARCHAR(25),
    DERIVED_MSA_MD                           VARCHAR(10),
    STATE_CODE                               VARCHAR(2),
    COUNTY_CODE                              VARCHAR(5),
    CENSUS_TRACT                             VARCHAR(12),
    CONFORMING_LOAN_LIMIT                    VARCHAR(10),
    DERIVED_LOAN_PRODUCT_TYPE                VARCHAR(60),
    DERIVED_DWELLING_CATEGORY                VARCHAR(60),
    DERIVED_ETHNICITY                        VARCHAR(60),
    DERIVED_RACE                             VARCHAR(60),
    DERIVED_SEX                              VARCHAR(30),
    ACTION_TAKEN                             VARCHAR(2),
    PURCHASER_TYPE                           VARCHAR(2),
    PREAPPROVAL                              VARCHAR(2),
    LOAN_TYPE                                VARCHAR(2),
    LOAN_PURPOSE                             VARCHAR(2),
    LIEN_STATUS                              VARCHAR(2),
    REVERSE_MORTGAGE                         VARCHAR(2),
    OPEN_END_LINE_OF_CREDIT                  VARCHAR(2),
    BUSINESS_OR_COMMERCIAL_PURPOSE           VARCHAR(2),
    LOAN_AMOUNT                              VARCHAR(20),
    LOAN_TO_VALUE_RATIO                      VARCHAR(20),
    INTEREST_RATE                            VARCHAR(20),
    RATE_SPREAD                              VARCHAR(20),
    HOEPA_STATUS                             VARCHAR(2),
    TOTAL_LOAN_COSTS                         VARCHAR(20),
    TOTAL_POINTS_AND_FEES                    VARCHAR(20),
    ORIGINATION_CHARGES                      VARCHAR(20),
    DISCOUNT_POINTS                          VARCHAR(20),
    LENDER_CREDITS                           VARCHAR(20),
    LOAN_TERM                                VARCHAR(10),
    PREPAYMENT_PENALTY_TERM                  VARCHAR(10),
    INTRO_RATE_PERIOD                        VARCHAR(10),
    NEGATIVE_AMORTIZATION                    VARCHAR(2),
    INTEREST_ONLY_PAYMENT                    VARCHAR(2),
    BALLOON_PAYMENT                          VARCHAR(2),
    OTHER_NONAMORTIZING_FEATURES             VARCHAR(2),
    PROPERTY_VALUE                           VARCHAR(20),
    CONSTRUCTION_METHOD                      VARCHAR(2),
    OCCUPANCY_TYPE                           VARCHAR(2),
    MANUFACTURED_HOME_SECURED_PROPERTY_TYPE  VARCHAR(2),
    MANUFACTURED_HOME_LAND_PROPERTY_INTEREST VARCHAR(2),
    TOTAL_UNITS                              VARCHAR(5),
    MULTIFAMILY_AFFORDABLE_UNITS             VARCHAR(10),
    INCOME                                   VARCHAR(20),
    DEBT_TO_INCOME_RATIO                     VARCHAR(20),
    APPLICANT_CREDIT_SCORE_TYPE              VARCHAR(4),
    CO_APPLICANT_CREDIT_SCORE_TYPE           VARCHAR(4),
    APPLICANT_ETHNICITY_1                    VARCHAR(4),
    APPLICANT_ETHNICITY_2                    VARCHAR(4),
    APPLICANT_ETHNICITY_3                    VARCHAR(4),
    APPLICANT_ETHNICITY_4                    VARCHAR(4),
    APPLICANT_ETHNICITY_5                    VARCHAR(4),
    CO_APPLICANT_ETHNICITY_1                 VARCHAR(4),
    CO_APPLICANT_ETHNICITY_2                 VARCHAR(4),
    CO_APPLICANT_ETHNICITY_3                 VARCHAR(4),
    CO_APPLICANT_ETHNICITY_4                 VARCHAR(4),
    CO_APPLICANT_ETHNICITY_5                 VARCHAR(4),
    APPLICANT_ETHNICITY_OBSERVED             VARCHAR(2),
    CO_APPLICANT_ETHNICITY_OBSERVED          VARCHAR(2),
    APPLICANT_RACE_1                         VARCHAR(4),
    APPLICANT_RACE_2                         VARCHAR(4),
    APPLICANT_RACE_3                         VARCHAR(4),
    APPLICANT_RACE_4                         VARCHAR(4),
    APPLICANT_RACE_5                         VARCHAR(4),
    CO_APPLICANT_RACE_1                      VARCHAR(4),
    CO_APPLICANT_RACE_2                      VARCHAR(4),
    CO_APPLICANT_RACE_3                      VARCHAR(4),
    CO_APPLICANT_RACE_4                      VARCHAR(4),
    CO_APPLICANT_RACE_5                      VARCHAR(4),
    APPLICANT_RACE_OBSERVED                  VARCHAR(2),
    CO_APPLICANT_RACE_OBSERVED               VARCHAR(2),
    APPLICANT_SEX                            VARCHAR(2),
    CO_APPLICANT_SEX                         VARCHAR(2),
    APPLICANT_SEX_OBSERVED                   VARCHAR(2),
    CO_APPLICANT_SEX_OBSERVED                VARCHAR(2),
    APPLICANT_AGE                            VARCHAR(10),
    CO_APPLICANT_AGE                         VARCHAR(10),
    APPLICANT_AGE_ABOVE_62                   VARCHAR(5),
    CO_APPLICANT_AGE_ABOVE_62                VARCHAR(5),
    SUBMISSION_OF_APPLICATION                VARCHAR(4),
    INITIALLY_PAYABLE_TO_INSTITUTION         VARCHAR(4),
    AUS_1                                    VARCHAR(4),
    AUS_2                                    VARCHAR(4),
    AUS_3                                    VARCHAR(4),
    AUS_4                                    VARCHAR(4),
    AUS_5                                    VARCHAR(4),
    DENIAL_REASON_1                          VARCHAR(4),
    DENIAL_REASON_2                          VARCHAR(4),
    DENIAL_REASON_3                          VARCHAR(4),
    DENIAL_REASON_4                          VARCHAR(4),
    TRACT_POPULATION                         VARCHAR(20),
    TRACT_MINORITY_POPULATION_PERCENT        VARCHAR(20),
    FFIEC_MSA_MD_MEDIAN_FAMILY_INCOME        VARCHAR(20),
    TRACT_TO_MSA_INCOME_PERCENTAGE           VARCHAR(20),
    TRACT_OWNER_OCCUPIED_UNITS               VARCHAR(20),
    TRACT_ONE_TO_FOUR_FAMILY_HOMES           VARCHAR(20),
    TRACT_MEDIAN_AGE_OF_HOUSING_UNITS        VARCHAR(20)
);

-- From https://ffiec.cfpb.gov/data-publication/snapshot-national-loan-level-dataset/2024: Transmittal Sheet Records (TS)
CREATE OR REPLACE TABLE BADGER_DB.RAW.HMDA_TS_2024 (
    ACTIVITY_YEAR      VARCHAR(4),
    CALENDAR_QUARTER   VARCHAR(2),
    LEI                VARCHAR(25),
    TAX_ID             VARCHAR(20),
    AGENCY_CODE        VARCHAR(2),
    RESPONDENT_NAME    VARCHAR(200),
    RESPONDENT_STATE   VARCHAR(2),
    RESPONDENT_CITY    VARCHAR(100),
    RESPONDENT_ZIP     VARCHAR(10),
    LAR_COUNT          VARCHAR(10)
);

-- From https://ffiec.cfpb.gov/data-publication/snapshot-national-loan-level-dataset/2024: MSA/MD Description
CREATE OR REPLACE TABLE BADGER_DB.RAW.HMDA_MSABD_2024 (
    MSA_MD       VARCHAR(10),
    MSA_MD_NAME  VARCHAR(200),
    STATE_NAME   VARCHAR(50)
);

-- Create a stage for loading local files
CREATE OR REPLACE STAGE BADGER_DB.RAW.HMDA_STAGE
    FILE_FORMAT = (
        TYPE = 'CSV'
        FIELD_DELIMITER = ','
        SKIP_HEADER = 1
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'  -- handles quoted fields like "HomeStreet Bank"
        EMPTY_FIELD_AS_NULL = TRUE
        NULL_IF = ('', 'NA', 'Exempt')
    );

// Had to run this once in order to upload data via Snowflake connector instead of UI since UI upload was failing. Bypasses TFA for ten minutes so I could login with just my password. 
-- ALTER USER BADGER SET MINS_TO_BYPASS_MFA = 10;


-- Load LAR (Minnesota)
COPY INTO BADGER_DB.RAW.HMDA_LAR_2024
FROM @BADGER_DB.RAW.HMDA_STAGE/state_MN.csv
ON_ERROR = 'CONTINUE';

-- Load Transmittal Sheet
COPY INTO BADGER_DB.RAW.HMDA_TS_2024
FROM @BADGER_DB.RAW.HMDA_STAGE/2024_public_ts_csv.csv
ON_ERROR = 'CONTINUE';

-- Load MSA/MD descriptions
COPY INTO BADGER_DB.RAW.HMDA_MSABD_2024
FROM @BADGER_DB.RAW.HMDA_STAGE/2024_public_msamd_csv.csv
ON_ERROR = 'CONTINUE';

-- Verify row counts
SELECT 'HMDA_LAR_2024'   AS table_name, COUNT(*) AS row_count FROM BADGER_DB.RAW.HMDA_LAR_2024
UNION ALL
SELECT 'HMDA_TS_2024',                  COUNT(*)              FROM BADGER_DB.RAW.HMDA_TS_2024
UNION ALL
SELECT 'HMDA_MSABD_2024',               COUNT(*)              FROM BADGER_DB.RAW.HMDA_MSABD_2024;


-- ============================================================
-- WORKSHEET SECTION 1: CURATION LAYER
-- ============================================================
-- Schema naming convention: CUR_ prefix for all objects
-- Tag: BadgerProject applied to all tables and views
-- 8 curation steps documented below
-- ============================================================

-- Step 0: Create curation schema and semantic tag
CREATE OR REPLACE SCHEMA BADGER_DB.CUR;

-- Create the semantic tag used to label all project objects
CREATE OR REPLACE TAG BADGER_DB.CUR.BADGER_PROJECT
    COMMENT = 'Tag for all objects created as part of the Badger HMDA final project';

-- ============================================================
-- STEP 1: Filter Exempt rows and null-handle key financial
-- columns. Rows where LTV = 'Exempt' indicate small
-- institutions exempt from full HMDA reporting. These rows
-- have nulls across all financial fields and are not useful
-- for analysis. Also cast numeric columns from VARCHAR.
-- ============================================================
CREATE OR REPLACE TABLE BADGER_DB.CUR.CUR_LAR_2024 AS
SELECT
    -- Keys and identifiers
    ACTIVITY_YEAR,
    LEI,
    DERIVED_MSA_MD,
    STATE_CODE,
    COUNTY_CODE,
    CENSUS_TRACT,

    -- Loan product characteristics (kept as-is from raw)
    CONFORMING_LOAN_LIMIT,
    DERIVED_LOAN_PRODUCT_TYPE,
    DERIVED_DWELLING_CATEGORY,
    LOAN_TYPE,
    LOAN_PURPOSE,
    LIEN_STATUS,
    PREAPPROVAL,
    PURCHASER_TYPE,
    SUBMISSION_OF_APPLICATION,
    INITIALLY_PAYABLE_TO_INSTITUTION,
    HOEPA_STATUS,
    REVERSE_MORTGAGE,
    OPEN_END_LINE_OF_CREDIT,
    BUSINESS_OR_COMMERCIAL_PURPOSE,
    NEGATIVE_AMORTIZATION,
    INTEREST_ONLY_PAYMENT,
    BALLOON_PAYMENT,
    OTHER_NONAMORTIZING_FEATURES,
    CONSTRUCTION_METHOD,
    OCCUPANCY_TYPE,
    MANUFACTURED_HOME_SECURED_PROPERTY_TYPE,
    MANUFACTURED_HOME_LAND_PROPERTY_INTEREST,
    TOTAL_UNITS,
    MULTIFAMILY_AFFORDABLE_UNITS,

    -- AUS fields
    AUS_1, AUS_2, AUS_3, AUS_4, AUS_5,

    -- Demographic fields (raw)
    DERIVED_ETHNICITY,
    DERIVED_RACE,
    DERIVED_SEX,
    APPLICANT_RACE_1, APPLICANT_RACE_2, APPLICANT_RACE_3,
    APPLICANT_RACE_4, APPLICANT_RACE_5,
    CO_APPLICANT_RACE_1, CO_APPLICANT_RACE_2, CO_APPLICANT_RACE_3,
    CO_APPLICANT_RACE_4, CO_APPLICANT_RACE_5,
    APPLICANT_ETHNICITY_1, APPLICANT_ETHNICITY_2, APPLICANT_ETHNICITY_3,
    APPLICANT_ETHNICITY_4, APPLICANT_ETHNICITY_5,
    CO_APPLICANT_ETHNICITY_1, CO_APPLICANT_ETHNICITY_2,
    CO_APPLICANT_ETHNICITY_3, CO_APPLICANT_ETHNICITY_4,
    CO_APPLICANT_ETHNICITY_5,
    APPLICANT_RACE_OBSERVED,    CO_APPLICANT_RACE_OBSERVED,
    APPLICANT_ETHNICITY_OBSERVED, CO_APPLICANT_ETHNICITY_OBSERVED,
    APPLICANT_SEX,              CO_APPLICANT_SEX,
    APPLICANT_SEX_OBSERVED,     CO_APPLICANT_SEX_OBSERVED,
    APPLICANT_AGE,              CO_APPLICANT_AGE,
    APPLICANT_AGE_ABOVE_62,     CO_APPLICANT_AGE_ABOVE_62,
    APPLICANT_CREDIT_SCORE_TYPE, CO_APPLICANT_CREDIT_SCORE_TYPE,

    -- Denial reasons (raw codes kept; labels added below as derived fields)
    DENIAL_REASON_1, DENIAL_REASON_2, DENIAL_REASON_3, DENIAL_REASON_4,

    -- Numeric financial fields cast from VARCHAR
    -- NULL out 'Exempt', 'NA', and empty strings
    TRY_CAST(NULLIF(NULLIF(LOAN_AMOUNT, 'Exempt'), 'NA')           AS FLOAT) AS LOAN_AMOUNT,
    TRY_CAST(NULLIF(NULLIF(LOAN_TO_VALUE_RATIO, 'Exempt'), 'NA')   AS FLOAT) AS LOAN_TO_VALUE_RATIO,
    TRY_CAST(NULLIF(NULLIF(INTEREST_RATE, 'Exempt'), 'NA')         AS FLOAT) AS INTEREST_RATE,
    TRY_CAST(NULLIF(NULLIF(RATE_SPREAD, 'Exempt'), 'NA')           AS FLOAT) AS RATE_SPREAD,
    TRY_CAST(NULLIF(NULLIF(TOTAL_LOAN_COSTS, 'Exempt'), 'NA')      AS FLOAT) AS TOTAL_LOAN_COSTS,
    TRY_CAST(NULLIF(NULLIF(TOTAL_POINTS_AND_FEES, 'Exempt'), 'NA') AS FLOAT) AS TOTAL_POINTS_AND_FEES,
    TRY_CAST(NULLIF(NULLIF(ORIGINATION_CHARGES, 'Exempt'), 'NA')   AS FLOAT) AS ORIGINATION_CHARGES,
    TRY_CAST(NULLIF(NULLIF(DISCOUNT_POINTS, 'Exempt'), 'NA')       AS FLOAT) AS DISCOUNT_POINTS,
    TRY_CAST(NULLIF(NULLIF(LENDER_CREDITS, 'Exempt'), 'NA')        AS FLOAT) AS LENDER_CREDITS,
    TRY_CAST(NULLIF(NULLIF(LOAN_TERM, 'Exempt'), 'NA')             AS FLOAT) AS LOAN_TERM,
    TRY_CAST(NULLIF(NULLIF(PREPAYMENT_PENALTY_TERM,'Exempt'),'NA') AS FLOAT) AS PREPAYMENT_PENALTY_TERM,
    TRY_CAST(NULLIF(NULLIF(INTRO_RATE_PERIOD, 'Exempt'), 'NA')     AS FLOAT) AS INTRO_RATE_PERIOD,
    TRY_CAST(NULLIF(NULLIF(PROPERTY_VALUE, 'Exempt'), 'NA')        AS FLOAT) AS PROPERTY_VALUE,
    TRY_CAST(NULLIF(NULLIF(INCOME, 'Exempt'), 'NA')                AS FLOAT) AS INCOME,

    -- Tract fields cast to numeric
    TRY_CAST(NULLIF(TRACT_POPULATION, 'NA')                         AS FLOAT) AS TRACT_POPULATION,
    TRY_CAST(NULLIF(TRACT_MINORITY_POPULATION_PERCENT, 'NA')        AS FLOAT) AS TRACT_MINORITY_POPULATION_PERCENT,
    TRY_CAST(NULLIF(FFIEC_MSA_MD_MEDIAN_FAMILY_INCOME, 'NA')        AS FLOAT) AS FFIEC_MSA_MD_MEDIAN_FAMILY_INCOME,
    TRY_CAST(NULLIF(TRACT_TO_MSA_INCOME_PERCENTAGE, 'NA')           AS FLOAT) AS TRACT_TO_MSA_INCOME_PERCENTAGE,
    TRY_CAST(NULLIF(TRACT_OWNER_OCCUPIED_UNITS, 'NA')               AS FLOAT) AS TRACT_OWNER_OCCUPIED_UNITS,
    TRY_CAST(NULLIF(TRACT_ONE_TO_FOUR_FAMILY_HOMES, 'NA')           AS FLOAT) AS TRACT_ONE_TO_FOUR_FAMILY_HOMES,
    TRY_CAST(NULLIF(TRACT_MEDIAN_AGE_OF_HOUSING_UNITS, 'NA')        AS FLOAT) AS TRACT_MEDIAN_AGE_OF_HOUSING_UNITS,

    -- ============================================================
    -- STEP 2: Map ACTION_TAKEN codes to human-readable labels
    -- and create APPROVED_FLAG (Y/N indicator).
    -- Codes: 1=Originated, 2=Approved Not Accepted, 3=Denied,
    -- 4=Withdrawn, 5=Incomplete, 6=Purchased, 7=Preapproval Denied,
    -- 8=Preapproval Approved Not Accepted
    -- ============================================================
    ACTION_TAKEN,
    CASE ACTION_TAKEN
        WHEN '1' THEN 'Loan Originated'
        WHEN '2' THEN 'Approved - Not Accepted'
        WHEN '3' THEN 'Application Denied'
        WHEN '4' THEN 'Withdrawn by Applicant'
        WHEN '5' THEN 'File Closed - Incomplete'
        WHEN '6' THEN 'Purchased Loan'
        WHEN '7' THEN 'Preapproval Denied'
        WHEN '8' THEN 'Preapproval Approved - Not Accepted'
        ELSE 'Unknown'
    END AS ACTION_TAKEN_LABEL,

    -- Binary flag: Y = applicant received or was offered a loan
    -- Codes 1, 2, 8 are favorable outcomes; 3 is denial
    -- Excludes 4, 5, 6, 7 as non-underwriting outcomes
    CASE
        WHEN ACTION_TAKEN IN ('1', '2', '8') THEN 'Y'
        WHEN ACTION_TAKEN = '3'              THEN 'N'
        ELSE NULL
    END AS APPROVED_FLAG,

    -- ============================================================
    -- STEP 3: Map DENIAL_REASON_1 codes to labels
    -- Only populated for denied applications (action_taken = 3 or 7)
    -- 1=DTI, 2=Employment, 3=Credit History, 4=Collateral,
    -- 5=Insufficient Cash, 6=Unverifiable Info, 7=Incomplete,
    -- 8=Mortgage Insurance Denied, 9=Other, 10=Not Applicable
    -- ============================================================
    CASE DENIAL_REASON_1
        WHEN '1'  THEN 'Debt-to-Income Ratio'
        WHEN '2'  THEN 'Employment History'
        WHEN '3'  THEN 'Credit History'
        WHEN '4'  THEN 'Collateral'
        WHEN '5'  THEN 'Insufficient Cash'
        WHEN '6'  THEN 'Unverifiable Information'
        WHEN '7'  THEN 'Credit Application Incomplete'
        WHEN '8'  THEN 'Mortgage Insurance Denied'
        WHEN '9'  THEN 'Other'
        WHEN '10' THEN 'Not Applicable'
        ELSE NULL
    END AS DENIAL_REASON_1_LABEL,

    -- ============================================================
    -- STEP 4: Re-bin DEBT_TO_INCOME_RATIO
    -- Raw data mixes broad buckets (<20%, 20%-<30%, 50-60%, >60%)
    -- with individual integers (36, 37, 38 ... 49) for the middle
    -- range. Consolidate into uniform 10-point-wide buckets
    -- matching standard underwriting risk tiers.
    -- ============================================================
    DEBT_TO_INCOME_RATIO,
    CASE
        WHEN DEBT_TO_INCOME_RATIO = '<20%'                        THEN '<20%'
        WHEN DEBT_TO_INCOME_RATIO = '20%-<30%'                   THEN '20%-<30%'
        WHEN DEBT_TO_INCOME_RATIO IN ('30%-<36%','36','37','38','39') THEN '30%-<40%'
        WHEN DEBT_TO_INCOME_RATIO IN ('40','41','42','43','44',
                                       '45','46','47','48','49') THEN '40%-<50%'
        WHEN DEBT_TO_INCOME_RATIO = '50%-60%'                    THEN '50%-60%'
        WHEN DEBT_TO_INCOME_RATIO = '>60%'                       THEN '>60%'
        WHEN DEBT_TO_INCOME_RATIO = 'Exempt'                     THEN 'Exempt'
        ELSE NULL
    END AS DTI_BUCKET,

    -- ============================================================
    -- STEP 5: Higher-priced loan flag
    -- Rate spread is the difference between the loan APR and the
    -- Average Prime Offer Rate (APOR) for a comparable transaction.
    -- HMDA defines a "higher-priced" threshold at rate_spread >= 1.5
    -- for first-lien loans. This flag is a key fair lending signal:
    -- protected class borrowers receiving disproportionately more
    -- higher-priced loans is a classic pricing disparity indicator.
    -- ============================================================
    CASE
        WHEN TRY_CAST(NULLIF(RATE_SPREAD,'NA') AS FLOAT) >= 1.5 THEN 'Y'
        WHEN TRY_CAST(NULLIF(RATE_SPREAD,'NA') AS FLOAT) <  1.5 THEN 'N'
        ELSE NULL
    END AS IS_HIGHER_PRICED_FLAG,

    -- ============================================================
    -- STEP 6: Clean DERIVED_RACE and DERIVED_ETHNICITY
    -- "Free Form Text Only" occurs when applicants write in a race
    -- or ethnicity not in the standard code list. This is a data
    -- quality issue - treat as not available for analysis purposes.
    -- ============================================================
    CASE
        WHEN DERIVED_RACE = 'Free Form Text Only' THEN 'Race Not Available'
        ELSE DERIVED_RACE
    END AS DERIVED_RACE_CLEAN,

    CASE
        WHEN DERIVED_ETHNICITY = 'Free Form Text Only' THEN 'Ethnicity Not Available'
        ELSE DERIVED_ETHNICITY
    END AS DERIVED_ETHNICITY_CLEAN,

    -- ============================================================
    -- STEP 7: Derived age flag
    -- Combines applicant and co-applicant age_above_62 fields into
    -- a single flag. ECOA prohibits age discrimination; this flag
    -- enables fair lending analysis for the age-protected class.
    -- Y = at least one party on the application is over 62.
    -- ============================================================
    CASE
        WHEN APPLICANT_AGE_ABOVE_62    = 'Yes'
          OR CO_APPLICANT_AGE_ABOVE_62 = 'Yes' THEN 'Y'
        ELSE 'N'
    END AS ANY_APPLICANT_ABOVE_62_FLAG,

    -- ============================================================
    -- STEP 8: Map HOEPA_STATUS codes to labels
    -- HOEPA (Home Ownership and Equity Protection Act) status
    -- identifies high-cost mortgages subject to additional
    -- consumer protections. A high-cost flag is analytically
    -- distinct from the rate spread flag above.
    -- 1=High-Cost Mortgage, 2=Not High-Cost, 3=Not Applicable
    -- ============================================================
    CASE HOEPA_STATUS
        WHEN '1' THEN 'High-Cost Mortgage'
        WHEN '2' THEN 'Not a High-Cost Mortgage'
        WHEN '3' THEN 'Not Applicable'
        ELSE NULL
    END AS HOEPA_STATUS_LABEL

FROM BADGER_DB.RAW.HMDA_LAR_2024
WHERE
    -- Exclude Exempt institutions (missing all financial fields)
    LOAN_TO_VALUE_RATIO NOT IN ('Exempt', 'NA', '')
    OR LOAN_TO_VALUE_RATIO IS NULL;

-- Apply tag to CUR_LAR_2024
ALTER TABLE BADGER_DB.CUR.CUR_LAR_2024
    SET TAG BADGER_DB.CUR.BADGER_PROJECT = 'BadgerProject';


-- ============================================================
-- CUR_INSTITUTION_2024
-- Joins unique LEIs from the LAR with the Transmittal Sheet
-- to produce a clean institution reference table.
-- ============================================================
CREATE OR REPLACE TABLE BADGER_DB.CUR.CUR_INSTITUTION_2024 AS
SELECT DISTINCT
    l.LEI,
    t.RESPONDENT_NAME,
    t.RESPONDENT_CITY,
    t.RESPONDENT_STATE,
    t.RESPONDENT_ZIP,
    t.AGENCY_CODE,
    TRY_CAST(t.LAR_COUNT AS INTEGER) AS LAR_COUNT
FROM BADGER_DB.RAW.HMDA_LAR_2024 l
LEFT JOIN BADGER_DB.RAW.HMDA_TS_2024 t
    ON l.LEI = t.LEI;

ALTER TABLE BADGER_DB.CUR.CUR_INSTITUTION_2024
    SET TAG BADGER_DB.CUR.BADGER_PROJECT = 'BadgerProject';


-- ============================================================
-- CUR_GEOGRAPHY_2024
-- Enriches census tract geography with MSA/MD names by joining
-- the LAR to the MSA/MD description lookup table.
-- ============================================================
CREATE OR REPLACE TABLE BADGER_DB.CUR.CUR_GEOGRAPHY_2024 AS
SELECT DISTINCT
    l.CENSUS_TRACT,
    l.STATE_CODE,
    l.COUNTY_CODE,
    l.DERIVED_MSA_MD,
    m.MSA_MD_NAME,
    m.STATE_NAME,
    TRY_CAST(NULLIF(l.TRACT_POPULATION, 'NA')                   AS FLOAT) AS TRACT_POPULATION,
    TRY_CAST(NULLIF(l.TRACT_MINORITY_POPULATION_PERCENT, 'NA')  AS FLOAT) AS TRACT_MINORITY_POPULATION_PERCENT,
    TRY_CAST(NULLIF(l.FFIEC_MSA_MD_MEDIAN_FAMILY_INCOME, 'NA')  AS FLOAT) AS FFIEC_MSA_MD_MEDIAN_FAMILY_INCOME,
    TRY_CAST(NULLIF(l.TRACT_TO_MSA_INCOME_PERCENTAGE, 'NA')     AS FLOAT) AS TRACT_TO_MSA_INCOME_PERCENTAGE,
    TRY_CAST(NULLIF(l.TRACT_OWNER_OCCUPIED_UNITS, 'NA')         AS FLOAT) AS TRACT_OWNER_OCCUPIED_UNITS,
    TRY_CAST(NULLIF(l.TRACT_ONE_TO_FOUR_FAMILY_HOMES, 'NA')     AS FLOAT) AS TRACT_ONE_TO_FOUR_FAMILY_HOMES,
    TRY_CAST(NULLIF(l.TRACT_MEDIAN_AGE_OF_HOUSING_UNITS, 'NA')  AS FLOAT) AS TRACT_MEDIAN_AGE_OF_HOUSING_UNITS
FROM BADGER_DB.RAW.HMDA_LAR_2024 l
LEFT JOIN BADGER_DB.RAW.HMDA_MSABD_2024 m
    ON l.DERIVED_MSA_MD = m.MSA_MD;

ALTER TABLE BADGER_DB.CUR.CUR_GEOGRAPHY_2024
    SET TAG BADGER_DB.CUR.BADGER_PROJECT = 'BadgerProject';


-- ============================================================
-- Sanity checks - run these to verify the curation layer
-- ============================================================
-- Row count
SELECT COUNT(*) FROM BADGER_DB.CUR.CUR_LAR_2024;

-- Verify APPROVED_FLAG distribution
SELECT APPROVED_FLAG, COUNT(*) AS N
FROM BADGER_DB.CUR.CUR_LAR_2024
GROUP BY APPROVED_FLAG
ORDER BY APPROVED_FLAG;

-- Verify DTI_BUCKET consolidation
SELECT DTI_BUCKET, COUNT(*) AS N
FROM BADGER_DB.CUR.CUR_LAR_2024
GROUP BY DTI_BUCKET
ORDER BY N DESC;

-- Verify institution join worked
SELECT COUNT(*), COUNT(RESPONDENT_NAME) AS WITH_NAME
FROM BADGER_DB.CUR.CUR_INSTITUTION_2024;

-- Verify MSA/MD name join worked
SELECT DERIVED_MSA_MD, MSA_MD_NAME, COUNT(*) AS N
FROM BADGER_DB.CUR.CUR_GEOGRAPHY_2024
GROUP BY DERIVED_MSA_MD, MSA_MD_NAME
ORDER BY N DESC
LIMIT 10;

-- ------------------------------------------------------------
-- WORKSHEET SECTION 2: STORED PROCEDURE
-- ------------------------------------------------------------
-- Procedure: PROC_ENRICH_LAR
-- Produces CUR_LAR_ENRICHED_2024 with 2 new transformations:
--
-- Transformation 1: INCOME_TO_AMI_BUCKET
--   HMDA income is stored in thousands of dollars. The FFIEC
--   area median income (AMI) is stored in actual dollars.
--   Computing (income * 1000) / ffiec_ami yields the ratio of
--   applicant income to the area median. This ratio is then
--   bucketed into the standard CRA income tiers used by
--   regulators to assess whether lenders serve low- and
--   moderate-income borrowers.
--
-- Transformation 2: TRACT_MINORITY_CATEGORY
--   Classifies each census tract by its minority population
--   share into three categories used in redlining analysis:
--   Majority-Minority (>=50%), Mixed (20-50%), and
--   Predominantly Non-Minority (<20%). This is a different
--   type of transformation from Worksheet 1 -- it operates
--   on geographic attributes rather than application outcomes
--   or borrower characteristics.
-- -- ------------------------------------------------------------

CREATE OR REPLACE PROCEDURE BADGER_DB.CUR.PROC_ENRICH_LAR()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    CREATE OR REPLACE TABLE BADGER_DB.CUR.CUR_LAR_ENRICHED_2024 AS
    SELECT
        -- Carry forward all columns from the curation layer
        l.*,

        -- ------------------------------------------------
        -- Transformation 1: Income-to-AMI ratio and bucket
        -- Ratio = (applicant income in $) / (FFIEC area median income in $)
        -- Income field is in thousands, so multiply by 1000 first.
        -- Standard CRA income tiers:
        --   Low        = < 50% of AMI
        --   Moderate   = 50% to < 80% of AMI
        --   Middle     = 80% to < 120% of AMI
        --   Upper      = >= 120% of AMI
        -- ------------------------------------------------
        CASE
            WHEN l.INCOME IS NULL
              OR l.FFIEC_MSA_MD_MEDIAN_FAMILY_INCOME IS NULL
              OR l.FFIEC_MSA_MD_MEDIAN_FAMILY_INCOME = 0
            THEN NULL
            ELSE ROUND((l.INCOME * 1000.0) / l.FFIEC_MSA_MD_MEDIAN_FAMILY_INCOME, 4)
        END AS INCOME_TO_AMI_RATIO,

        CASE
            WHEN l.INCOME IS NULL
              OR l.FFIEC_MSA_MD_MEDIAN_FAMILY_INCOME IS NULL
              OR l.FFIEC_MSA_MD_MEDIAN_FAMILY_INCOME = 0
                THEN 'Unknown'
            WHEN (l.INCOME * 1000.0) / l.FFIEC_MSA_MD_MEDIAN_FAMILY_INCOME < 0.50
                THEN 'Low (<50% AMI)'
            WHEN (l.INCOME * 1000.0) / l.FFIEC_MSA_MD_MEDIAN_FAMILY_INCOME < 0.80
                THEN 'Moderate (50-80% AMI)'
            WHEN (l.INCOME * 1000.0) / l.FFIEC_MSA_MD_MEDIAN_FAMILY_INCOME < 1.20
                THEN 'Middle (80-120% AMI)'
            ELSE 'Upper (>120% AMI)'
        END AS INCOME_TO_AMI_BUCKET,

        -- ------------------------------------------------
        -- Transformation 2: Tract minority population category
        -- Uses tract_minority_population_percent to classify
        -- the property's census tract into three tiers used
        -- in redlining and fair lending geographic analysis.
        -- ------------------------------------------------
        CASE
            WHEN l.TRACT_MINORITY_POPULATION_PERCENT IS NULL
                THEN 'Unknown'
            WHEN l.TRACT_MINORITY_POPULATION_PERCENT >= 50
                THEN 'Majority-Minority Tract (>=50%)'
            WHEN l.TRACT_MINORITY_POPULATION_PERCENT >= 20
                THEN 'Mixed Tract (20-50%)'
            ELSE 'Predominantly Non-Minority Tract (<20%)'
        END AS TRACT_MINORITY_CATEGORY

    FROM BADGER_DB.CUR.CUR_LAR_2024 l;

    -- Apply tag to enriched table
    ALTER TABLE BADGER_DB.CUR.CUR_LAR_ENRICHED_2024
        SET TAG BADGER_DB.CUR.BADGER_PROJECT = 'BadgerProject';

    RETURN 'SUCCESS: CUR_LAR_ENRICHED_2024 created with '
        || (SELECT COUNT(*)::VARCHAR FROM BADGER_DB.CUR.CUR_LAR_ENRICHED_2024)
        || ' rows.';
END;
$$;

-- Execute the procedure
CALL BADGER_DB.CUR.PROC_ENRICH_LAR();

-- Verify: spot-check income-to-AMI bucket distribution
SELECT INCOME_TO_AMI_BUCKET, COUNT(*) AS N
FROM BADGER_DB.CUR.CUR_LAR_ENRICHED_2024
GROUP BY INCOME_TO_AMI_BUCKET
ORDER BY N DESC;

-- Verify: tract minority category distribution
SELECT TRACT_MINORITY_CATEGORY, COUNT(*) AS N
FROM BADGER_DB.CUR.CUR_LAR_ENRICHED_2024
GROUP BY TRACT_MINORITY_CATEGORY
ORDER BY N DESC;


---- ------------------------------------------------------------
-- WORKSHEET SECTION 3: AGGREGATION LAYER
---- ------------------------------------------------------------
-- Schema naming convention: AGG_ prefix for all objects
-- All tables query from the enriched curation layer.
-- Aggregation types used: COUNT, AVG, SUM, MIN, MAX
-- Fair lending framing: each table answers a distinct
-- analytical question a journalist or regulator would ask.
-- ============================================================

CREATE OR REPLACE SCHEMA BADGER_DB.AGG;

-- -- ------------------------------------------------------------
-- AGG_APPROVAL_BY_RACE_INSTITUTION
-- Aggregation types: COUNT, SUM, AVG (derived approval rate)
-- Question: For each lender, what is the approval rate for
-- applicants of each race? The foundation for computing AIR
-- (Adverse Impact Ratio) in fair lending analysis.
-- Only includes action_taken IN (1,2,3,8) -- underwriting
-- decisions only; excludes withdrawn/purchased/incomplete.
---- ------------------------------------------------------------
CREATE OR REPLACE TABLE BADGER_DB.AGG.AGG_APPROVAL_BY_RACE_INSTITUTION AS
SELECT
    e.LEI,
    i.RESPONDENT_NAME,
    e.DERIVED_RACE_CLEAN,
    COUNT(*)                                              AS TOTAL_APPLICATIONS,
    SUM(CASE WHEN e.APPROVED_FLAG = 'Y' THEN 1 ELSE 0 END) AS TOTAL_APPROVED,
    SUM(CASE WHEN e.APPROVED_FLAG = 'N' THEN 1 ELSE 0 END) AS TOTAL_DENIED,
    ROUND(
        SUM(CASE WHEN e.APPROVED_FLAG = 'Y' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0) * 100
    , 2)                                                  AS APPROVAL_RATE_PCT
FROM BADGER_DB.CUR.CUR_LAR_ENRICHED_2024 e
LEFT JOIN BADGER_DB.CUR.CUR_INSTITUTION_2024 i
    ON e.LEI = i.LEI
WHERE e.APPROVED_FLAG IS NOT NULL  -- underwriting decisions only
GROUP BY e.LEI, i.RESPONDENT_NAME, e.DERIVED_RACE_CLEAN;

ALTER TABLE BADGER_DB.AGG.AGG_APPROVAL_BY_RACE_INSTITUTION
    SET TAG BADGER_DB.CUR.BADGER_PROJECT = 'BadgerProject';


-- ------------------------------------------------------------
-- AGG_LOAN_METRICS_BY_INSTITUTION
-- Aggregation types: COUNT, AVG, SUM, MIN, MAX
-- Question: How do key loan metrics vary across lenders?
-- Includes average loan size, average rate spread, average
-- LTV, and total origination volume -- useful for identifying
-- outlier institutions for further scrutiny.
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE BADGER_DB.AGG.AGG_LOAN_METRICS_BY_INSTITUTION AS
SELECT
    e.LEI,
    i.RESPONDENT_NAME,
    COUNT(*)                                AS TOTAL_APPLICATIONS,
    SUM(CASE WHEN e.ACTION_TAKEN = '1'
             THEN 1 ELSE 0 END)            AS TOTAL_ORIGINATIONS,
    ROUND(AVG(e.LOAN_AMOUNT), 2)           AS AVG_LOAN_AMOUNT_THOUSANDS,
    ROUND(SUM(e.LOAN_AMOUNT), 2)           AS TOTAL_LOAN_VOLUME_THOUSANDS,
    ROUND(MIN(e.LOAN_AMOUNT), 2)           AS MIN_LOAN_AMOUNT_THOUSANDS,
    ROUND(MAX(e.LOAN_AMOUNT), 2)           AS MAX_LOAN_AMOUNT_THOUSANDS,
    ROUND(AVG(e.RATE_SPREAD), 4)           AS AVG_RATE_SPREAD,
    ROUND(AVG(e.LOAN_TO_VALUE_RATIO), 2)   AS AVG_LTV_RATIO,
    SUM(CASE WHEN e.IS_HIGHER_PRICED_FLAG = 'Y'
             THEN 1 ELSE 0 END)            AS HIGHER_PRICED_COUNT,
    ROUND(
        SUM(CASE WHEN e.IS_HIGHER_PRICED_FLAG = 'Y' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0) * 100
    , 2)                                   AS HIGHER_PRICED_RATE_PCT
FROM BADGER_DB.CUR.CUR_LAR_ENRICHED_2024 e
LEFT JOIN BADGER_DB.CUR.CUR_INSTITUTION_2024 i
    ON e.LEI = i.LEI
WHERE e.ACTION_TAKEN IN ('1','2','3','8')
GROUP BY e.LEI, i.RESPONDENT_NAME;

ALTER TABLE BADGER_DB.AGG.AGG_LOAN_METRICS_BY_INSTITUTION
    SET TAG BADGER_DB.CUR.BADGER_PROJECT = 'BadgerProject';


-- ------------------------------------------------------------
-- AGG_DENIAL_REASONS_BY_RACE
-- Aggregation type: COUNT
-- Question: Do denial reasons differ by race? Disproportionate
-- denial for DTI or collateral reasons among protected classes
-- can indicate steering or inconsistent underwriting.
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE BADGER_DB.AGG.AGG_DENIAL_REASONS_BY_RACE AS
SELECT
    DERIVED_RACE_CLEAN,
    DENIAL_REASON_1_LABEL,
    COUNT(*) AS DENIAL_COUNT,
    ROUND(
        COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY DERIVED_RACE_CLEAN) * 100
    , 2) AS PCT_OF_DENIALS_FOR_RACE
FROM BADGER_DB.CUR.CUR_LAR_ENRICHED_2024
WHERE APPROVED_FLAG = 'N'
  AND DENIAL_REASON_1_LABEL IS NOT NULL
GROUP BY DERIVED_RACE_CLEAN, DENIAL_REASON_1_LABEL;

ALTER TABLE BADGER_DB.AGG.AGG_DENIAL_REASONS_BY_RACE
    SET TAG BADGER_DB.CUR.BADGER_PROJECT = 'BadgerProject';


-- ------------------------------------------------------------
-- AGG_INCOME_AMI_BY_MSA
-- Aggregation types: COUNT, AVG, MIN, MAX
-- Question: How does the income profile of applicants vary
-- across Minnesota metro areas? Combines the AMI bucket
-- derived in the stored procedure with MSA geography.
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE BADGER_DB.AGG.AGG_INCOME_AMI_BY_MSA AS
SELECT
    g.MSA_MD_NAME,
    e.INCOME_TO_AMI_BUCKET,
    COUNT(*)                              AS TOTAL_APPLICATIONS,
    ROUND(AVG(e.INCOME_TO_AMI_RATIO), 4) AS AVG_INCOME_TO_AMI_RATIO,
    ROUND(MIN(e.INCOME), 2)              AS MIN_INCOME_THOUSANDS,
    ROUND(MAX(e.INCOME), 2)              AS MAX_INCOME_THOUSANDS,
    ROUND(AVG(e.INCOME), 2)             AS AVG_INCOME_THOUSANDS,
    SUM(CASE WHEN e.APPROVED_FLAG = 'Y' THEN 1 ELSE 0 END) AS APPROVALS,
    SUM(CASE WHEN e.APPROVED_FLAG = 'N' THEN 1 ELSE 0 END) AS DENIALS
FROM BADGER_DB.CUR.CUR_LAR_ENRICHED_2024 e
LEFT JOIN BADGER_DB.CUR.CUR_GEOGRAPHY_2024 g
    ON e.CENSUS_TRACT = g.CENSUS_TRACT
   AND e.STATE_CODE   = g.STATE_CODE
WHERE e.APPROVED_FLAG IS NOT NULL
GROUP BY g.MSA_MD_NAME, e.INCOME_TO_AMI_BUCKET;

ALTER TABLE BADGER_DB.AGG.AGG_INCOME_AMI_BY_MSA
    SET TAG BADGER_DB.CUR.BADGER_PROJECT = 'BadgerProject';


-- ------------------------------------------------------------
-- STEP 2: Materialized view
-- Filters AGG_APPROVAL_BY_RACE_INSTITUTION to institutions
-- with >= 100 applications per race group for statistical
-- reliability. This is the analysis-ready table a journalist
-- or examiner would actually use to compare lenders.
-- ------------------------------------------------------------
CREATE OR REPLACE MATERIALIZED VIEW BADGER_DB.AGG.AGG_MV_APPROVAL_RATES_RELIABLE AS
SELECT
    LEI,
    RESPONDENT_NAME,
    DERIVED_RACE_CLEAN,
    TOTAL_APPLICATIONS,
    TOTAL_APPROVED,
    TOTAL_DENIED,
    APPROVAL_RATE_PCT
FROM BADGER_DB.AGG.AGG_APPROVAL_BY_RACE_INSTITUTION
WHERE TOTAL_APPLICATIONS >= 100;

ALTER VIEW BADGER_DB.AGG.AGG_MV_APPROVAL_RATES_RELIABLE
    SET TAG BADGER_DB.CUR.BADGER_PROJECT = 'BadgerProject';

-- ------------------------------------------------------------
-- Sanity checks
-- ------------------------------------------------------------
-- Top institutions by application volume
SELECT RESPONDENT_NAME, TOTAL_APPLICATIONS, TOTAL_ORIGINATIONS,
       AVG_LOAN_AMOUNT_THOUSANDS, HIGHER_PRICED_RATE_PCT
FROM BADGER_DB.AGG.AGG_LOAN_METRICS_BY_INSTITUTION
ORDER BY TOTAL_APPLICATIONS DESC
LIMIT 10;

-- Approval rates for White vs Black applicants across all lenders
SELECT DERIVED_RACE_CLEAN,
       SUM(TOTAL_APPLICATIONS) AS APPLICATIONS,
       SUM(TOTAL_APPROVED)     AS APPROVED,
       ROUND(SUM(TOTAL_APPROVED) / NULLIF(SUM(TOTAL_APPLICATIONS),0) * 100, 2) AS OVERALL_APPROVAL_RATE_PCT
FROM BADGER_DB.AGG.AGG_APPROVAL_BY_RACE_INSTITUTION
GROUP BY DERIVED_RACE_CLEAN
ORDER BY APPLICATIONS DESC;

-- Materialized view row count
SELECT COUNT(*) FROM BADGER_DB.AGG.AGG_MV_APPROVAL_RATES_RELIABLE;


-- ------------------------------------------------------------
-- WORKSHEET SECTION 4: TABLE FUNCTION
-- ------------------------------------------------------------
-- Function: FN_APPROVAL_RATES_BY_INSTITUTION
-- Returns approval rate metrics broken down by race for a
-- given institution, identified by partial name match.
--
-- Use case: A Streamlit dashboard calls this function with
-- the institution name selected by the user, and displays
-- the results as a fair lending comparison table.
--
-- Example call:
--   SELECT * FROM TABLE(
--       BADGER_DB.AGG.FN_APPROVAL_RATES_BY_INSTITUTION('US BANK')
--   );
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION BADGER_DB.AGG.FN_APPROVAL_RATES_BY_INSTITUTION(
    INSTITUTION_NAME VARCHAR
)
RETURNS TABLE (
    LEI                  VARCHAR,
    RESPONDENT_NAME      VARCHAR,
    DERIVED_RACE_CLEAN   VARCHAR,
    TOTAL_APPLICATIONS   NUMBER,
    TOTAL_APPROVED       NUMBER,
    TOTAL_DENIED         NUMBER,
    APPROVAL_RATE_PCT    NUMBER(10,2),
    -- AIR vs White baseline: approval rate for this group
    -- divided by the White approval rate for the same institution.
    -- AIR < 0.80 is the standard fair lending threshold for
    -- adverse impact (80% rule).
    AIR_VS_WHITE         NUMBER(10,4)
)
AS
$$
    WITH BASE AS (
        SELECT
            LEI,
            RESPONDENT_NAME,
            DERIVED_RACE_CLEAN,
            TOTAL_APPLICATIONS,
            TOTAL_APPROVED,
            TOTAL_DENIED,
            APPROVAL_RATE_PCT
        FROM BADGER_DB.AGG.AGG_APPROVAL_BY_RACE_INSTITUTION
        WHERE UPPER(RESPONDENT_NAME) LIKE UPPER('%' || INSTITUTION_NAME || '%')
    ),
    WHITE_RATE AS (
        SELECT
            LEI,
            APPROVAL_RATE_PCT AS WHITE_APPROVAL_RATE
        FROM BASE
        WHERE DERIVED_RACE_CLEAN = 'White'
    )
    SELECT
        b.LEI,
        b.RESPONDENT_NAME,
        b.DERIVED_RACE_CLEAN,
        b.TOTAL_APPLICATIONS,
        b.TOTAL_APPROVED,
        b.TOTAL_DENIED,
        b.APPROVAL_RATE_PCT,
        ROUND(b.APPROVAL_RATE_PCT / NULLIF(w.WHITE_APPROVAL_RATE, 0), 4)
            AS AIR_VS_WHITE
    FROM BASE b
    LEFT JOIN WHITE_RATE w
        ON b.LEI = w.LEI
    ORDER BY b.LEI, b.APPROVAL_RATE_PCT DESC
$$;

ALTER FUNCTION BADGER_DB.AGG.FN_APPROVAL_RATES_BY_INSTITUTION(VARCHAR)
    SET TAG BADGER_DB.CUR.BADGER_PROJECT = 'BadgerProject';

-- ------------------------------------------------------------
-- Test the function with a few institutions
-- ------------------------------------------------------------

-- US Bank
SELECT * FROM TABLE(
    BADGER_DB.AGG.FN_APPROVAL_RATES_BY_INSTITUTION('US BANK')
);

-- Rocket Mortgage
SELECT * FROM TABLE(
    BADGER_DB.AGG.FN_APPROVAL_RATES_BY_INSTITUTION('ROCKET')
);

-- Bell Bank
SELECT * FROM TABLE(
    BADGER_DB.AGG.FN_APPROVAL_RATES_BY_INSTITUTION('BELL BANK')
);

-- ------------------------------------------------------------
-- WORKSHEET SECTION 5: TASK
-- ------------------------------------------------------------
-- Task: TASK_ENRICH_LAR_WEEKLY
-- Runs every Sunday at 4:00 AM CT (UTC-5 standard, UTC-6 DST)
-- Executes PROC_ENRICH_LAR() to refresh CUR_LAR_ENRICHED_2024
-- with the latest data from the curation layer.
--
-- NOTE: Task is tested then immediately suspended per
-- assignment instructions. Do not resume without first
-- confirming compute availability.
-- ------------------------------------------------------------

CREATE OR REPLACE TASK BADGER_DB.CUR.TASK_ENRICH_LAR_WEEKLY
    WAREHOUSE = BADGER_WH
    SCHEDULE  = 'USING CRON 0 4 * * 0 America/Chicago'
    COMMENT   = 'Weekly refresh of CUR_LAR_ENRICHED_2024 via PROC_ENRICH_LAR. Runs every Sunday at 4am CT.'
AS
    CALL BADGER_DB.CUR.PROC_ENRICH_LAR();

-- ------------------------------------------------------------
-- Tasks start in SUSPENDED state by default.
-- Must RESUME before testing, then SUSPEND immediately after.
-- ------------------------------------------------------------

-- Step 1: Resume to enable execution
ALTER TASK BADGER_DB.CUR.TASK_ENRICH_LAR_WEEKLY RESUME;

-- Step 2: Manually trigger one run to test
EXECUTE TASK BADGER_DB.CUR.TASK_ENRICH_LAR_WEEKLY;

-- Step 3: Confirm the run succeeded
SELECT
    NAME,
    STATE,
    QUERY_TEXT,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    ERROR_MESSAGE
FROM TABLE(
    BADGER_DB.INFORMATION_SCHEMA.TASK_HISTORY(
        TASK_NAME => 'TASK_ENRICH_LAR_WEEKLY',
        SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP)
    )
)
ORDER BY SCHEDULED_TIME DESC
LIMIT 5;

-- Step 4: Suspend immediately after confirming success
ALTER TASK BADGER_DB.CUR.TASK_ENRICH_LAR_WEEKLY SUSPEND;

-- Step 5: Verify it is suspended
SHOW TASKS LIKE 'TASK_ENRICH_LAR_WEEKLY' IN SCHEMA BADGER_DB.CUR;


-- ============================================================
-- WORKSHEET SECTION 6: PROJECT SUMMARY
-- ============================================================

-- ------------------------------------------------------------
-- DATASET
-- ------------------------------------------------------------
-- Name: 2024 HMDA Loan/Application Register (LAR) - Minnesota
--
-- Source: Federal Financial Institutions Examination Council
-- (FFIEC) / Consumer Financial Protection Bureau (CFPB)
-- URL: https://ffiec.cfpb.gov/data-publication/snapshot-national-loan-level-dataset/2024
--
-- Description: The Home Mortgage Disclosure Act (HMDA) requires
-- nearly every U.S. mortgage lender to publicly disclose
-- loan-level data on every mortgage application they receive.
-- This dataset contains 177,638 mortgage applications filed by
-- financial institutions operating in Minnesota in 2024,
-- covering 99 fields per record including applicant
-- demographics (race, ethnicity, sex, age), loan
-- characteristics (amount, rate spread, LTV, DTI), application
-- outcomes (approved, denied, withdrawn), property information,
-- and census tract geography. Two supplemental files are also
-- loaded: the Transmittal Sheet (TS), which maps Legal Entity
-- Identifiers (LEIs) to institution names, and the MSA/MD
-- Description file, which maps metro area codes to names.
-- The data is used here to support fair lending analysis --
-- specifically, identifying potential disparities in mortgage
-- approval rates and pricing across racial and ethnic groups
-- at individual lending institutions in Minnesota.
--
-- Raw row counts:
--   HMDA_LAR_2024:   177,638 rows (Minnesota applications)
--   HMDA_TS_2024:      4,908 rows (national institution list)
--   HMDA_MSABD_2024:     418 rows (MSA/MD name lookup)

-- ------------------------------------------------------------
-- NAMING CONVENTION AND SCHEMA LOCATIONS
-- ------------------------------------------------------------
-- All objects live in BADGER_DB. Three schemas are used:
--
--   BADGER_DB.RAW  -- Raw ingested data, untransformed.
--                     Tables: HMDA_LAR_2024, HMDA_TS_2024,
--                     HMDA_MSABD_2024
--
--   BADGER_DB.CUR  -- Curation layer. Prefix: CUR_
--                     Tables: CUR_LAR_2024, CUR_LAR_ENRICHED_2024,
--                     CUR_INSTITUTION_2024, CUR_GEOGRAPHY_2024
--                     Stored Procedure: PROC_ENRICH_LAR
--                     Task: TASK_ENRICH_LAR_WEEKLY
--
--   BADGER_DB.AGG  -- Aggregation layer. Prefix: AGG_
--                     Tables: AGG_APPROVAL_BY_RACE_INSTITUTION,
--                     AGG_LOAN_METRICS_BY_INSTITUTION,
--                     AGG_DENIAL_REASONS_BY_RACE,
--                     AGG_INCOME_AMI_BY_MSA
--                     Materialized View: AGG_MV_APPROVAL_RATES_RELIABLE
--                     Table Function: FN_APPROVAL_RATES_BY_INSTITUTION
--
-- All objects are tagged with BADGER_DB.CUR.BADGER_PROJECT
-- = 'BadgerProject' for catalog discovery.

-- ------------------------------------------------------------
-- MINI DATA CATALOG: CUSTOM FIELDS IN CURATION LAYER
-- ------------------------------------------------------------
--
-- CUR_LAR_2024 (Worksheet 1 -- 8 curation steps)
-- -----------------------------------------------
--
-- ACTION_TAKEN_LABEL (Step 2)
--   Maps the raw ACTION_TAKEN integer code (1-8) to a
--   human-readable label. Example: 1 -> 'Loan Originated',
--   3 -> 'Application Denied'. Source codes defined in
--   HMDA Filing Instructions Guide, Table 4.
--
-- APPROVED_FLAG (Step 2)
--   Binary Y/N field. Y = applicant received or was offered
--   a loan (action_taken IN 1, 2, 8). N = application denied
--   (action_taken = 3). NULL for withdrawn, incomplete, or
--   purchased loans (action_taken IN 4, 5, 6, 7), which are
--   not underwriting decisions and are excluded from approval
--   rate calculations.
--
-- DENIAL_REASON_1_LABEL (Step 3)
--   Maps the raw DENIAL_REASON_1 integer code (1-10) to a
--   human-readable label. Example: 1 -> 'Debt-to-Income
--   Ratio', 3 -> 'Credit History'. Only populated for denied
--   applications (APPROVED_FLAG = 'N').
--
-- DTI_BUCKET (Step 4)
--   Re-bins the DEBT_TO_INCOME_RATIO field into uniform
--   10-point-wide buckets. The raw HMDA data mixes broad
--   string buckets (<20%, 20%-<30%, 50%-60%, >60%) with
--   individual integers (36, 37, ... 49) for the 30-50%
--   range. This field consolidates those integers:
--   36-39 -> '30%-<40%', 40-49 -> '40%-<50%', preserving
--   the existing broad buckets unchanged.
--
-- IS_HIGHER_PRICED_FLAG (Step 5)
--   Y/N flag. Y = rate_spread >= 1.5, the HMDA regulatory
--   threshold for a "higher-priced" first-lien loan (APR
--   exceeds the Average Prime Offer Rate by 1.5+ points).
--   A key fair lending signal: disproportionate higher-priced
--   lending to protected class borrowers indicates potential
--   pricing discrimination.
--
-- DERIVED_RACE_CLEAN / DERIVED_ETHNICITY_CLEAN (Step 6)
--   Replaces 'Free Form Text Only' values in DERIVED_RACE and
--   DERIVED_ETHNICITY with 'Race Not Available' and 'Ethnicity
--   Not Available' respectively. 'Free Form Text Only' occurs
--   when applicants write in a race/ethnicity not in the HMDA
--   standard code list; it is treated as not available for
--   analysis because it cannot be consistently categorized.
--
-- ANY_APPLICANT_ABOVE_62_FLAG (Step 7)
--   Y/N flag. Y = APPLICANT_AGE_ABOVE_62 = 'Yes' OR
--   CO_APPLICANT_AGE_ABOVE_62 = 'Yes'. Combines both parties
--   on the application into a single flag for fair lending
--   analysis under ECOA's age-protected class provisions.
--
-- HOEPA_STATUS_LABEL (Step 8)
--   Maps raw HOEPA_STATUS codes to labels:
--   1 -> 'High-Cost Mortgage', 2 -> 'Not a High-Cost
--   Mortgage', 3 -> 'Not Applicable'. HOEPA (Home Ownership
--   and Equity Protection Act) identifies loans subject to
--   additional consumer protections due to high costs.
--
-- CUR_LAR_ENRICHED_2024 (Worksheet 2 -- stored procedure)
-- --------------------------------------------------------
--
-- INCOME_TO_AMI_RATIO (Transformation 1)
--   Formula: (INCOME * 1000) / FFIEC_MSA_MD_MEDIAN_FAMILY_INCOME
--   HMDA income is stored in thousands of dollars; multiplying
--   by 1000 converts to actual dollars before dividing by the
--   FFIEC area median income for the applicant's MSA/MD.
--   Result is the applicant's income as a proportion of their
--   local area median. NULL when either input is missing.
--
-- INCOME_TO_AMI_BUCKET (Transformation 1)
--   Categorizes INCOME_TO_AMI_RATIO into standard CRA
--   (Community Reinvestment Act) income tiers used by
--   regulators to assess whether lenders serve low- and
--   moderate-income communities:
--     Low        = ratio < 0.50  (below 50% of AMI)
--     Moderate   = 0.50 to <0.80 (50-80% of AMI)
--     Middle     = 0.80 to <1.20 (80-120% of AMI)
--     Upper      = >= 1.20       (above 120% of AMI)
--
-- TRACT_MINORITY_CATEGORY (Transformation 2)
--   Classifies each census tract by its minority population
--   share (TRACT_MINORITY_POPULATION_PERCENT) into three
--   tiers used in redlining and geographic fair lending
--   analysis:
--     Majority-Minority Tract  = >= 50% minority population
--     Mixed Tract              = 20% to <50%
--     Predominantly Non-Minority Tract = < 20%
--
-- AGG layer derived fields
-- ------------------------
--
-- APPROVAL_RATE_PCT (AGG_APPROVAL_BY_RACE_INSTITUTION)
--   Formula: (TOTAL_APPROVED / TOTAL_APPLICATIONS) * 100
--   Percentage of applications from a given race group at a
--   given institution that resulted in a favorable outcome
--   (APPROVED_FLAG = 'Y').
--
-- AIR_VS_WHITE (FN_APPROVAL_RATES_BY_INSTITUTION)
--   Formula: APPROVAL_RATE_PCT (group) / APPROVAL_RATE_PCT (White)
--   Adverse Impact Ratio relative to White applicants at the
--   same institution. Values below 0.80 meet the regulatory
--   "4/5ths rule" threshold for potential adverse impact and
--   warrant further fair lending review. NULL when no White
--   applicants exist for that institution.
--
-- HIGHER_PRICED_RATE_PCT (AGG_LOAN_METRICS_BY_INSTITUTION)
--   Formula: (higher_priced_count / total_applications) * 100
--   Percentage of an institution's applications that received
--   a higher-priced loan (IS_HIGHER_PRICED_FLAG = 'Y').
--
-- PCT_OF_DENIALS_FOR_RACE (AGG_DENIAL_REASONS_BY_RACE)
--   Formula: denial_count / SUM(denial_count) OVER
--   (PARTITION BY race) * 100. Within each racial group, the
--   percentage of denials attributable to each denial reason.
--   Allows comparison of denial reason distributions across
--   groups to identify potential inconsistencies in
--   underwriting rationale.
-- ============================================================

ALTER USER BADGER SET MINS_TO_BYPASS_MFA = 60;

-- Getting data out of Snowflake to create hosted app
SELECT * FROM BADGER_DB.AGG.AGG_APPROVAL_BY_RACE_INSTITUTION;
SELECT * FROM BADGER_DB.AGG.AGG_LOAN_METRICS_BY_INSTITUTION;
SELECT * FROM BADGER_DB.AGG.AGG_DENIAL_REASONS_BY_RACE;
SELECT * FROM BADGER_DB.AGG.AGG_INCOME_AMI_BY_MSA;