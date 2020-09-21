-- ----------------------------------------------------------------------------
-- MIMIC IV to OMOP CDM
-- pre-analysis against the MIMIC IV Demo Dataset
-- ----------------------------------------------------------------------------

-- ----------------------------------------------------------------------------
-- Open points
-- 
-- Target version: V5.3.1 or V6?
-- ----------------------------------------------------------------------------

-- ----------------------------------------------------------------------------
-- collect table structure
-- ----------------------------------------------------------------------------

SELECT
    table_schema, table_name, column_name, is_nullable, data_type
FROM
    `physionet-data`.mimic_demo_hosp.INFORMATION_SCHEMA.COLUMNS
ORDER BY 
    table_name
;

-- ----------------------------------------------------------------------------
-- data samples
-- ----------------------------------------------------------------------------

SELECT DISTINCT
    careunit, COUNT(*) AS row_count
FROM
    `physionet-data`.mimic_demo_core.transfers
GROUP BY 
    careunit
ORDER BY
    row_count DESC
;


SELECT DISTINCT
    proc_type, COUNT(*) AS row_count
FROM
    `physionet-data`.mimic_demo_hosp.pharmacy
GROUP BY 
    proc_type
ORDER BY
    row_count DESC
;



-- ----------------------------------------------------------------------------
-- visits need diagnosis information to distinguish organ donors
-- ----------------------------------------------------------------------------

SELECT DISTINCT diagnosis
FROM `physionet-data.mimiciii_demo.admissions` 
WHERE diagnosis LIKE '%ORGAN%'
LIMIT 10
-- nothing
;

SELECT DISTINCT long_title
FROM `physionet-data.mimic_demo_hosp.d_icd_diagnoses` 
-- WHERE long_title LIKE '%ORGAN%'
LIMIT 10
-- nothing
;


