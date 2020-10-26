-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Populate cdm_visit_occurrence table
-- 
-- Dependencies: run after 
--      st_core.sql,
--      cdm_person.sql,
--      cdm_care_site
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize "create or replace"
-- `@etl_project`.@etl_dataset.cdm_person;
--
-- negative unique id from FARM_FINGERPRINT(GENERATE_UUID())
--
-- Using cdm_care_site:
--      care_site_name = 'BIDMC' -- Beth Israel hospital for all
--      (populate with departments)
--
-- Field diagnosis is not found in admissions table.
--      diagnosis is used to set admission/discharge concepts for organ donors
--      use hosp.diagnosis_icd + hosp.d_icd_diagnoses/voc_concept?
--
-- gcpt_* tables - custom mapping
-- Add actual custom mapping
--
-- 0 AS visit_source_concept_id, -- concept_id should not be mimic_id
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- tmp_admissions_emerged
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.tmp_admissions_emerged AS
SELECT
    hadm_id,
    subject_id,
    admission_location,
    discharge_location,
    FARM_FINGERPRINT(GENERATE_UUID()) AS visit_occurrence_id, -- mimic_id
    COALESCE(edregtime, admittime) AS admittime, -- ed?
    dischtime,
    admission_type,
    edregtime, -- is not used, but keep to check admittime
    '' AS diagnosis,
    --
    load_table_id,
    load_row_id,
    trace_id
FROM
    `@etl_project`.@etl_dataset.src_admissions 
;

-- -------------------------------------------------------------------
-- tmp_admissions
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.tmp_admissions AS
SELECT
    hadm_id                     hadm_id,
    subject_id                  AS subject_id,
    admission_location          AS admission_location,
    discharge_location          AS discharge_location,
    visit_occurrence_id         AS visit_occurrence_id,
    admittime                   AS admittime,
    dischtime                   AS dischtime,
    admission_type              AS admission_type,
    diagnosis                   AS diagnosis,
    LAG(visit_occurrence_id) OVER ( 
        PARTITION BY subject_id 
        ORDER BY admittime
    )                           AS preceding_visit_occurrence_id,
    --
    load_table_id,
    load_row_id,
    trace_id
FROM `@etl_project`.@etl_dataset.tmp_admissions_emerged
;

-- -------------------------------------------------------------------
-- tmp_gcpt_admission_type_to_concept
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE 
    `@etl_project`.@etl_dataset.tmp_gcpt_admission_type_to_concept AS
SELECT 
    0 AS visit_source_concept_id,
    'admission_type' AS admission_type,
    0 AS visit_concept_id 
--     mimic_id AS visit_source_concept_id,
--     admission_type AS admission_type,
--     visit_concept_id 
-- FROM gcpt_admission_type_to_concept
-- gcpt_admission_type_to_concept -> mimiciv_vis_admission_type
;

-- -------------------------------------------------------------------
-- tmp_gcpt_admission_location_to_concept
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE 
    `@etl_project`.@etl_dataset.tmp_gcpt_admission_location_to_concept AS
SELECT 
    0 AS admitting_concept_id,
    0 AS admitting_source_concept_id,
    'admission_location' AS admission_location 
    -- concept_id AS admitting_concept_id,
    -- mimic_id AS admitting_source_concept_id,
    -- admission_location 
-- FROM gcpt_admission_location_to_concept
;

-- -------------------------------------------------------------------
-- tmp_gcpt_discharge_location_to_concept
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE 
    `@etl_project`.@etl_dataset.tmp_gcpt_discharge_location_to_concept AS
SELECT 
    0 AS discharge_to_concept_id,
    0 AS discharge_to_source_concept_id,
    'discharge_location' AS discharge_location 
--     concept_id AS discharge_to_concept_id,
--     mimic_id AS discharge_to_source_concept_id,
--     discharge_location 
-- FROM gcpt_discharge_location_to_concept
;

-- -------------------------------------------------------------------
-- cdm_visit_occurrence
-- -------------------------------------------------------------------

--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.cdm_visit_occurrence
(
    visit_occurrence_id           INT64     not null ,
    person_id                     INT64     not null ,
    visit_concept_id              INT64     not null ,
    visit_start_date              DATE      not null ,
    visit_start_datetime          DATETIME           ,
    visit_end_date                DATE      not null ,
    visit_end_datetime            DATETIME           ,
    visit_type_concept_id         INT64     not null ,
    provider_id                   INT64              ,
    care_site_id                  INT64              ,
    visit_source_value            STRING             ,
    visit_source_concept_id       INT64              ,
    admitting_source_concept_id   INT64              ,
    admitting_source_value        STRING             ,
    discharge_to_concept_id       INT64              ,
    discharge_to_source_value     STRING             ,
    preceding_visit_occurrence_id INT64              ,
    -- 
    unit_id                       STRING,
    load_table_id                 STRING,
    load_row_id                   INT64,
    trace_id                      STRING
)
;

INSERT INTO `@etl_project`.@etl_dataset.cdm_visit_occurrence
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())   AS visit_occurrence_id,
    per.person_id                       AS person_id,
    COALESCE(lat.visit_concept_id, 0)   AS visit_concept_id, -- lat.admission_type goes nowhere
    CAST(src.admittime AS DATE)         AS visit_start_date,
    src.admittime                       AS visit_start_datetime,
    CAST(src.dischtime AS DATE)         AS visit_end_date,
    src.dischtime                       AS visit_end_datetime,
    44818518                            AS visit_type_concept_id, -- Visit derived from EHR record
    CAST(NULL AS INT64)                 AS provider_id,
    cs.care_site_id                     AS care_site_id,
    CAST(src.hadm_id AS STRING)         AS visit_source_value, -- it should be an ID for visits
    0                                   AS visit_source_concept_id, -- should not be mimic_id
    CASE
        WHEN diagnosis LIKE 'organ donor' THEN  4216643 -- DEAD/EXPIRED
        ELSE COALESCE(la.admitting_concept_id, 0)
    END                 AS admitting_source_concept_id,
    CASE
        WHEN diagnosis LIKE 'organ donor' THEN 'DEAD/EXPIRED'
        ELSE la.admission_location
    END                 AS admitting_source_value,
    CASE
        WHEN diagnosis LIKE 'organ donor' THEN 4022058 --ORGAN DONOR
        ELSE COALESCE(ld.discharge_to_concept_id, 0)
    END                 AS discharge_to_concept_id,
    CASE
        WHEN diagnosis LIKE 'organ donor' THEN diagnosis
        ELSE ld.discharge_location
    END                 AS  discharge_to_source_value,
    src.preceding_visit_occurrence_id   AS preceding_visit_occurrence_id,
    --
    'visit.admissions'              AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM 
    `@etl_project`.@etl_dataset.tmp_admissions src
LEFT JOIN 
    `@etl_project`.@etl_dataset.tmp_gcpt_admission_location_to_concept la 
        USING (admission_location)
LEFT JOIN 
    `@etl_project`.@etl_dataset.tmp_gcpt_discharge_location_to_concept ld
        USING (discharge_location)
LEFT JOIN 
    `@etl_project`.@etl_dataset.tmp_gcpt_admission_type_to_concept lat
        USING (admission_type)
LEFT JOIN
    `@etl_project`.@etl_dataset.cdm_person per
        ON CAST(src.subject_id AS STRING) = per.person_source_value
LEFT JOIN 
    `@etl_project`.@etl_dataset.cdm_care_site cs
        ON care_site_name = 'BIDMC' -- Beth Israel hospital for all
;
