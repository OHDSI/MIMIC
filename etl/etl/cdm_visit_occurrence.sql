-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Populate visit_occurrence table
-- 
-- Dependencies: run after 
--      st_core.sql,
--      person.sql,
--      care_site
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize "create or replace"
-- negative unique id from FARM_FINGERPRINT(GENERATE_UUID())
--
-- Using care_site:
--      care_site_name = 'BIDMC' -- Beth Israel hospital for all
--      (populate with departments)
--
-- Field diagnosis is not found in admissions table.
--      diagnosis is used to set admission/discharge concepts for organ donors
--      use hosp.diagnosis_icd + hosp.d_icd_diagnoses/voc_concept?
--
-- Review logic for organ donors. Concepts used in MIMIC III:
--      4216643 -- DEAD/EXPIRED
--      4022058 -- ORGAN DONOR
--
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- visit_occurrence
-- -------------------------------------------------------------------

--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.visit_occurrence
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
    admitted_from_concept_id      INT64              ,
    admitted_from_source_value    STRING             ,
    discharged_to_concept_id      INT64              ,
    discharged_to_source_value    STRING             ,
    preceding_visit_occurrence_id INT64              ,
    -- 
    unit_id                       STRING,
    load_table_id                 STRING,
    load_row_id                   INT64,
    trace_id                      STRING
)
;

INSERT INTO @etl_project.@etl_dataset.visit_occurrence
SELECT
    src.visit_occurrence_id                 AS visit_occurrence_id,
    per.person_id                           AS person_id,
    COALESCE(lat.target_concept_id, 0)      AS visit_concept_id,
    DATE_ADD(CAST(src.start_datetime AS DATE), INTERVAL ds.offset_days DAY) AS visit_start_date,
    DATETIME_ADD(src.start_datetime, INTERVAL ds.offset_days DAY) AS visit_start_datetime,
    DATE_ADD(CAST(src.end_datetime AS DATE), INTERVAL ds.offset_days DAY) AS visit_end_date,
    DATETIME_ADD(src.end_datetime, INTERVAL ds.offset_days DAY) AS visit_end_datetime,
    32817                                   AS visit_type_concept_id,   -- EHR   Type Concept    Standard                          
    CAST(NULL AS INT64)                     AS provider_id,
    cs.care_site_id                         AS care_site_id,
    src.source_value                        AS visit_source_value, -- it should be an ID for visits
    COALESCE(lat.source_concept_id, 0)      AS visit_source_concept_id, -- it is where visit_concept_id comes from
    IF(
        src.admission_location IS NOT NULL,
        COALESCE(la.target_concept_id, 0),
        NULL)                               AS admitted_from_concept_id,
    src.admission_location                  AS admitted_from_source_value,
    IF(
        src.discharge_location IS NOT NULL,
        COALESCE(ld.target_concept_id, 0),
        NULL)                               AS discharged_to_concept_id,
    src.discharge_location                  AS discharged_to_source_value,
    LAG(src.visit_occurrence_id) OVER ( 
        PARTITION BY subject_id, hadm_id 
        ORDER BY start_datetime
    )                                   AS preceding_visit_occurrence_id,
    --
    CONCAT('visit.', src.unit_id)   AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM 
    @etl_project.@etl_dataset.lk_visit_clean src
INNER JOIN
    @etl_project.@etl_dataset.person per
        ON CAST(src.subject_id AS STRING) = per.person_source_value
LEFT JOIN 
    @etl_project.@etl_dataset.lk_visit_concept lat
        ON lat.source_code = src.admission_type
LEFT JOIN 
    @etl_project.@etl_dataset.lk_visit_concept la 
        ON la.source_code = src.admission_location
LEFT JOIN 
    @etl_project.@etl_dataset.lk_visit_concept ld
        ON ld.source_code = src.discharge_location
LEFT JOIN 
    @etl_project.@etl_dataset.care_site cs
        ON care_site_name = 'BIDMC' -- Beth Israel hospital for all
LEFT JOIN 
    @etl_project.@etl_dataset.person_date_shift_lookup ds
        ON per.person_id = ds.person_id
;
