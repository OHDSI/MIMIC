-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Populate cdm_specimen table
-- 
-- Dependencies: run after 
--      cdm_person.sql,
--      cdm_visit_occurrence,
--      cdm_visit_detail(?)
--      lk_meas_labevents (empty specimen from labevents)
--      
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize create or replace
--
-- -------------------------------------------------------------------



-- -------------------------------------------------------------------
-- Rule 1 specimen from microbiology
-- -------------------------------------------------------------------


-- -------------------------------------------------------------------
-- Rule 2 specimen from labevents (fake?)
-- -------------------------------------------------------------------


-- -------------------------------------------------------------------
-- Rule 3 specimen from chartevents (fake?)
-- -------------------------------------------------------------------


























-- -------------------------------------------------------------------
-- lk_specimen_mapped
-- -------------------------------------------------------------------


-- -------------------------------------------------------------------
-- cdm_specimen
-- -------------------------------------------------------------------

--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.cdm_specimen
(
    specimen_id                 INT64     not null ,
    person_id                   INT64     not null ,
    specimen_concept_id         INT64     not null ,
    specimen_type_concept_id    INT64     not null ,
    specimen_date               DATE      not null ,
    specimen_datetime           DATETIME           ,
    quantity                    FLOAT64            ,
    unit_concept_id             INT64              ,
    anatomic_site_concept_id    INT64              ,
    disease_status_concept_id   INT64              ,
    specimen_source_id          STRING             ,
    specimen_source_value       STRING             ,
    unit_source_value           STRING             ,
    anatomic_site_source_value  STRING             ,
    disease_status_source_value STRING             ,
    -- 
    unit_id                       STRING,
    load_table_id                 STRING,
    load_row_id                   INT64,
    trace_id                      STRING
)
;


INSERT INTO `@etl_project`.@etl_dataset.cdm_specimen
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())           AS specimen_id,
    per.person_id                               AS person_id,
    src.target_concept_id                       AS specimen_concept_id,
    src.type_concept_id                         AS specimen_type_concept_id,
    CAST(src.start_datetime AS DATE)            AS specimen_date,
    src.start_datetime                          AS specimen_datetime,
    CAST(src.quantity AS INT64)                 AS quantity,
    src.unit_concept_id                         AS unit_concept_id,
    src.anatomic_site_concept_id                AS anatomic_site_concept_id
    src.disease_status_concept_id               AS disease_status_concept_id,
    src.specimen_source_id                      AS specimen_source_id,
    src.specimen_source_value                   AS specimen_source_value,
    src.unit_source_value                       AS unit_source_value,
    src.anatomic_site_source_value              AS anatomic_site_source_value,
    src.disease_status_source_value             AS disease_status_source_value,
    -- 
    CONCAT('specimen.', src.unit_id)    AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    `@etl_project`.@etl_dataset.lk_specimen_mapped src
LEFT JOIN 
    `@etl_project`.@etl_dataset.cdm_person per
        ON CAST(src.subject_id AS STRING) = per.person_source_value
WHERE
    src.target_domain_id = 'Specimen'
;
