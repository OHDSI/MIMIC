-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Populate cdm_specimen table
-- 
-- Dependencies: run after 
--      cdm_person.sql,
--      lk_meas_specimen.sql
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
-- cdm_specimen
-- -------------------------------------------------------------------

--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_specimen
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


INSERT INTO @etl_project.@etl_dataset.cdm_specimen
SELECT
    src.specimen_id                             AS specimen_id,
    per.person_id                               AS person_id,
    COALESCE(src.target_concept_id, 0)          AS specimen_concept_id,
    32856                                       AS specimen_type_concept_id, -- OMOP4976929 Lab
    CAST(src.start_datetime AS DATE)            AS specimen_date,
    src.start_datetime                          AS specimen_datetime,
    CAST(NULL AS FLOAT64)                       AS quantity,
    CAST(NULL AS INT64)                         AS unit_concept_id,
    0                                           AS anatomic_site_concept_id,
    0                                           AS disease_status_concept_id,
    src.trace_id                                AS specimen_source_id,
    src.source_code                             AS specimen_source_value,
    CAST(NULL AS STRING)                        AS unit_source_value,
    CAST(NULL AS STRING)                        AS anatomic_site_source_value,
    CAST(NULL AS STRING)                        AS disease_status_source_value,
    -- 
    CONCAT('specimen.', src.unit_id)    AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    @etl_project.@etl_dataset.lk_specimen_mapped src
INNER JOIN
    @etl_project.@etl_dataset.cdm_person per
        ON CAST(src.subject_id AS STRING) = per.person_source_value
WHERE
    src.target_domain_id = 'Specimen'
;
