-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Populate cdm_device_exposure table
-- 
-- Dependencies: run after 
--      lk_drug_prescriptions.sql
--      lk_meas_chartevents.sql
--      cdm_person.sql
--      cdm_visit_occurrence.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize create or replace
--
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- cdm_device_exposure
-- Rule 1 lk_drug_mapped
-- -------------------------------------------------------------------

--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_device_exposure
(
    device_exposure_id              INT64       not null ,
    person_id                       INT64       not null ,
    device_concept_id               INT64       not null ,
    device_exposure_start_date      DATE        not null ,
    device_exposure_start_datetime  DATETIME             ,
    device_exposure_end_date        DATE                 ,
    device_exposure_end_datetime    DATETIME             ,
    device_type_concept_id          INT64       not null ,
    unique_device_id                STRING               ,
    quantity                        INT64                ,
    provider_id                     INT64                ,
    visit_occurrence_id             INT64                ,
    visit_detail_id                 INT64                ,
    device_source_value             STRING               ,
    device_source_concept_id        INT64                ,
    -- 
    unit_id                       STRING,
    load_table_id                 STRING,
    load_row_id                   INT64,
    trace_id                      STRING
)
;


INSERT INTO @etl_project.@etl_dataset.cdm_device_exposure
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())           AS device_exposure_id,
    per.person_id                               AS person_id,
    src.target_concept_id                       AS device_concept_id,
    CAST(src.start_datetime AS DATE)            AS device_exposure_start_date,
    src.start_datetime                          AS device_exposure_start_datetime,
    CAST(src.end_datetime AS DATE)              AS device_exposure_end_date,
    src.end_datetime                            AS device_exposure_end_datetime,
    src.type_concept_id                         AS device_type_concept_id,
    CAST(NULL AS STRING)                        AS unique_device_id,
    CAST(
        IF(ROUND(src.quantity) = src.quantity, src.quantity, NULL)
        AS INT64)                               AS quantity,
    CAST(NULL AS INT64)                         AS provider_id,
    vis.visit_occurrence_id                     AS visit_occurrence_id,
    CAST(NULL AS INT64)                         AS visit_detail_id,
    src.source_code                             AS device_source_value,
    src.source_concept_id                       AS device_source_concept_id,
    -- 
    CONCAT('device.', src.unit_id)  AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    @etl_project.@etl_dataset.lk_drug_mapped src
INNER JOIN
    @etl_project.@etl_dataset.cdm_person per
        ON CAST(src.subject_id AS STRING) = per.person_source_value
INNER JOIN
    @etl_project.@etl_dataset.cdm_visit_occurrence vis
        ON  vis.visit_source_value = 
            CONCAT(CAST(src.subject_id AS STRING), '|', CAST(src.hadm_id AS STRING))
WHERE
    src.target_domain_id = 'Device'
;


INSERT INTO @etl_project.@etl_dataset.cdm_device_exposure
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())           AS device_exposure_id,
    per.person_id                               AS person_id,
    src.target_concept_id                       AS device_concept_id,
    CAST(src.start_datetime AS DATE)            AS device_exposure_start_date,
    src.start_datetime                          AS device_exposure_start_datetime,
    CAST(src.start_datetime AS DATE)            AS device_exposure_end_date,
    src.start_datetime                          AS device_exposure_end_datetime,
    src.type_concept_id                         AS device_type_concept_id,
    CAST(NULL AS STRING)                        AS unique_device_id,
    CAST(
        IF(ROUND(src.value_as_number) = src.value_as_number, src.value_as_number, NULL)
        AS INT64)                               AS quantity,
    CAST(NULL AS INT64)                         AS provider_id,
    vis.visit_occurrence_id                     AS visit_occurrence_id,
    CAST(NULL AS INT64)                         AS visit_detail_id,
    src.source_code                             AS device_source_value,
    src.source_concept_id                       AS device_source_concept_id,
    -- 
    CONCAT('device.', src.unit_id)  AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    @etl_project.@etl_dataset.lk_chartevents_mapped src
INNER JOIN
    @etl_project.@etl_dataset.cdm_person per
        ON CAST(src.subject_id AS STRING) = per.person_source_value
INNER JOIN
    @etl_project.@etl_dataset.cdm_visit_occurrence vis
        ON  vis.visit_source_value = 
            CONCAT(CAST(src.subject_id AS STRING), '|', CAST(src.hadm_id AS STRING))
WHERE
    src.target_domain_id = 'Device'
;
