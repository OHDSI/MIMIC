-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Populate cdm_observation table
-- 
-- Dependencies: run after 
--      lk_observation
--      lk_procedure
--      lk_meas_chartevents
--      lk_cond_diagnoses
--      cdm_person.sql
--      cdm_visit_occurrence
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- cdm_observation
-- -------------------------------------------------------------------

--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_observation
(
    observation_id                INT64     not null ,
    person_id                     INT64     not null ,
    observation_concept_id        INT64     not null ,
    observation_date              DATE      not null ,
    observation_datetime          DATETIME           ,
    observation_type_concept_id   INT64     not null ,
    value_as_number               FLOAT64        ,
    value_as_string               STRING         ,
    value_as_concept_id           INT64          ,
    qualifier_concept_id          INT64          ,
    unit_concept_id               INT64          ,
    provider_id                   INT64          ,
    visit_occurrence_id           INT64          ,
    visit_detail_id               INT64          ,
    observation_source_value      STRING         ,
    observation_source_concept_id INT64          ,
    unit_source_value             STRING         ,
    qualifier_source_value        STRING         ,
    -- 
    unit_id                       STRING,
    load_table_id                 STRING,
    load_row_id                   INT64,
    trace_id                      STRING
)
;

-- -------------------------------------------------------------------
-- Rules 1-4
-- lk_observation_mapped (demographics and DRG codes)
-- -------------------------------------------------------------------

INSERT INTO @etl_project.@etl_dataset.cdm_observation
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())           AS observation_id,
    per.person_id                               AS person_id,
    src.target_concept_id                       AS observation_concept_id,
    CAST(src.start_datetime AS DATE)            AS observation_date,
    src.start_datetime                          AS observation_datetime,
    src.type_concept_id                         AS observation_type_concept_id,
    CAST(NULL AS FLOAT64)                       AS value_as_number,
    src.value_as_string                         AS value_as_string,
    IF(src.value_as_string IS NOT NULL,
        COALESCE(src.value_as_concept_id, 0),
        NULL)                                   AS value_as_concept_id,
    CAST(NULL AS INT64)                         AS qualifier_concept_id,
    CAST(NULL AS INT64)                         AS unit_concept_id,
    CAST(NULL AS INT64)                         AS provider_id,
    vis.visit_occurrence_id                     AS visit_occurrence_id,
    CAST(NULL AS INT64)                         AS visit_detail_id,
    src.source_code                             AS observation_source_value,
    src.source_concept_id                       AS observation_source_concept_id,
    CAST(NULL AS STRING)                        AS unit_source_value,
    CAST(NULL AS STRING)                        AS qualifier_source_value,
    -- 
    CONCAT('observation.', src.unit_id)         AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    @etl_project.@etl_dataset.lk_observation_mapped src
INNER JOIN
    @etl_project.@etl_dataset.cdm_person per
        ON CAST(src.subject_id AS STRING) = per.person_source_value
INNER JOIN
    @etl_project.@etl_dataset.cdm_visit_occurrence vis
        ON  vis.visit_source_value = 
            CONCAT(CAST(src.subject_id AS STRING), '|', CAST(src.hadm_id AS STRING))
WHERE
    src.target_domain_id = 'Observation'
;

-- -------------------------------------------------------------------
-- Rule 5
-- chartevents
-- -------------------------------------------------------------------

INSERT INTO @etl_project.@etl_dataset.cdm_observation
SELECT
    src.measurement_id                          AS observation_id, -- id is generated already
    per.person_id                               AS person_id,
    src.target_concept_id                       AS observation_concept_id,
    CAST(src.start_datetime AS DATE)            AS observation_date,
    src.start_datetime                          AS observation_datetime,
    src.type_concept_id                         AS observation_type_concept_id,
    src.value_as_number                         AS value_as_number,
    src.value_source_value                      AS value_as_string,
    IF(src.value_source_value IS NOT NULL,
        COALESCE(src.value_as_concept_id, 0),
        NULL)                                   AS value_as_concept_id,
    CAST(NULL AS INT64)                         AS qualifier_concept_id,
    src.unit_concept_id                         AS unit_concept_id,
    CAST(NULL AS INT64)                         AS provider_id,
    vis.visit_occurrence_id                     AS visit_occurrence_id,
    CAST(NULL AS INT64)                         AS visit_detail_id,
    src.source_code                             AS observation_source_value,
    src.source_concept_id                       AS observation_source_concept_id,
    src.unit_source_value                       AS unit_source_value,
    CAST(NULL AS STRING)                        AS qualifier_source_value,
    -- 
    CONCAT('observation.', src.unit_id)         AS unit_id,
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
    src.target_domain_id = 'Observation'
;

-- -------------------------------------------------------------------
-- Rule 6
-- lk_procedure_mapped
-- -------------------------------------------------------------------

INSERT INTO @etl_project.@etl_dataset.cdm_observation
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())           AS observation_id,
    per.person_id                               AS person_id,
    src.target_concept_id                       AS observation_concept_id,
    CAST(src.start_datetime AS DATE)            AS observation_date,
    src.start_datetime                          AS observation_datetime,
    src.type_concept_id                         AS observation_type_concept_id,
    CAST(NULL AS FLOAT64)                       AS value_as_number,
    CAST(NULL AS STRING)                        AS value_as_string,
    CAST(NULL AS INT64)                         AS value_as_concept_id,
    CAST(NULL AS INT64)                         AS qualifier_concept_id,
    CAST(NULL AS INT64)                         AS unit_concept_id,
    CAST(NULL AS INT64)                         AS provider_id,
    vis.visit_occurrence_id                     AS visit_occurrence_id,
    CAST(NULL AS INT64)                         AS visit_detail_id,
    src.source_code                             AS observation_source_value,
    src.source_concept_id                       AS observation_source_concept_id,
    CAST(NULL AS STRING)                        AS unit_source_value,
    CAST(NULL AS STRING)                        AS qualifier_source_value,
    -- 
    CONCAT('observation.', src.unit_id)         AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    @etl_project.@etl_dataset.lk_procedure_mapped src
INNER JOIN
    @etl_project.@etl_dataset.cdm_person per
        ON CAST(src.subject_id AS STRING) = per.person_source_value
INNER JOIN
    @etl_project.@etl_dataset.cdm_visit_occurrence vis
        ON  vis.visit_source_value = 
            CONCAT(CAST(src.subject_id AS STRING), '|', CAST(src.hadm_id AS STRING))
WHERE
    src.target_domain_id = 'Observation'
;

-- -------------------------------------------------------------------
-- Rule 7
-- diagnoses
-- -------------------------------------------------------------------

INSERT INTO @etl_project.@etl_dataset.cdm_observation
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())           AS observation_id,
    per.person_id                               AS person_id,
    src.target_concept_id                       AS observation_concept_id, -- to rename fields in *_mapped
    CAST(src.start_datetime AS DATE)            AS observation_date,
    src.start_datetime                          AS observation_datetime,
    src.type_concept_id                         AS observation_type_concept_id,
    CAST(NULL AS FLOAT64)                       AS value_as_number,
    CAST(NULL AS STRING)                        AS value_as_string,
    CAST(NULL AS INT64)                         AS value_as_concept_id,
    CAST(NULL AS INT64)                         AS qualifier_concept_id,
    CAST(NULL AS INT64)                         AS unit_concept_id,
    CAST(NULL AS INT64)                         AS provider_id,
    vis.visit_occurrence_id                     AS visit_occurrence_id,
    CAST(NULL AS INT64)                         AS visit_detail_id,
    src.source_code                             AS observation_source_value,
    src.source_concept_id                       AS observation_source_concept_id,
    CAST(NULL AS STRING)                        AS unit_source_value,
    CAST(NULL AS STRING)                        AS qualifier_source_value,
    -- 
    CONCAT('observation.', src.unit_id)         AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    @etl_project.@etl_dataset.lk_diagnoses_icd_mapped src
INNER JOIN
    @etl_project.@etl_dataset.cdm_person per
        ON CAST(src.subject_id AS STRING) = per.person_source_value
INNER JOIN
    @etl_project.@etl_dataset.cdm_visit_occurrence vis
        ON  vis.visit_source_value = 
            CONCAT(CAST(src.subject_id AS STRING), '|', CAST(src.hadm_id AS STRING))
WHERE
    src.target_domain_id = 'Observation'
;

-- -------------------------------------------------------------------
-- Rule 8
-- lk_specimen_mapped
-- -------------------------------------------------------------------

INSERT INTO @etl_project.@etl_dataset.cdm_observation
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())           AS observation_id,
    per.person_id                               AS person_id,
    src.target_concept_id                       AS observation_concept_id,
    CAST(src.start_datetime AS DATE)            AS observation_date,
    src.start_datetime                          AS observation_datetime,
    src.type_concept_id                         AS observation_type_concept_id,
    CAST(NULL AS FLOAT64)                       AS value_as_number,
    CAST(NULL AS STRING)                        AS value_as_string,
    CAST(NULL AS INT64)                         AS value_as_concept_id,
    CAST(NULL AS INT64)                         AS qualifier_concept_id,
    CAST(NULL AS INT64)                         AS unit_concept_id,
    CAST(NULL AS INT64)                         AS provider_id,
    vis.visit_occurrence_id                     AS visit_occurrence_id,
    CAST(NULL AS INT64)                         AS visit_detail_id,
    src.source_code                             AS observation_source_value,
    src.source_concept_id                       AS observation_source_concept_id,
    CAST(NULL AS STRING)                        AS unit_source_value,
    CAST(NULL AS STRING)                        AS qualifier_source_value,
    -- 
    CONCAT('observation.', src.unit_id)         AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    @etl_project.@etl_dataset.lk_specimen_mapped src
INNER JOIN
    @etl_project.@etl_dataset.cdm_person per
        ON CAST(src.subject_id AS STRING) = per.person_source_value
INNER JOIN
    @etl_project.@etl_dataset.cdm_visit_occurrence vis
        ON  vis.visit_source_value = 
            CONCAT(CAST(src.subject_id AS STRING), '|', 
                COALESCE(CAST(src.hadm_id AS STRING), CAST(src.date_id AS STRING)))
WHERE
    src.target_domain_id = 'Observation'
;

