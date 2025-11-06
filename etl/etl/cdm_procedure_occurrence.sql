-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Populate procedure_occurrence table
-- 
-- Dependencies: run after 
--      person.sql,
--      visit_occurrence,
--      lk_procedure_occurrence
--      lk_meas_specimen
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize create or replace
--
-- -------------------------------------------------------------------


-- -------------------------------------------------------------------
-- procedure_occurrence
-- -------------------------------------------------------------------

--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.procedure_occurrence
(
    procedure_occurrence_id     INT64     not null ,
    person_id                   INT64     not null ,
    procedure_concept_id        INT64     not null ,
    procedure_date              DATE      not null ,
    procedure_datetime          DATETIME           ,
    procedure_end_date          DATE               ,
    procedure_end_datetime      DATETIME           ,
    procedure_type_concept_id   INT64     not null ,
    modifier_concept_id         INT64              ,
    quantity                    INT64              ,
    provider_id                 INT64              ,
    visit_occurrence_id         INT64              ,
    visit_detail_id             INT64              ,
    procedure_source_value      STRING             ,
    procedure_source_concept_id INT64              ,
    modifier_source_value      STRING              ,
    -- 
    unit_id                       STRING,
    load_table_id                 STRING,
    load_row_id                   INT64,
    trace_id                      STRING
)
;

-- -------------------------------------------------------------------
-- Rules 1-4
-- lk_procedure_mapped
-- -------------------------------------------------------------------

INSERT INTO @etl_project.@etl_dataset.procedure_occurrence
SELECT
    `@etl_project.@etl_dataset`.obf_id_str(CONCAT(
        src.trace_id, '|', 
        COALESCE(src.source_code, '')
    ), 32)                                      AS procedure_occurrence_id,
    per.person_id                               AS person_id,
    src.target_concept_id                       AS procedure_concept_id,
    DATE_ADD(CAST(src.start_datetime AS DATE), INTERVAL ds.offset_days DAY) AS procedure_date,
    DATETIME_ADD(src.start_datetime, INTERVAL ds.offset_days DAY) AS procedure_datetime,
    CAST(NULL AS DATE)                          AS procedure_end_date,
    CAST(NULL AS DATETIME)                      AS procedure_end_datetime,
    src.type_concept_id                         AS procedure_type_concept_id,
    0                                           AS modifier_concept_id,
    CAST(src.quantity AS INT64)                 AS quantity,
    CAST(NULL AS INT64)                         AS provider_id,
    vis.visit_occurrence_id                     AS visit_occurrence_id,
    CAST(NULL AS INT64)                         AS visit_detail_id,
    src.source_code                             AS procedure_source_value,
    src.source_concept_id                       AS procedure_source_concept_id,
    CAST(NULL AS STRING)                        AS modifier_source_value,
    -- 
    CONCAT('procedure.', src.unit_id)           AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    @etl_project.@etl_dataset.lk_procedure_mapped src
INNER JOIN
    @etl_project.@etl_dataset.person per
        ON CAST(src.subject_id AS STRING) = per.person_source_value
INNER JOIN
    @etl_project.@etl_dataset.visit_occurrence vis
        ON  vis.visit_source_value = 
            CONCAT(CAST(src.subject_id AS STRING), '|', CAST(src.hadm_id AS STRING))
LEFT JOIN 
    @etl_project.@etl_dataset.person_date_shift_lookup ds
        ON per.person_id = ds.person_id
WHERE
    src.target_domain_id = 'Procedure'
;

-- -------------------------------------------------------------------
-- Rule 5
-- lk_observation_mapped, possible DRG codes
-- -------------------------------------------------------------------

INSERT INTO @etl_project.@etl_dataset.procedure_occurrence
SELECT
    `@etl_project.@etl_dataset`.obf_id_str(CONCAT(
        src.trace_id, '|', 
        COALESCE(src.source_code, '')
    ), 32)                                      AS procedure_occurrence_id,
    per.person_id                               AS person_id,
    src.target_concept_id                       AS procedure_concept_id,
    DATE_ADD(CAST(src.start_datetime AS DATE), INTERVAL ds.offset_days DAY) AS procedure_date,
    DATETIME_ADD(src.start_datetime, INTERVAL ds.offset_days DAY) AS procedure_datetime,
    CAST(NULL AS DATE)                          AS procedure_end_date,
    CAST(NULL AS DATETIME)                      AS procedure_end_datetime,
    src.type_concept_id                         AS procedure_type_concept_id,
    0                                           AS modifier_concept_id,
    CAST(NULL AS INT64)                         AS quantity,
    CAST(NULL AS INT64)                         AS provider_id,
    vis.visit_occurrence_id                     AS visit_occurrence_id,
    CAST(NULL AS INT64)                         AS visit_detail_id,
    src.source_code                             AS procedure_source_value,
    src.source_concept_id                       AS procedure_source_concept_id,
    CAST(NULL AS STRING)                        AS modifier_source_value,
    -- 
    CONCAT('procedure.', src.unit_id)           AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    @etl_project.@etl_dataset.lk_observation_mapped src
INNER JOIN
    @etl_project.@etl_dataset.person per
        ON CAST(src.subject_id AS STRING) = per.person_source_value
INNER JOIN
    @etl_project.@etl_dataset.visit_occurrence vis
        ON  vis.visit_source_value = 
            CONCAT(CAST(src.subject_id AS STRING), '|', CAST(src.hadm_id AS STRING))
LEFT JOIN 
    @etl_project.@etl_dataset.person_date_shift_lookup ds
        ON per.person_id = ds.person_id
WHERE
    src.target_domain_id = 'Procedure'
;

-- -------------------------------------------------------------------
-- Rule 6
-- lk_specimen_mapped, small part of specimen is mapped to Procedure
-- -------------------------------------------------------------------

INSERT INTO @etl_project.@etl_dataset.procedure_occurrence
SELECT
    src.specimen_id                             AS procedure_occurrence_id,
    per.person_id                               AS person_id,
    src.target_concept_id                       AS procedure_concept_id,
    DATE_ADD(CAST(src.start_datetime AS DATE), INTERVAL ds.offset_days DAY) AS procedure_date,
    DATETIME_ADD(src.start_datetime, INTERVAL ds.offset_days DAY) AS procedure_datetime,
    CAST(NULL AS DATE)                          AS procedure_end_date,
    CAST(NULL AS DATETIME)                      AS procedure_end_datetime,
    src.type_concept_id                         AS procedure_type_concept_id,
    0                                           AS modifier_concept_id,
    CAST(NULL AS INT64)                         AS quantity,
    CAST(NULL AS INT64)                         AS provider_id,
    vis.visit_occurrence_id                     AS visit_occurrence_id,
    CAST(NULL AS INT64)                         AS visit_detail_id,
    src.source_code                             AS procedure_source_value,
    src.source_concept_id                       AS procedure_source_concept_id,
    CAST(NULL AS STRING)                        AS modifier_source_value,
    -- 
    CONCAT('procedure.', src.unit_id)           AS unit_id,
    src.load_table_id                           AS load_table_id,
    src.load_row_id                             AS load_row_id,
    src.trace_id                                AS trace_id
FROM
    @etl_project.@etl_dataset.lk_specimen_mapped src
INNER JOIN
    @etl_project.@etl_dataset.person per
        ON CAST(src.subject_id AS STRING) = per.person_source_value
INNER JOIN
    @etl_project.@etl_dataset.visit_occurrence vis
        ON  vis.visit_source_value = 
            CONCAT(CAST(src.subject_id AS STRING), '|', 
                COALESCE(CAST(src.hadm_id AS STRING), CAST(src.date_id AS STRING)))
LEFT JOIN 
    @etl_project.@etl_dataset.person_date_shift_lookup ds
        ON per.person_id = ds.person_id
WHERE
    src.target_domain_id = 'Procedure'
;


-- -------------------------------------------------------------------
-- Rule 7
-- lk_chartevents_mapped, a part of chartevents table is mapped to Procedure
-- -------------------------------------------------------------------

INSERT INTO @etl_project.@etl_dataset.procedure_occurrence
SELECT
    `@etl_project.@etl_dataset`.obf_id_str(src.trace_id, 32)    AS procedure_occurrence_id,
    per.person_id                                               AS person_id,
    src.target_concept_id                                       AS procedure_concept_id,
    DATE_ADD(CAST(src.start_datetime AS DATE), INTERVAL ds.offset_days DAY) AS procedure_date,
    DATETIME_ADD(src.start_datetime, INTERVAL ds.offset_days DAY) AS procedure_datetime,
    CAST(NULL AS DATE)                                          AS procedure_end_date,
    CAST(NULL AS DATETIME)                                      AS procedure_end_datetime,
    src.type_concept_id                                         AS procedure_type_concept_id,
    0                                                           AS modifier_concept_id,
    CAST(NULL AS INT64)                                         AS quantity,
    CAST(NULL AS INT64)                                         AS provider_id,
    vis.visit_occurrence_id                                     AS visit_occurrence_id,
    CAST(NULL AS INT64)                                         AS visit_detail_id,
    src.source_code                                             AS procedure_source_value,
    src.source_concept_id                                       AS procedure_source_concept_id,
    CAST(NULL AS STRING)                                        AS modifier_source_value,
    -- 
    CONCAT('procedure.', src.unit_id)                           AS unit_id,
    src.load_table_id                                           AS load_table_id,
    src.load_row_id                                             AS load_row_id,
    src.trace_id                                                AS trace_id
FROM
    @etl_project.@etl_dataset.lk_chartevents_mapped src
INNER JOIN
    @etl_project.@etl_dataset.person per
        ON CAST(src.subject_id AS STRING) = per.person_source_value
INNER JOIN
    @etl_project.@etl_dataset.visit_occurrence vis
        ON  vis.visit_source_value = 
            CONCAT(CAST(src.subject_id AS STRING), '|', CAST(src.hadm_id AS STRING))
LEFT JOIN 
    @etl_project.@etl_dataset.person_date_shift_lookup ds
        ON per.person_id = ds.person_id
WHERE
    src.target_domain_id = 'Procedure'
;

