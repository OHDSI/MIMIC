-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Populate staging tables for cdm dimension tables
-- 
-- Dependencies: run first after DDL
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- src_procedureevents
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.src_procedureevents AS
SELECT
    hadm_id                             AS hadm_id,
    subject_id                          AS subject_id,
    stay_id                             AS stay_id,
    itemid                              AS itemid,
    starttime                           AS starttime,
    value                               AS value,
    CAST(0 AS INT64)                    AS cancelreason, -- MIMIC IV 2.0 change, the field is removed
    --
    'procedureevents'                   AS load_table_id,
    FARM_FINGERPRINT(GENERATE_UUID())   AS load_row_id,
    TO_JSON_STRING(STRUCT(
        subject_id AS subject_id,
        hadm_id AS hadm_id,
        starttime AS starttime
    ))                                  AS trace_id
FROM
    @source_project.@icu_dataset.procedureevents
;

-- -------------------------------------------------------------------
-- src_d_items
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.src_d_items AS
SELECT
    itemid                              AS itemid,
    label                               AS label,
    linksto                             AS linksto,
    -- abbreviation 
    -- category
    -- unitname
    -- param_type
    -- lownormalvalue
    -- highnormalvalue
    --
    'd_items'                           AS load_table_id,
    FARM_FINGERPRINT(GENERATE_UUID())   AS load_row_id,
    TO_JSON_STRING(STRUCT(
        itemid AS itemid,
        linksto AS linksto
    ))                                  AS trace_id
FROM
    @source_project.@icu_dataset.d_items
;

-- -------------------------------------------------------------------
-- src_datetimeevents
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.src_datetimeevents AS
SELECT
    subject_id  AS subject_id,
    hadm_id     AS hadm_id,
    stay_id     AS stay_id,
    itemid      AS itemid,
    charttime   AS charttime,
    value       AS value,
    --
    'datetimeevents'                    AS load_table_id,
    FARM_FINGERPRINT(GENERATE_UUID())   AS load_row_id,
    TO_JSON_STRING(STRUCT(
        subject_id AS subject_id,
        hadm_id AS hadm_id,
        stay_id AS stay_id,
        charttime AS charttime
    ))                                  AS trace_id
FROM
    @source_project.@icu_dataset.datetimeevents
;


CREATE OR REPLACE TABLE @etl_project.@etl_dataset.src_chartevents AS
SELECT
    subject_id  AS subject_id,
    hadm_id     AS hadm_id,
    stay_id     AS stay_id,
    itemid      AS itemid,
    charttime   AS charttime,
    value       AS value,
    valuenum    AS valuenum,
    valueuom    AS valueuom,
    --
    'chartevents'                       AS load_table_id,
    FARM_FINGERPRINT(GENERATE_UUID())   AS load_row_id,
    TO_JSON_STRING(STRUCT(
        subject_id AS subject_id,
        hadm_id AS hadm_id,
        stay_id AS stay_id,
        charttime AS charttime
    ))                                  AS trace_id
FROM
    @source_project.@icu_dataset.chartevents
;
