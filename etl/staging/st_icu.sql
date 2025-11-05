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
    p.hadm_id                             AS hadm_id,
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
        p.hadm_id AS hadm_id,
        starttime AS starttime
    ))                                  AS trace_id
FROM
    @source_project.@icu_dataset.procedureevents p
JOIN @etl_project.@etl_dataset.hadm_ids_to_include h
ON p.hadm_id = h.hadm_id
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
    d.hadm_id     AS hadm_id,
    stay_id     AS stay_id,
    itemid      AS itemid,
    charttime   AS charttime,
    value       AS value,
    --
    'datetimeevents'                    AS load_table_id,
    FARM_FINGERPRINT(GENERATE_UUID())   AS load_row_id,
    TO_JSON_STRING(STRUCT(
        subject_id AS subject_id,
        d.hadm_id AS hadm_id,
        stay_id AS stay_id,
        charttime AS charttime
    ))                                  AS trace_id
FROM
    @source_project.@icu_dataset.datetimeevents d
JOIN @etl_project.@etl_dataset.hadm_ids_to_include h
ON d.hadm_id = h.hadm_id
;


CREATE OR REPLACE TABLE @etl_project.@etl_dataset.src_chartevents AS
SELECT
    subject_id  AS subject_id,
    c.hadm_id     AS hadm_id,
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
        c.hadm_id AS hadm_id,
        stay_id AS stay_id,
        charttime AS charttime
    ))                                  AS trace_id
FROM
    @source_project.@icu_dataset.chartevents c
JOIN @etl_project.@etl_dataset.hadm_ids_to_include h
ON c.hadm_id = h.hadm_id
;

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.src_outputevents AS
SELECT
    subject_id  AS subject_id,
    o.hadm_id     AS hadm_id,
    stay_id     AS stay_id,
    charttime   AS charttime,
    storetime   AS storetime,
    itemid      AS itemid,
    value       AS value,
    valueuom    AS valueuom,
    --
    'outputevents'                       AS load_table_id,
    FARM_FINGERPRINT(GENERATE_UUID())   AS load_row_id,
    TO_JSON_STRING(STRUCT(
        subject_id AS subject_id,
        o.hadm_id AS hadm_id,
        stay_id AS stay_id,
        charttime AS charttime
    ))                                  AS trace_id
FROM
    @source_project.@icu_dataset.outputevents o
JOIN @etl_project.@etl_dataset.hadm_ids_to_include h
ON o.hadm_id = h.hadm_id
;
