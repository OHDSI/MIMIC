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
-- transfers.stay_id - does not exist in Demo, but is described in the online Documentation
-- -------------------------------------------------------------------


-- -------------------------------------------------------------------
-- src_patients
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.src_patients AS
SELECT DISTINCT
    c.subject_id                        AS subject_id,
    anchor_year                         AS anchor_year,
    anchor_age                          AS anchor_age,
    anchor_year_group                   AS anchor_year_group,
    gender                              AS gender,
    --
    'patients'                          AS load_table_id,
    FARM_FINGERPRINT(GENERATE_UUID())   AS load_row_id,
    TO_JSON_STRING(STRUCT(
        c.subject_id AS subject_id
    ))                                  AS trace_id
FROM
    @source_project.@core_dataset.patients c
JOIN @etl_project.@etl_dataset.subjects_to_include s
ON c.subject_id = s.subject_id
;

-- -------------------------------------------------------------------
-- src_admissions
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.src_admissions AS
SELECT DISTINCT
    c.hadm_id                           AS hadm_id, -- PK
    c.subject_id                        AS subject_id,
    admittime                           AS admittime,
    dischtime                           AS dischtime,
    deathtime                           AS deathtime,
    admission_type                      AS admission_type,
    admission_location                  AS admission_location,
    discharge_location                  AS discharge_location,
    race                                AS ethnicity, -- MIMIC IV 2.0 change, field race replaced field ethnicity
    edregtime                           AS edregtime,
    insurance                           AS insurance,
    marital_status                      AS marital_status,
    language                            AS language,
    -- edouttime
    -- hospital_expire_flag
    --
    'admissions'                        AS load_table_id,
    FARM_FINGERPRINT(GENERATE_UUID())   AS load_row_id,
    TO_JSON_STRING(STRUCT(
        c.subject_id AS subject_id,
        c.hadm_id AS hadm_id
    ))                                  AS trace_id
FROM
    @source_project.@core_dataset.admissions c
JOIN @etl_project.@etl_dataset.subjects_to_include s
ON c.subject_id = s.subject_id
JOIN @etl_project.@etl_dataset.hadm_ids_to_include a
ON c.hadm_id = a.hadm_id
;

-- -------------------------------------------------------------------
-- src_transfers
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.src_transfers AS
SELECT DISTINCT
    transfer_id                         AS transfer_id,
    c.hadm_id                             AS hadm_id,
    c.subject_id                          AS subject_id,
    careunit                            AS careunit,
    intime                              AS intime,
    outtime                             AS outtime,
    eventtype                           AS eventtype,
    --
    'transfers'                         AS load_table_id,
    FARM_FINGERPRINT(GENERATE_UUID())   AS load_row_id,
    TO_JSON_STRING(STRUCT(
        c.subject_id AS subject_id,
        c.hadm_id AS hadm_id,
        transfer_id AS transfer_id
    ))                                  AS trace_id
FROM
    @source_project.@core_dataset.transfers c
JOIN @etl_project.@etl_dataset.subjects_to_include s
ON c.subject_id = s.subject_id
JOIN @etl_project.@etl_dataset.hadm_ids_to_include a
ON c.hadm_id = a.hadm_id
;

