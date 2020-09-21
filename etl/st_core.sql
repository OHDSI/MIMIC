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

CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.src_patients AS
    SELECT 
        subject_id                          AS subject_id,
        anchor_year                         AS year_of_birth,
        gender                              AS gender_source_value,
        --
        'patients'                          AS load_table_id,
        FARM_FINGERPRINT(GENERATE_UUID())   AS load_row_id,
        TO_JSON_STRING(STRUCT(
            subject_id AS subject_id
        ))                                  AS trace_id
    FROM
        `physionet-data`.mimic_demo_core.patients
;

-- -------------------------------------------------------------------
-- src_admissions
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.src_admissions AS
    SELECT
        hadm_id                             AS hadm_id, -- PK
        subject_id                          AS subject_id,
        admittime                           AS admittime,
        dischtime                           AS dischtime,
        -- deathtime
        admission_type                      AS admission_type,
        admission_location                  AS admission_location,
        discharge_location                  AS discharge_location,
        ethnicity                           AS ethnicity,
        edregtime                           AS edregtime,
        -- edouttime
        -- hospital_expire_flag
        --
        'admissions'                        AS load_table_id,
        FARM_FINGERPRINT(GENERATE_UUID())   AS load_row_id,
        TO_JSON_STRING(STRUCT(
            hadm_id AS hadm_id
        ))                                  AS trace_id
    FROM
        `physionet-data`.mimic_demo_core.admissions
;

-- -------------------------------------------------------------------
-- src_transfers
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.src_transfers AS
    SELECT
        transfer_id                         AS transfer_id,
        careunit                            AS careunit,
        --
        'transfers'                         AS load_table_id,
        FARM_FINGERPRINT(GENERATE_UUID())   AS load_row_id,
        TO_JSON_STRING(STRUCT(
            transfer_id AS transfer_id
        ))                                  AS trace_id
    FROM
        `physionet-data`.mimic_demo_core.transfers
;

