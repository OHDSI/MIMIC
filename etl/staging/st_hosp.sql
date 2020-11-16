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
-- for Condition_occurrence
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- src_diagnoses_icd
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@target_project`.@target_dataset.src_diagnoses_icd AS
SELECT
    subject_id      AS subject_id,
    hadm_id         AS hadm_id,
    seq_num         AS seq_num,
    icd_code        AS icd_code,
    icd_version     AS icd_version,
    --
    'diagnoses_icd'                     AS load_table_id,
    FARM_FINGERPRINT(GENERATE_UUID())   AS load_row_id,
    TO_JSON_STRING(STRUCT(
        hadm_id AS hadm_id,
        seq_num AS seq_num
    ))                                  AS trace_id
FROM
    `@source_project`.@hosp_dataset.diagnoses_icd
;

-- -------------------------------------------------------------------
-- for Measurement
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- src_services
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@target_project`.@target_dataset.src_services AS
SELECT
    subject_id                          AS subject_id,
    hadm_id                             AS hadm_id,
    transfertime                        AS transfertime,
    prev_service                        AS prev_service,
    curr_service                        AS curr_service,
    --
    'services'                          AS load_table_id,
    FARM_FINGERPRINT(GENERATE_UUID())   AS load_row_id,
    TO_JSON_STRING(STRUCT(
        hadm_id AS hadm_id,
        transfertime AS transfertime
    ))                                  AS trace_id
FROM
    `@source_project`.@hosp_dataset.services
;

-- -------------------------------------------------------------------
-- src_labevents
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@target_project`.@target_dataset.src_labevents AS
SELECT
    subject_id                          AS subject_id,
    charttime                           AS charttime,
    hadm_id                             AS hadm_id,
    itemid                              AS itemid,
    valueuom                            AS valueuom,
    value                               AS value,
    flag                                AS flag,
    ref_range_lower                     AS ref_range_lower,
    ref_range_upper                     AS ref_range_upper,
    --
    'labevents'                         AS load_table_id,
    FARM_FINGERPRINT(GENERATE_UUID())   AS load_row_id,
    TO_JSON_STRING(STRUCT(
        hadm_id AS hadm_id,
        charttime AS charttime
    ))                                  AS trace_id
FROM
    `@source_project`.@hosp_dataset.labevents
;

-- -------------------------------------------------------------------
-- src_d_labitems
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@target_project`.@target_dataset.src_d_labitems AS
SELECT
    itemid                              AS itemid,
    label                               AS label,
    fluid                               AS fluid,
    category                            AS category,
    loinc_code                          AS loinc_code,
    --
    'd_labitems'                        AS load_table_id,
    FARM_FINGERPRINT(GENERATE_UUID())   AS load_row_id,
    TO_JSON_STRING(STRUCT(
        itemid AS itemid
    ))                                  AS trace_id
FROM
    `@source_project`.@hosp_dataset.d_labitems
;


-- -------------------------------------------------------------------
-- for Procedure
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- src_procedures_icd
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@target_project`.@target_dataset.src_procedures_icd AS
SELECT
    subject_id                          AS subject_id,
    hadm_id                             AS hadm_id,
    icd_code        AS icd_code,
    icd_version     AS icd_version,
    --
    'procedures_icd'                    AS load_table_id,
    FARM_FINGERPRINT(GENERATE_UUID())   AS load_row_id,
    TO_JSON_STRING(STRUCT(
        hadm_id AS hadm_id,
        icd_code AS icd_code,
        icd_version AS icd_version
    ))                                  AS trace_id -- this set of fields is not unique. To set quantity?
FROM
    `@source_project`.@hosp_dataset.procedures_icd
;

-- -------------------------------------------------------------------
-- src_hcpcsevents
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@target_project`.@target_dataset.src_hcpcsevents AS
SELECT
    hadm_id                             AS hadm_id,
    subject_id                          AS subject_id,
    hcpcs_cd                            AS hcpcs_cd,
    seq_num                             AS seq_num,
    short_description                   AS short_description,
    --
    'hcpcsevents'                       AS load_table_id,
    FARM_FINGERPRINT(GENERATE_UUID())   AS load_row_id,
    TO_JSON_STRING(STRUCT(
        hadm_id AS hadm_id,
        hcpcs_cd AS hcpcs_cd,
        seq_num AS seq_num
    ))                                  AS trace_id -- this set of fields is not unique. To set quantity?
FROM
    `@source_project`.@hosp_dataset.hcpcsevents
;


-- -------------------------------------------------------------------
-- src_drgcodes
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@target_project`.@target_dataset.src_drgcodes AS
SELECT
    hadm_id                             AS hadm_id,
    subject_id                          AS subject_id,
    drg_code                            AS drg_code,
    description                         AS description,
    --
    'drgcodes'                       AS load_table_id,
    FARM_FINGERPRINT(GENERATE_UUID())   AS load_row_id,
    TO_JSON_STRING(STRUCT(
        hadm_id AS hadm_id,
        COALESCE(drg_code, '') AS drg_code
    ))                                  AS trace_id -- this set of fields is not unique.
FROM
    `@source_project`.@hosp_dataset.drgcodes
;

-- -------------------------------------------------------------------
-- src_prescriptions
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@target_project`.@target_dataset.src_prescriptions AS
SELECT
    hadm_id                             AS hadm_id,
    subject_id                          AS subject_id,
    pharmacy_id                         AS pharmacy_id,
    starttime                           AS starttime,
    stoptime                            AS stoptime,
    drug_type                           AS drug_type,
    drug                                AS drug,
    gsn                                 AS gsn,
    ndc                                 AS ndc,
    prod_strength                       AS prod_strength,
    form_rx                             AS form_rx,
    dose_val_rx                         AS dose_val_rx,
    dose_unit_rx                        AS dose_unit_rx,
    form_val_disp                       AS form_val_disp,
    form_unit_disp                      AS form_unit_disp,
    doses_per_24_hrs                    AS doses_per_24_hrs,
    route                               AS route,
    --
    'prescriptions'                     AS load_table_id,
    FARM_FINGERPRINT(GENERATE_UUID())   AS load_row_id,
    TO_JSON_STRING(STRUCT(
        pharmacy_id AS pharmacy_id
    ))                                  AS trace_id -- this set of fields is not unique.
FROM
    `@source_project`.@hosp_dataset.prescriptions
;


-- -------------------------------------------------------------------
-- src_microbiologyevents
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@target_project`.@target_dataset.src_microbiologyevents AS
SELECT
    microevent_id               AS microevent_id,
    subject_id                  AS subject_id,
    hadm_id                     AS hadm_id,
    chartdate                   AS chartdate,
    charttime                   AS charttime, -- usage: COALESCE(charttime, chartdate)
    spec_itemid                 AS spec_itemid, -- d_micro, type of specimen taken. If no grouth, then all other fields is null
    spec_type_desc              AS spec_type_desc, -- for reference
    test_itemid                 AS test_itemid, -- d_micro, what test is taken, goes to measurement
    test_name                   AS test_name, -- for reference
    org_itemid                  AS org_itemid, -- d_micro, what bacteria have grown
    org_name                    AS org_name, -- for reference
    ab_itemid                   AS ab_itemid, -- d_micro, antibiotic tested on the bacteria
    ab_name                     AS ab_name, -- for reference
    dilution_comparison         AS dilution_comparison, -- operator sign
    dilution_value              AS dilution_value, -- numeric value
    interpretation              AS interpretation, -- bacteria's degree of resistance to the antibiotic
    --
    'microbiologyevents'                AS load_table_id,
    FARM_FINGERPRINT(GENERATE_UUID())   AS load_row_id,
    TO_JSON_STRING(STRUCT(
        microevent_id AS microevent_id
    ))                                  AS trace_id
FROM
    `@source_project`.@hosp_dataset.microbiologyevents
;

-- -------------------------------------------------------------------
-- src_d_micro
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@target_project`.@target_dataset.src_d_micro AS
SELECT
    itemid                      AS itemid, -- numeric ID
    label                       AS label, -- source_code for custom mapping
    category                    AS category, 
    --
    'd_micro'                   AS load_table_id,
    FARM_FINGERPRINT(GENERATE_UUID())   AS load_row_id,
    TO_JSON_STRING(STRUCT(
        itemid AS itemid
    ))                                  AS trace_id
FROM
    `@source_project`.@hosp_dataset.d_micro
;
