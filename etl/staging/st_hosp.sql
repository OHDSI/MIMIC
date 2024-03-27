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

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.src_diagnoses_icd AS
SELECT
    h.subject_id      AS subject_id,
    h.hadm_id         AS hadm_id,
    seq_num         AS seq_num,
    icd_code        AS icd_code,
    icd_version     AS icd_version,
    --
    'diagnoses_icd'                     AS load_table_id,
    FARM_FINGERPRINT(GENERATE_UUID())   AS load_row_id,
    TO_JSON_STRING(STRUCT(
        h.hadm_id AS hadm_id,
        seq_num AS seq_num
    ))                                  AS trace_id
FROM
    @source_project.@hosp_dataset.diagnoses_icd h
JOIN @etl_project.@etl_dataset.subjects_to_include s
ON h.subject_id = s.subject_id
JOIN @etl_project.@etl_dataset.hadm_ids_to_include a
ON h.hadm_id = a.hadm_id
;

-- -------------------------------------------------------------------
-- for Measurement
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- src_services
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.src_services AS
SELECT
    h.subject_id                          AS subject_id,
    h.hadm_id                             AS hadm_id,
    transfertime                        AS transfertime,
    prev_service                        AS prev_service,
    curr_service                        AS curr_service,
    --
    'services'                          AS load_table_id,
    FARM_FINGERPRINT(GENERATE_UUID())   AS load_row_id,
    TO_JSON_STRING(STRUCT(
        h.subject_id AS subject_id,
        h.hadm_id AS hadm_id,
        transfertime AS transfertime
    ))                                  AS trace_id
FROM
    @source_project.@hosp_dataset.services h
JOIN @etl_project.@etl_dataset.subjects_to_include s
ON h.subject_id = s.subject_id
JOIN @etl_project.@etl_dataset.hadm_ids_to_include a
ON h.hadm_id = a.hadm_id
;

-- -------------------------------------------------------------------
-- src_labevents
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.src_labevents AS
SELECT
    labevent_id                         AS labevent_id,
    h.subject_id                          AS subject_id,
    charttime                           AS charttime,
    h.hadm_id                             AS hadm_id,
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
        labevent_id AS labevent_id
    ))                                  AS trace_id
FROM
    @source_project.@hosp_dataset.labevents h
JOIN @etl_project.@etl_dataset.subjects_to_include s
ON h.subject_id = s.subject_id
JOIN @etl_project.@etl_dataset.hadm_ids_to_include a
ON h.hadm_id = a.hadm_id
;

-- -------------------------------------------------------------------
-- src_d_labitems
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.src_d_labitems AS
SELECT
    itemid                              AS itemid,
    label                               AS label,
    fluid                               AS fluid,
    category                            AS category,
    CAST(NULL AS STRING)                AS loinc_code, -- MIMIC IV 2.0 change, the field is removed
    --
    'd_labitems'                        AS load_table_id,
    FARM_FINGERPRINT(GENERATE_UUID())   AS load_row_id,
    TO_JSON_STRING(STRUCT(
        itemid AS itemid
    ))                                  AS trace_id
FROM
    @source_project.@hosp_dataset.d_labitems
;


-- -------------------------------------------------------------------
-- for Procedure
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- src_procedures_icd
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.src_procedures_icd AS
SELECT
    h.subject_id                          AS subject_id,
    h.hadm_id                             AS hadm_id,
    icd_code        AS icd_code,
    icd_version     AS icd_version,
    --
    'procedures_icd'                    AS load_table_id,
    FARM_FINGERPRINT(GENERATE_UUID())   AS load_row_id,
    TO_JSON_STRING(STRUCT(
        h.subject_id AS subject_id,
        h.hadm_id AS hadm_id,
        icd_code AS icd_code,
        icd_version AS icd_version
    ))                                  AS trace_id -- this set of fields is not unique. To set quantity?
FROM
    @source_project.@hosp_dataset.procedures_icd h
JOIN @etl_project.@etl_dataset.subjects_to_include s
ON h.subject_id = s.subject_id
JOIN @etl_project.@etl_dataset.hadm_ids_to_include a
ON h.hadm_id = a.hadm_id
;

-- -------------------------------------------------------------------
-- src_hcpcsevents
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.src_hcpcsevents AS
SELECT
    h.hadm_id                             AS hadm_id,
    h.subject_id                          AS subject_id,
    hcpcs_cd                            AS hcpcs_cd,
    seq_num                             AS seq_num,
    short_description                   AS short_description,
    --
    'hcpcsevents'                       AS load_table_id,
    FARM_FINGERPRINT(GENERATE_UUID())   AS load_row_id,
    TO_JSON_STRING(STRUCT(
        h.subject_id AS subject_id,
        h.hadm_id AS hadm_id,
        hcpcs_cd AS hcpcs_cd,
        seq_num AS seq_num
    ))                                  AS trace_id -- this set of fields is not unique. To set quantity?
FROM
    @source_project.@hosp_dataset.hcpcsevents h
JOIN @etl_project.@etl_dataset.subjects_to_include s
ON h.subject_id = s.subject_id
JOIN @etl_project.@etl_dataset.hadm_ids_to_include a
ON h.hadm_id = a.hadm_id
;


-- -------------------------------------------------------------------
-- src_drgcodes
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.src_drgcodes AS
SELECT
    h.hadm_id                             AS hadm_id,
    h.subject_id                          AS subject_id,
    drg_code                            AS drg_code,
    description                         AS description,
    --
    'drgcodes'                       AS load_table_id,
    FARM_FINGERPRINT(GENERATE_UUID())   AS load_row_id,
    TO_JSON_STRING(STRUCT(
        h.subject_id AS subject_id,
        h.hadm_id AS hadm_id,
        COALESCE(drg_code, '') AS drg_code
    ))                                  AS trace_id -- this set of fields is not unique.
FROM
    @source_project.@hosp_dataset.drgcodes h
JOIN @etl_project.@etl_dataset.subjects_to_include s
ON h.subject_id = s.subject_id
JOIN @etl_project.@etl_dataset.hadm_ids_to_include a
ON h.hadm_id = a.hadm_id
;

-- -------------------------------------------------------------------
-- src_prescriptions
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.src_prescriptions AS
SELECT
    h.hadm_id                             AS hadm_id,
    h.subject_id                          AS subject_id,
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
        h.subject_id AS subject_id,
        h.hadm_id AS hadm_id,
        pharmacy_id AS pharmacy_id,
        starttime AS starttime
    ))                                  AS trace_id
FROM
    @source_project.@hosp_dataset.prescriptions h
JOIN @etl_project.@etl_dataset.subjects_to_include s
ON h.subject_id = s.subject_id
JOIN @etl_project.@etl_dataset.hadm_ids_to_include a
ON h.hadm_id = a.hadm_id
;


-- -------------------------------------------------------------------
-- src_microbiologyevents
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.src_microbiologyevents AS
SELECT
    microevent_id               AS microevent_id,
    h.subject_id                  AS subject_id,
    h.hadm_id                     AS hadm_id,
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
        h.subject_id AS subject_id,
        h.hadm_id AS hadm_id,
        microevent_id AS microevent_id
    ))                                  AS trace_id
FROM
    @source_project.@hosp_dataset.microbiologyevents h
JOIN @etl_project.@etl_dataset.subjects_to_include s
ON h.subject_id = s.subject_id
JOIN @etl_project.@etl_dataset.hadm_ids_to_include a
ON h.hadm_id = a.hadm_id
;

-- -------------------------------------------------------------------
-- src_d_micro
-- raw d_micro is no longer available both in mimic_hosp and mimiciv_hosp
-- -------------------------------------------------------------------

-- CREATE OR REPLACE TABLE @etl_project.@etl_dataset.src_d_micro AS
-- SELECT
--     itemid                      AS itemid, -- numeric ID
--     label                       AS label, -- source_code for custom mapping
--     category                    AS category, 
--     --
--     'd_micro'                   AS load_table_id,
--     FARM_FINGERPRINT(GENERATE_UUID())   AS load_row_id,
--     TO_JSON_STRING(STRUCT(
--         itemid AS itemid
--     ))                                  AS trace_id
-- FROM
--     @source_project.@hosp_dataset.d_micro
-- ;

-- -------------------------------------------------------------------
-- src_d_micro
-- MIMIC IV 2.0: generate src_d_micro from microbiologyevents
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.src_d_micro AS
WITH d_micro AS (

    SELECT DISTINCT
        ab_itemid                   AS itemid, -- numeric ID
        ab_name                     AS label, -- source_code for custom mapping
        'ANTIBIOTIC'                AS category, 
        --
        TO_JSON_STRING(STRUCT(
            'ab_itemid' AS field_name,
            ab_itemid AS itemid
        ))                                  AS trace_id
    FROM
        @source_project.@hosp_dataset.microbiologyevents
    WHERE
        ab_itemid IS NOT NULL
    UNION ALL
    SELECT DISTINCT
        test_itemid                 AS itemid, -- numeric ID
        test_name                   AS label, -- source_code for custom mapping
        'MICROTEST'                 AS category, 
        --
        TO_JSON_STRING(STRUCT(
            'test_itemid' AS field_name,
            test_itemid AS itemid
        ))                                  AS trace_id
    FROM
        @source_project.@hosp_dataset.microbiologyevents
    WHERE
        test_itemid IS NOT NULL
    UNION ALL
    SELECT DISTINCT
        org_itemid                  AS itemid, -- numeric ID
        org_name                    AS label, -- source_code for custom mapping
        'ORGANISM'                  AS category, 
        --
        TO_JSON_STRING(STRUCT(
            'org_itemid' AS field_name,
            org_itemid AS itemid
        ))                                  AS trace_id
    FROM
        @source_project.@hosp_dataset.microbiologyevents
    WHERE
        org_itemid IS NOT NULL
    UNION ALL
    SELECT DISTINCT
        spec_itemid                 AS itemid, -- numeric ID
        spec_type_desc              AS label, -- source_code for custom mapping
        'SPECIMEN'                  AS category, 
        --
        TO_JSON_STRING(STRUCT(
            'spec_itemid' AS field_name,
            spec_itemid AS itemid
        ))                                  AS trace_id
    FROM
        @source_project.@hosp_dataset.microbiologyevents
    WHERE
        spec_itemid IS NOT NULL
)
SELECT
    itemid                      AS itemid, -- numeric ID
    label                       AS label, -- source_code for custom mapping
    category                    AS category, 
    --
    'microbiologyevents'                AS load_table_id,
    FARM_FINGERPRINT(GENERATE_UUID())   AS load_row_id,
    trace_id                            AS trace_id
FROM
    d_micro
;

-- -------------------------------------------------------------------
-- src_pharmacy
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.src_pharmacy AS
SELECT
    pharmacy_id                         AS pharmacy_id,
    medication                          AS medication,
    -- hadm_id                             AS hadm_id,
    -- subject_id                          AS subject_id,
    -- starttime                           AS starttime,
    -- stoptime                            AS stoptime,
    -- route                               AS route,
    --
    'pharmacy'                          AS load_table_id,
    FARM_FINGERPRINT(GENERATE_UUID())   AS load_row_id,
    TO_JSON_STRING(STRUCT(
        pharmacy_id AS pharmacy_id
    ))                                  AS trace_id
FROM
    @source_project.@hosp_dataset.pharmacy
;

