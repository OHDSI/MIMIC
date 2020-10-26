-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Populate lookup tables for cdm_measurement table
-- Rule 1
-- Labs from labevents
-- 
-- Dependencies: run after 
--      st_core.sql,
--      cdm_person.sql,
--      cdm_visit_occurrence
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize create or replace
--
-- src_labevents: look closer to fields priority and specimen_id
-- src_labevents.value: 
--      investigate if there are formatted values with thousand separators,
--      and if we need to use more complicated parsing.
--      see `@etl_project`.@etl_dataset.an_labevents_full
--      see a possibility to use 'Maps to value'
-- implement custom mapping using custom mapping from III
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Rule 1
-- LABS from labevents
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- lk_meas_labevents_clean
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_meas_labevents_clean AS
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())       AS measurement_id,
    src.subject_id                          AS subject_id,
    src.charttime                           AS charttime, -- measurement_datetime,
    src.hadm_id                             AS hadm_id,
    src.itemid                              AS itemid,
    src.value                               AS value, -- value_source_value
    REGEXP_EXTRACT(src.value, r'^(\<=|\>=|\>|\<|=|)')   AS value_operator,
    REGEXP_EXTRACT(src.value, r'[-]?[\d]+[.]?[\d]*')    AS value_number, -- assume "-0.34 etc"
    src.valueuom                            AS valueuom, -- unit_source_value,
    src.ref_range_lower                     AS ref_range_lower,
    src.ref_range_upper                     AS ref_range_upper,
    --
    src.load_table_id       AS load_table_id,
    src.load_row_id         AS load_row_id,
    src.trace_id            AS trace_id
FROM
    `@etl_project`.@etl_dataset.src_labevents src
;

-- -------------------------------------------------------------------
-- lk_meas_d_labitems_concept
-- gcpt_lab_label_to_concept -> mimiciv_meas_lab_loinc
-- open points: Add 'Maps to value'
-- mapping rule: 
--      a) see the joins, 
--      b) all dlab.itemid, all available concepts from LOINC and custom mapped dlab.label
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_meas_d_labitems_concept AS
SELECT
    dlab.itemid             AS itemid, -- PK
    COALESCE(dlab.loinc_code, dlab.label)   AS source_code,
    vc.domain_id            AS source_domain_id,
    vc.concept_id           AS source_concept_id,
    vc2.domain_id           AS target_domain_id,
    vc2.concept_id          AS target_concept_id,
    dlab.fluid              AS fluid,
    dlab.category           AS category -- 'Blood Gas', 'Chemistry', 'Hematology'
FROM
    `@etl_project`.@etl_dataset.src_d_labitems dlab
        -- double check loinc codes: do we need to use event dates in join?
        -- and do we need to do any parsing/replacement to match codes?
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept vc
        ON  vc.concept_code = COALESCE(dlab.loinc_code, dlab.label)
        AND (
            vc.vocabulary_id = 'LOINC' AND vc.domain_id = 'Measurement'
            OR vc.vocabulary_id = 'mimiciv_me_lab_loinc'
        )
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept_relationship vcr
        ON  vc.concept_id = vcr.concept_id_1
        AND vcr.relationship_id = 'Maps to'
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept vc2
        ON vc2.concept_id = vcr.concept_id_2
        AND vc2.standard_concept = 'S'
        AND vc2.invalid_reason IS NULL
;

-- -------------------------------------------------------------------
-- lk_meas_operator_concept
-- open point: operators are usually just hard-coded
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_meas_operator_concept AS
SELECT
    vc.concept_name     AS source_code, -- operator_name,
    vc.concept_id       AS target_concept_id -- operator_concept_id
FROM
    `@etl_project`.@etl_dataset.voc_concept vc
WHERE
    vc.domain_id = 'Meas Value Operator'
;

-- -------------------------------------------------------------------
-- lk_meas_unit_concept
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_meas_unit_concept AS
SELECT
    vc.concept_name     AS source_code, -- operator_name,
    vc.concept_id       AS target_concept_id -- operator_concept_id
FROM
    `@etl_project`.@etl_dataset.voc_concept vc
WHERE
    -- gcpt_lab_unit_to_concept -> mimiciv_meas_unit
    vc.vocabulary_id IN ('UCUM', 'mimiciv_meas_unit')
    AND vc.domain_id = 'Unit'
;

-- -------------------------------------------------------------------
-- lk_meas_labevents_mapped
-- Rule 1 (LABS from labevents)
-- rule for measurement_source_value:
--      CONCAT(labc.source_code, ' | ', src.itemid)
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_meas_labevents_mapped AS
SELECT
    src.measurement_id                      AS measurement_id,
    src.subject_id                          AS subject_id,
    src.hadm_id                             AS hadm_id,
    COALESCE(labc.target_concept_id, 0)     AS target_concept_id,
    COALESCE(labc.target_domain_id, 'Measurement')  AS target_domain_id,
    src.charttime                           AS start_datetime,
    labc.category                           AS category,
    opc.target_concept_id                   AS operator_concept_id,
    src.value_number                        AS value_as_number,
    CASE
        WHEN src.valueuom IS NOT NULL THEN COALESCE(uc.target_concept_id, 0)
        ELSE NULL
    END                                     AS unit_concept_id,
    src.ref_range_lower                     AS range_low,
    src.ref_range_upper                     AS range_high,
    labc.source_code                        AS labc_source_code,
    src.itemid                              AS itemid,
    labc.source_concept_id                  AS source_concept_id,
    src.valueuom                            AS unit_source_value,
    src.value                               AS value_source_value,
    --
    'meas.labevents'                AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM  
    `@etl_project`.@etl_dataset.lk_meas_labevents_clean src
INNER JOIN 
    `@etl_project`.@etl_dataset.lk_meas_d_labitems_concept labc
        ON labc.itemid = src.itemid
LEFT JOIN 
    `@etl_project`.@etl_dataset.lk_meas_operator_concept opc
        ON opc.source_code = src.value_operator
LEFT JOIN 
    `@etl_project`.@etl_dataset.lk_meas_unit_concept uc
        ON uc.source_code = src.valueuom
;
