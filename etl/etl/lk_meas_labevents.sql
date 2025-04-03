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
--      st_hosp.sql,
--      lk_vis_part_1.sql,
--      lk_meas_unit.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize create or replace
--
-- src_labevents: 
--      look closer to fields priority and specimen_id
--      Add 'Maps to value'
-- src_labevents.value: 
--      investigate if there are formatted values with thousand separators,
--      and if we need to use more complicated parsing.
--      see @etl_project.@etl_dataset.an_labevents_full
--      see a possibility to use 'Maps to value'
-- custom mapping:
--      gcpt_lab_label_to_concept -> mimiciv_meas_lab_loinc
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Rule 1
-- LABS from labevents
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- lk_meas_d_labevents_clean
-- source label for custom mapping: label|fluid|category
-- source code to join vocabulary tables: coalesce(LOINC, itemid)
-- source code represented in cdm tables: itemid
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.lk_meas_d_labitems_clean AS
SELECT
    dlab.itemid                                                 AS itemid, -- for <cdm>.<source_value>
    COALESCE(dlab.loinc_code, 
        CAST(dlab.itemid AS STRING))                            AS source_code, -- to join to vocabs
    dlab.loinc_code                                             AS loinc_code, -- for the crosswalk table
    CONCAT(dlab.label, '|', dlab.fluid, '|', dlab.category)     AS source_label, -- for the crosswalk table
    IF(dlab.loinc_code IS NOT NULL, 
        'LOINC', 
        'mimiciv_meas_lab_loinc'
    )                                                           AS source_vocabulary_id
FROM
    @etl_project.@etl_dataset.src_d_labitems dlab
;

-- -------------------------------------------------------------------
-- lk_meas_labevents_clean
-- source_code: itemid
-- filter: only valid itemid (100%)
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.lk_meas_labevents_clean AS
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())       AS measurement_id,
    src.subject_id                          AS subject_id,
    src.charttime                           AS start_datetime, -- measurement_datetime,
    src.hadm_id                             AS hadm_id,
    src.itemid                              AS itemid,
    src.value                               AS value, -- value_source_value
    REGEXP_EXTRACT(src.value, r'^(\<=|\>=|\>|\<|=|)')   AS value_operator,
    REGEXP_EXTRACT(src.value, r'[-]?[\d]+[.]?[\d]*')    AS value_number, -- assume "-0.34 etc"
    IF(TRIM(src.valueuom) <> '', src.valueuom, NULL)    AS valueuom, -- unit_source_value,
    src.ref_range_lower                     AS ref_range_lower,
    src.ref_range_upper                     AS ref_range_upper,
    'labevents'                             AS unit_id,
    --
    src.load_table_id       AS load_table_id,
    src.load_row_id         AS load_row_id,
    src.trace_id            AS trace_id
FROM
    @etl_project.@etl_dataset.src_labevents src
INNER JOIN
    @etl_project.@etl_dataset.src_d_labitems dlab
        ON src.itemid = dlab.itemid
;

-- -------------------------------------------------------------------
-- lk_meas_d_labitems_concept
--  gcpt_lab_label_to_concept -> mimiciv_meas_lab_loinc
-- all dlab.itemid, all available concepts from LOINC and custom mapped dlab.label
-- -------------------------------------------------------------------
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.lk_meas_d_labitems_concept AS
SELECT
    dlab.itemid                 AS itemid,
    dlab.source_code            AS source_code,
    dlab.loinc_code             AS loinc_code,
    dlab.source_label           AS source_label,
    dlab.source_vocabulary_id   AS source_vocabulary_id,
    -- source concept
    vc.domain_id                AS source_domain_id,
    vc.concept_id               AS source_concept_id,
    vc.concept_name             AS source_concept_name,
    -- target concept
    vc2.vocabulary_id           AS target_vocabulary_id,
    vc2.domain_id               AS target_domain_id,
    vc2.concept_id              AS target_concept_id,
    vc2.concept_name            AS target_concept_name,
    vc2.standard_concept        AS target_standard_concept
FROM
    @etl_project.@etl_dataset.lk_meas_d_labitems_clean dlab
LEFT JOIN
    @etl_project.@etl_dataset.voc_concept vc
        ON  vc.concept_code = dlab.source_code -- join 
        AND vc.vocabulary_id = dlab.source_vocabulary_id
        -- AND vc.domain_id = 'Measurement'
LEFT JOIN
    @etl_project.@etl_dataset.voc_concept_relationship vcr
        ON  vc.concept_id = vcr.concept_id_1
        AND vcr.relationship_id = 'Maps to'
LEFT JOIN
    @etl_project.@etl_dataset.voc_concept vc2
        ON vc2.concept_id = vcr.concept_id_2
        AND vc2.standard_concept = 'S'
        AND vc2.invalid_reason IS NULL
;

-- -------------------------------------------------------------------
-- lk_meas_labevents_hadm_id
-- pick additional hadm_id by event start_datetime
-- row_num is added to select the earliest if more than one hadm_ids are found
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.lk_meas_labevents_hadm_id AS
SELECT
    src.trace_id                        AS event_trace_id, 
    adm.hadm_id                         AS hadm_id,
    ROW_NUMBER() OVER (
        PARTITION BY src.trace_id
        ORDER BY adm.start_datetime
    )                                   AS row_num
FROM  
    @etl_project.@etl_dataset.lk_meas_labevents_clean src
INNER JOIN 
    @etl_project.@etl_dataset.lk_admissions_clean adm
        ON adm.subject_id = src.subject_id
        AND src.start_datetime BETWEEN adm.start_datetime AND adm.end_datetime
WHERE
    src.hadm_id IS NULL
;

-- -------------------------------------------------------------------
-- lk_meas_labevents_mapped
-- Rule 1 (LABS from labevents)
-- measurement_source_value: itemid
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.lk_meas_labevents_mapped AS
SELECT
    src.measurement_id                      AS measurement_id,
    src.subject_id                          AS subject_id,
    COALESCE(src.hadm_id, hadm.hadm_id)     AS hadm_id,
    CAST(src.start_datetime AS DATE)        AS date_id,
    src.start_datetime                      AS start_datetime,
    src.itemid                              AS itemid,
    CAST(src.itemid AS STRING)              AS source_code, -- change working source code to the rerpresentation
    labc.source_vocabulary_id               AS source_vocabulary_id,
    labc.source_concept_id                  AS source_concept_id,
    COALESCE(labc.target_domain_id, 'Measurement')  AS target_domain_id,
    labc.target_concept_id                  AS target_concept_id,
    src.valueuom                            AS unit_source_value,
    IF(src.valueuom IS NOT NULL, 
        COALESCE(uc.target_concept_id, 0), NULL)    AS unit_concept_id,
    src.value_operator                      AS operator_source_value,
    opc.target_concept_id                   AS operator_concept_id,
    src.value                               AS value_source_value,
    src.value_number                        AS value_as_number,
    CAST(NULL AS INT64)                     AS value_as_concept_id,
    src.ref_range_lower                     AS range_low,
    src.ref_range_upper                     AS range_high,
    --
    CONCAT('meas.', src.unit_id)    AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM  
    @etl_project.@etl_dataset.lk_meas_labevents_clean src
INNER JOIN 
    @etl_project.@etl_dataset.lk_meas_d_labitems_concept labc
        ON labc.itemid = src.itemid
LEFT JOIN 
    @etl_project.@etl_dataset.lk_meas_operator_concept opc
        ON opc.source_code = src.value_operator
LEFT JOIN 
    @etl_project.@etl_dataset.lk_meas_unit_concept uc
        ON uc.source_code = src.valueuom
LEFT JOIN 
    @etl_project.@etl_dataset.lk_meas_labevents_hadm_id hadm
        ON hadm.event_trace_id = src.trace_id
        AND hadm.row_num = 1
;
