-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Unit tests for cdm_condition_occurrence table
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- unique
-- -------------------------------------------------------------------

INSERT INTO `@metrics_project`.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_condition_occurrence'          AS table_id,
    'unique'                            AS test_type, -- unique, not null, concept etc.
    'condition_occurrence_id'           AS field_name,
    CAST(NULL AS STRING)                AS condition_json,
    (COUNT(condition_occurrence_id) - COUNT(DISTINCT condition_occurrence_id) = 0) AS test_passed
FROM
    `@target_project`.@target_dataset.cdm_condition_occurrence
;

-- -------------------------------------------------------------------
-- not null
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- concept
--
-- standard_concept
-- domain
-- vocabulary
--      allows 0?
-- -------------------------------------------------------------------

INSERT INTO `@metrics_project`.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_condition_occurrence'                        AS table_id,
    'concept'                           AS test_type, -- unique, not null, concept etc.
    'condition_concept_id'              AS field_name,
    TO_JSON_STRING(STRUCT(
        'S'             AS standard_concept,
        ['ICD9CM', 'ICD10CM'] AS vocabulary,
        ['Condition']       AS domain
    ))                                  AS condition_json,
    (COUNT(*) > 0 AND COUNT(*) - COUNT(vc.concept_id) = 0) AS test_passed
FROM
    `@target_project`.@target_dataset.cdm_condition_occurrence cdm
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept vc
        ON cdm.condition_concept_id = vc.concept_id
        AND vc.standard_concept = 'S'
        AND vc.vocabulary_id IN ('ICD9CM', 'ICD10CM')
        AND vc.domain_id = 'Condition'
WHERE
    cdm.condition_concept_id <> 0
;

-- -------------------------------------------------------------------
-- range
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- start_end_consistency
-- start time should be not later than end time
-- -------------------------------------------------------------------

INSERT INTO `@metrics_project`.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_condition_occurrence'          AS table_id,
    'start_end_consistency'             AS test_type, -- unique, not null, concept etc.
    'condition_start_date'              AS field_name,
    CAST(NULL AS STRING)                AS condition_json,
    (COUNT(*) = 0) AS test_passed
FROM
    `@target_project`.@target_dataset.cdm_condition_occurrence
WHERE
    condition_start_date > COALESCE(condition_end_date, PARSE_DATE('%Y-%m-%d','2099-12-31'))
;

INSERT INTO `@metrics_project`.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_condition_occurrence'          AS table_id,
    'start_end_consistency'             AS test_type, -- unique, not null, concept etc.
    'condition_start_datetime'          AS field_name,
    CAST(NULL AS STRING)                AS condition_json,
    (COUNT(*) = 0) AS test_passed
FROM
    `@target_project`.@target_dataset.cdm_condition_occurrence
WHERE
    condition_start_datetime > COALESCE(condition_end_datetime, PARSE_DATETIME('%F','2099-12-31'))
;

-- -------------------------------------------------------------------
-- foreign key
-- -------------------------------------------------------------------

INSERT INTO `@metrics_project`.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_condition_occurrence'          AS table_id,
    'foreign key'                       AS test_type, -- unique, not null, concept etc.
    'person_id'                         AS field_name,
    CAST(NULL AS STRING)                AS condition_json,
    (COUNT(*) = 0)                      AS test_passed
FROM
    `@target_project`.@target_dataset.cdm_condition_occurrence cdm
LEFT JOIN
    `@target_project`.@target_dataset.cdm_person fk
        ON cdm.person_id = fk.person_id
WHERE
    -- cdm.person_id -- required = true
    fk.person_id IS NULL
;

INSERT INTO `@metrics_project`.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_condition_occurrence'          AS table_id,
    'foreign key'                       AS test_type, -- unique, not null, concept etc.
    'visit_occurrence_id'               AS field_name,
    CAST(NULL AS STRING)                AS condition_json,
    (COUNT(*) = 0)                      AS test_passed
FROM
    `@target_project`.@target_dataset.cdm_condition_occurrence cdm
LEFT JOIN
    `@target_project`.@target_dataset.cdm_visit_occurrence fk
        ON cdm.visit_occurrence_id = fk.visit_occurrence_id
WHERE
    cdm.visit_occurrence_id IS NOT NULL -- required = false
    AND fk.visit_occurrence_id IS NULL
;

INSERT INTO `@metrics_project`.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_condition_occurrence'          AS table_id,
    'foreign_key'                       AS test_type, -- unique, not null, concept etc.
    'condition_source_value'            AS field_name,
    TO_JSON_STRING(STRUCT(
        '@etl_project.@etl_dataset.src_diagnoses_icd.icd_code' AS foreign_key
    ))                                  AS condition_json,
    (COUNT(*) = 0)                      AS test_passed
FROM
    `@target_project`.@target_dataset.cdm_condition_occurrence cdm
LEFT JOIN
    `@etl_project`.@etl_dataset.src_diagnoses_icd fk
        ON cdm.condition_source_value = fk.icd_code
;
