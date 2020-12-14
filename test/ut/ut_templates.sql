-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Unit tests for cdm_{table_name} table
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- unique
-- -------------------------------------------------------------------

INSERT INTO `@metrics_project`.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_{table_name}'          AS table_id,
    'unique'                            AS test_type, -- unique, not null, concept etc.
    '{table_name}_id'           AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT({table_name}_id) - COUNT(DISTINCT {table_name}_id) = 0) AS test_passed
FROM
    `@etl_project`.@etl_dataset.cdm_{table_name}
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
    'cdm_{table_name}'                        AS table_id,
    'concept'                           AS test_type, -- unique, not null, concept etc.
    '{domain_id}_concept_id'              AS field_name,
    TO_JSON_STRING(STRUCT(
        'S'             AS standard_concept,
        ['{domain_id}']       AS domain -- Aaaaaa
    ))                                  AS criteria_json,
    (COUNT(*) > 0 AND COUNT(*) - COUNT(vc.concept_id) = 0) AS test_passed
FROM
    `@etl_project`.@etl_dataset.cdm_{table_name} cdm
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept vc
        ON cdm.{domain_id}_concept_id = vc.concept_id
        AND vc.standard_concept = 'S'
        AND vc.domain_id = '{domain_id}' -- Aaaaaa
WHERE
    cdm.{domain_id}_concept_id <> 0
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
    'cdm_{table_name}'          AS table_id,
    'start_end_consistency'             AS test_type, -- unique, not null, concept etc.
    '{date_prefix}_start_date'              AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(*) = 0) AS test_passed
FROM
    `@etl_project`.@etl_dataset.cdm_{table_name}
WHERE
    {date_prefix}_start_date > COALESCE({date_prefix}_end_date, PARSE_DATE('%Y-%m-%d','2099-12-31'))
;

INSERT INTO `@metrics_project`.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_{table_name}'          AS table_id,
    'start_end_consistency'             AS test_type, -- unique, not null, concept etc.
    '{date_prefix}_start_datetime'          AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(*) = 0) AS test_passed
FROM
    `@etl_project`.@etl_dataset.cdm_{table_name}
WHERE
    {date_prefix}_start_datetime > COALESCE({date_prefix}_end_datetime, PARSE_DATETIME('%F','2099-12-31'))
;

-- -------------------------------------------------------------------
-- foreign key
-- -------------------------------------------------------------------

INSERT INTO `@metrics_project`.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_{table_name}'          AS table_id,
    'foreign key'                       AS test_type, -- unique, not null, concept etc.
    'person_id'                         AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(cdm.person_id) = 0)          AS test_passed -- FK source
FROM
    `@etl_project`.@etl_dataset.cdm_{table_name} cdm
LEFT JOIN
    `@etl_project`.@etl_dataset.cdm_person fk
        ON cdm.person_id = fk.person_id
WHERE
    fk.person_id IS NULL -- FK target
;

INSERT INTO `@metrics_project`.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_{table_name}'          AS table_id,
    'foreign key'                       AS test_type, -- unique, not null, concept etc.
    'visit_occurrence_id'               AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(cdm.visit_occurrence_id) = 0)        AS test_passed
FROM
    `@etl_project`.@etl_dataset.cdm_{table_name} cdm
LEFT JOIN
    `@etl_project`.@etl_dataset.cdm_visit_occurrence fk
        ON cdm.visit_occurrence_id = fk.visit_occurrence_id
WHERE
    fk.visit_occurrence_id IS NULL
;

INSERT INTO `@metrics_project`.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_{table_name}'          AS table_id,
    'foreign_key'                       AS test_type, -- unique, not null, concept etc.
    '{source_value_prefix}_source_value'            AS field_name,
    TO_JSON_STRING(STRUCT(
        '@etl_project.@etl_dataset.src_diagnoses_icd.icd_code' AS foreign_key
    ))                                  AS criteria_json,
    (COUNT(cdm.{source_value_prefix}_source_value) = 0) AS test_passed
FROM
    `@etl_project`.@etl_dataset.cdm_{table_name} cdm
LEFT JOIN
    `@etl_project`.@etl_dataset.src_diagnoses_icd fk
        ON cdm.{source_value_prefix}_source_value = fk.icd_code
WHERE
    fk.icd_code IS NULL
;
