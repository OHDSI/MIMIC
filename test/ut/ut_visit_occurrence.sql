-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Unit tests for cdm_visit_occurrence table
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- unique
-- -------------------------------------------------------------------

INSERT INTO `@metrics_project`.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_visit_occurrence'                        AS table_id,
    'unique'                            AS test_type, -- unique, not null, concept etc.
    'visit_occurrence_id'                         AS field_name,
    CAST(NULL AS STRING)                AS condition_json,
    (COUNT(visit_occurrence_id) - COUNT(DISTINCT visit_occurrence_id) = 0) AS test_passed
FROM
    `@etl_project`.@etl_dataset.cdm_visit_occurrence
;

INSERT INTO `@metrics_project`.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_visit_occurrence'                        AS table_id,
    'unique'                            AS test_type, -- unique, not null, concept etc.
    'visit_source_value'               AS field_name,
    CAST(NULL AS STRING)                AS condition_json,
    (COUNT(visit_source_value) - COUNT(DISTINCT visit_source_value) = 0) AS test_passed
FROM
    `@etl_project`.@etl_dataset.cdm_visit_occurrence
;

-- -------------------------------------------------------------------
-- not null
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- standard_concept
--      allows 0?
-- -------------------------------------------------------------------

INSERT INTO `@metrics_project`.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_visit_occurrence'                        AS table_id,
    'standard_concept'                  AS test_type, -- unique, not null, concept etc.
    'visit_concept_id'                 AS field_name,
    CAST(NULL AS STRING)                AS condition_json,
    (COUNT(*) > 0 AND COUNT(*) - COUNT(vc.concept_id) = 0) AS test_passed
FROM
    `@etl_project`.@etl_dataset.cdm_visit_occurrence cdm
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept vc
        ON cdm.visit_concept_id = vc.concept_id
        AND vc.standard_concept = 'S'
WHERE
    cdm.visit_concept_id <> 0
;

-- -------------------------------------------------------------------
-- vocabulary
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- domain
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- range
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- foreign key
-- -------------------------------------------------------------------

INSERT INTO `@metrics_project`.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_visit_occurrence'              AS table_id,
    'foreign key'                       AS test_type, -- unique, not null, concept etc.
    'person_id'                         AS field_name,
    CAST(NULL AS STRING)                AS condition_json,
    (COUNT(*) - COUNT(fk.person_id) = 0) AS test_passed
FROM
    `@etl_project`.@etl_dataset.cdm_visit_occurrence cdm
LEFT JOIN
    `@etl_project`.@etl_dataset.cdm_person fk
        ON cdm.person_id = fk.person_id
;

INSERT INTO `@metrics_project`.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_visit_occurrence'              AS table_id,
    'foreign key'                       AS test_type, -- unique, not null, concept etc.
    'preceding_visit_occurrence_id'     AS field_name,
    CAST(NULL AS STRING)                AS condition_json,
    (COUNT(*) - COUNT(fk.person_id) = 0) AS test_passed
FROM
    `@etl_project`.@etl_dataset.cdm_visit_occurrence cdm
LEFT JOIN
    `@etl_project`.@etl_dataset.cdm_visit_occurrence fk
        ON cdm.preceding_visit_occurrence_id = fk.visit_occurrence_id
WHERE
    cdm.preceding_visit_occurrence_id IS NOT NULL
;

