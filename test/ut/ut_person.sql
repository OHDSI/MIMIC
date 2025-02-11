-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Unit tests for cdm_person table
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- unique
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_person'                        AS table_id,
    'unique'                            AS test_type, -- unique, not null, concept etc.
    'person_id'                         AS field_name,
    CAST(NULL AS STRING)                AS condition_json,
    (COUNT(person_id) - COUNT(DISTINCT person_id) = 0) AS test_passed
FROM
    @etl_project.@etl_dataset.cdm_person
;

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_person'                        AS table_id,
    'unique'                            AS test_type, -- unique, not null, concept etc.
    'person_source_value'               AS field_name,
    CAST(NULL AS STRING)                AS condition_json,
    (COUNT(person_source_value) - COUNT(DISTINCT person_source_value) = 0) AS test_passed
FROM
    @etl_project.@etl_dataset.cdm_person
;

-- -------------------------------------------------------------------
-- not null
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- standard_concept
--      allows 0?
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_person'                        AS table_id,
    'standard_concept'                  AS test_type, -- unique, not null, concept etc.
    'gender_concept_id'                 AS field_name,
    CAST(NULL AS STRING)                AS condition_json,
    (COUNT(*) > 0 AND COUNT(*) - COUNT(vc.concept_id) = 0) AS test_passed
FROM
    @etl_project.@etl_dataset.cdm_person cdm
LEFT JOIN
    @etl_project.@etl_dataset.voc_concept vc
        ON cdm.gender_concept_id = vc.concept_id
        AND vc.standard_concept = 'S'
WHERE
    cdm.gender_concept_id <> 0
;

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_person'                        AS table_id,
    'standard_concept'                  AS test_type, -- unique, not null, concept etc.
    'race_concept_id'                 AS field_name,
    CAST(NULL AS STRING)                AS condition_json,
    (COUNT(*) > 0 AND COUNT(*) - COUNT(vc.concept_id) = 0) AS test_passed
FROM
    @etl_project.@etl_dataset.cdm_person cdm
LEFT JOIN
    @etl_project.@etl_dataset.voc_concept vc
        ON cdm.race_concept_id = vc.concept_id
        AND vc.standard_concept = 'S'
WHERE
    cdm.race_concept_id <> 0
;

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_person'                        AS table_id,
    'standard_concept'                  AS test_type, -- unique, not null, concept etc.
    'ethnicity_concept_id'                 AS field_name,
    CAST(NULL AS STRING)                AS condition_json,
    (COUNT(*) > 0 AND COUNT(*) - COUNT(vc.concept_id) = 0) AS test_passed
FROM
    @etl_project.@etl_dataset.cdm_person cdm
LEFT JOIN
    @etl_project.@etl_dataset.voc_concept vc
        ON cdm.ethnicity_concept_id = vc.concept_id
        AND vc.standard_concept = 'S'
WHERE
    cdm.ethnicity_concept_id <> 0
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
