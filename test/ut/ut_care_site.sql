-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Unit tests for cdm_care_site table
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- unique
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_care_site'                        AS table_id,
    'unique'                            AS test_type, -- unique, not null, concept etc.
    'care_site_id'                         AS field_name,
    CAST(NULL AS STRING)                AS condition_json,
    (COUNT(care_site_id) - COUNT(DISTINCT care_site_id) = 0) AS test_passed
FROM
    @etl_project.@etl_dataset.cdm_care_site
;

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_care_site'                        AS table_id,
    'unique'                            AS test_type, -- unique, not null, concept etc.
    'care_site_source_value'               AS field_name,
    CAST(NULL AS STRING)                AS condition_json,
    (COUNT(care_site_source_value) - COUNT(DISTINCT care_site_source_value) = 0) AS test_passed
FROM
    @etl_project.@etl_dataset.cdm_care_site
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
    'cdm_care_site'                        AS table_id,
    'standard_concept'                  AS test_type, -- unique, not null, concept etc.
    'place_of_service_concept_id'                 AS field_name,
    CAST(NULL AS STRING)                AS condition_json,
    (COUNT(*) > 0 AND COUNT(*) - COUNT(vc.concept_id) = 0) AS test_passed
FROM
    @etl_project.@etl_dataset.cdm_care_site cdm
LEFT JOIN
    @etl_project.@etl_dataset.voc_concept vc
        ON cdm.place_of_service_concept_id = vc.concept_id
        AND vc.standard_concept = 'S'
WHERE
    cdm.place_of_service_concept_id <> 0
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
