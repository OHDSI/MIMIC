-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Unit tests for cdm_drug_exposure table: inputevents coverage
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- inputevents rows land in cdm_drug_exposure
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_drug_exposure'                 AS table_id,
    'row presence'                      AS test_type, -- unique, not null, concept etc.
    'load_table_id'                     AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(*) > 0)                      AS test_passed
FROM
    @etl_project.@etl_dataset.cdm_drug_exposure
WHERE
    load_table_id = 'inputevents'
;

-- -------------------------------------------------------------------
-- inputevents rows carry the administration type concept
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_drug_exposure'                 AS table_id,
    'type concept'                      AS test_type, -- unique, not null, concept etc.
    'drug_type_concept_id'              AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNTIF(drug_type_concept_id != 32839) = 0) AS test_passed
FROM
    @etl_project.@etl_dataset.cdm_drug_exposure
WHERE
    load_table_id = 'inputevents'
;

-- -------------------------------------------------------------------
-- inputevents mapping rate is non-zero
-- at least one administered drug resolves to a standard RxNorm concept
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_drug_exposure'                 AS table_id,
    'mapping rate'                      AS test_type, -- unique, not null, concept etc.
    'drug_concept_id'                   AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (SUM(CASE WHEN drug_concept_id != 0 THEN 1 ELSE 0 END) > 0) AS test_passed
FROM
    @etl_project.@etl_dataset.cdm_drug_exposure
WHERE
    load_table_id = 'inputevents'
;
