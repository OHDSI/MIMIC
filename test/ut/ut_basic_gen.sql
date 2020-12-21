
-- -------------------------------------------------------------------
-- MIMIC IV UT script generated 2020-12-15 18:57:59.572891 by gen_bq_ut_basic.py
-- -------------------------------------------------------------------


-- -------------------------------------------------------------------
-- cdm_fact_relationship
-- -------------------------------------------------------------------


-- -------------------------------------------------------------------
-- FK to `@etl_project`.@etl_dataset.cdm_measurement.measurement_id
-- -------------------------------------------------------------------

INSERT INTO `@metrics_project`.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_fact_relationship'          AS table_id,
    'foreign key'                       AS test_type, -- unique, not null, concept etc.
    'fact_id_1'                         AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(cdm.fact_id_1) = 0)          AS test_passed -- FK source
FROM
    `@etl_project`.@etl_dataset.cdm_fact_relationship cdm
LEFT JOIN
    `@etl_project`.@etl_dataset.cdm_measurement fk
        ON cdm.fact_id_1 = fk.measurement_id
WHERE
    fk.measurement_id IS NULL -- FK target
    AND cdm.unit_id = 'fact.ab.test'
;

-- -------------------------------------------------------------------
-- FK to `@etl_project`.@etl_dataset.cdm_specimen.specimen_id
-- -------------------------------------------------------------------

INSERT INTO `@metrics_project`.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_fact_relationship'          AS table_id,
    'foreign key'                       AS test_type, -- unique, not null, concept etc.
    'fact_id_2'                         AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(cdm.fact_id_2) = 0)          AS test_passed -- FK source
FROM
    `@etl_project`.@etl_dataset.cdm_fact_relationship cdm
LEFT JOIN
    `@etl_project`.@etl_dataset.cdm_specimen fk
        ON cdm.fact_id_2 = fk.specimen_id
WHERE
    fk.specimen_id IS NULL -- FK target
    AND cdm.unit_id = 'fact.test.spec'
;
