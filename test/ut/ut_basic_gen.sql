
-- -------------------------------------------------------------------
-- MIMIC IV UT script generated 2021-01-19 15:26:19.472142 by gen_bq_ut_basic.py
-- -------------------------------------------------------------------


-- -------------------------------------------------------------------
-- cdm_care_site
-- -------------------------------------------------------------------



-- -------------------------------------------------------------------
-- unique
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_care_site'          AS table_id,
    'unique'                            AS test_type, -- unique, not null, concept etc.
    'care_site_id'            AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(care_site_id) - COUNT(DISTINCT care_site_id) = 0) AS test_passed
FROM
    @etl_project.@etl_dataset.cdm_care_site
;

-- -------------------------------------------------------------------
-- cdm_person
-- -------------------------------------------------------------------



-- -------------------------------------------------------------------
-- unique
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_person'          AS table_id,
    'unique'                            AS test_type, -- unique, not null, concept etc.
    'person_id'            AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(person_id) - COUNT(DISTINCT person_id) = 0) AS test_passed
FROM
    @etl_project.@etl_dataset.cdm_person
;

-- -------------------------------------------------------------------
-- cdm_death
-- -------------------------------------------------------------------



-- -------------------------------------------------------------------
-- unique
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_death'          AS table_id,
    'unique'                            AS test_type, -- unique, not null, concept etc.
    'person_id'            AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(person_id) - COUNT(DISTINCT person_id) = 0) AS test_passed
FROM
    @etl_project.@etl_dataset.cdm_death
;

-- -------------------------------------------------------------------
-- FK to @etl_project.@etl_dataset.cdm_person.person_id
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_death'          AS table_id,
    'foreign key'                       AS test_type, -- unique, not null, concept etc.
    'person_id'                         AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(cdm.person_id) = 0)          AS test_passed -- FK source
FROM
    @etl_project.@etl_dataset.cdm_death cdm
LEFT JOIN
    @etl_project.@etl_dataset.cdm_person fk
        ON cdm.person_id = fk.person_id
WHERE
    fk.person_id IS NULL -- FK target
;

-- -------------------------------------------------------------------
-- cdm_observation_period
-- -------------------------------------------------------------------



-- -------------------------------------------------------------------
-- unique
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_observation_period'          AS table_id,
    'unique'                            AS test_type, -- unique, not null, concept etc.
    'observation_period_id'            AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(observation_period_id) - COUNT(DISTINCT observation_period_id) = 0) AS test_passed
FROM
    @etl_project.@etl_dataset.cdm_observation_period
;

-- -------------------------------------------------------------------
-- FK to @etl_project.@etl_dataset.cdm_person.person_id
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_observation_period'          AS table_id,
    'foreign key'                       AS test_type, -- unique, not null, concept etc.
    'person_id'                         AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(cdm.person_id) = 0)          AS test_passed -- FK source
FROM
    @etl_project.@etl_dataset.cdm_observation_period cdm
LEFT JOIN
    @etl_project.@etl_dataset.cdm_person fk
        ON cdm.person_id = fk.person_id
WHERE
    fk.person_id IS NULL -- FK target
;

-- -------------------------------------------------------------------
-- cdm_visit_occurrence
-- -------------------------------------------------------------------



-- -------------------------------------------------------------------
-- unique
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_visit_occurrence'          AS table_id,
    'unique'                            AS test_type, -- unique, not null, concept etc.
    'visit_occurrence_id'            AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(visit_occurrence_id) - COUNT(DISTINCT visit_occurrence_id) = 0) AS test_passed
FROM
    @etl_project.@etl_dataset.cdm_visit_occurrence
;


-- -------------------------------------------------------------------
-- unique
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_visit_occurrence'          AS table_id,
    'unique'                            AS test_type, -- unique, not null, concept etc.
    'visit_source_value'            AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(visit_source_value) - COUNT(DISTINCT visit_source_value) = 0) AS test_passed
FROM
    @etl_project.@etl_dataset.cdm_visit_occurrence
;

-- -------------------------------------------------------------------
-- FK to @etl_project.@etl_dataset.cdm_person.person_id
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_visit_occurrence'          AS table_id,
    'foreign key'                       AS test_type, -- unique, not null, concept etc.
    'person_id'                         AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(cdm.person_id) = 0)          AS test_passed -- FK source
FROM
    @etl_project.@etl_dataset.cdm_visit_occurrence cdm
LEFT JOIN
    @etl_project.@etl_dataset.cdm_person fk
        ON cdm.person_id = fk.person_id
WHERE
    fk.person_id IS NULL -- FK target
;

-- -------------------------------------------------------------------
-- FK to @etl_project.@etl_dataset.cdm_visit_occurrence.visit_occurrence_id
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_visit_occurrence'          AS table_id,
    'foreign key'                       AS test_type, -- unique, not null, concept etc.
    'preceding_visit_occurrence_id'                         AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(cdm.preceding_visit_occurrence_id) = 0)          AS test_passed -- FK source
FROM
    @etl_project.@etl_dataset.cdm_visit_occurrence cdm
LEFT JOIN
    @etl_project.@etl_dataset.cdm_visit_occurrence fk
        ON cdm.preceding_visit_occurrence_id = fk.visit_occurrence_id
WHERE
    fk.visit_occurrence_id IS NULL -- FK target
;

-- -------------------------------------------------------------------
-- cdm_visit_detail
-- -------------------------------------------------------------------



-- -------------------------------------------------------------------
-- unique
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_visit_detail'          AS table_id,
    'unique'                            AS test_type, -- unique, not null, concept etc.
    'visit_detail_id'            AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(visit_detail_id) - COUNT(DISTINCT visit_detail_id) = 0) AS test_passed
FROM
    @etl_project.@etl_dataset.cdm_visit_detail
;


-- -------------------------------------------------------------------
-- unique
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_visit_detail'          AS table_id,
    'unique'                            AS test_type, -- unique, not null, concept etc.
    'visit_detail_source_value'            AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(visit_detail_source_value) - COUNT(DISTINCT visit_detail_source_value) = 0) AS test_passed
FROM
    @etl_project.@etl_dataset.cdm_visit_detail
;

-- -------------------------------------------------------------------
-- FK to @etl_project.@etl_dataset.cdm_person.person_id
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_visit_detail'          AS table_id,
    'foreign key'                       AS test_type, -- unique, not null, concept etc.
    'person_id'                         AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(cdm.person_id) = 0)          AS test_passed -- FK source
FROM
    @etl_project.@etl_dataset.cdm_visit_detail cdm
LEFT JOIN
    @etl_project.@etl_dataset.cdm_person fk
        ON cdm.person_id = fk.person_id
WHERE
    fk.person_id IS NULL -- FK target
;

-- -------------------------------------------------------------------
-- FK to @etl_project.@etl_dataset.cdm_visit_occurrence.visit_occurrence_id
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_visit_detail'          AS table_id,
    'foreign key'                       AS test_type, -- unique, not null, concept etc.
    'visit_occurrence_id'                         AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(cdm.visit_occurrence_id) = 0)          AS test_passed -- FK source
FROM
    @etl_project.@etl_dataset.cdm_visit_detail cdm
LEFT JOIN
    @etl_project.@etl_dataset.cdm_visit_occurrence fk
        ON cdm.visit_occurrence_id = fk.visit_occurrence_id
WHERE
    fk.visit_occurrence_id IS NULL -- FK target
;

-- -------------------------------------------------------------------
-- FK to @etl_project.@etl_dataset.cdm_visit_detail.visit_detail_id
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_visit_detail'          AS table_id,
    'foreign key'                       AS test_type, -- unique, not null, concept etc.
    'preceding_visit_detail_id'                         AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(cdm.preceding_visit_detail_id) = 0)          AS test_passed -- FK source
FROM
    @etl_project.@etl_dataset.cdm_visit_detail cdm
LEFT JOIN
    @etl_project.@etl_dataset.cdm_visit_detail fk
        ON cdm.preceding_visit_detail_id = fk.visit_detail_id
WHERE
    fk.visit_detail_id IS NULL -- FK target
;

-- -------------------------------------------------------------------
-- cdm_condition_occurrence
-- -------------------------------------------------------------------



-- -------------------------------------------------------------------
-- unique
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_condition_occurrence'          AS table_id,
    'unique'                            AS test_type, -- unique, not null, concept etc.
    'condition_occurrence_id'            AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(condition_occurrence_id) - COUNT(DISTINCT condition_occurrence_id) = 0) AS test_passed
FROM
    @etl_project.@etl_dataset.cdm_condition_occurrence
;

-- -------------------------------------------------------------------
-- FK to @etl_project.@etl_dataset.cdm_person.person_id
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_condition_occurrence'          AS table_id,
    'foreign key'                       AS test_type, -- unique, not null, concept etc.
    'person_id'                         AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(cdm.person_id) = 0)          AS test_passed -- FK source
FROM
    @etl_project.@etl_dataset.cdm_condition_occurrence cdm
LEFT JOIN
    @etl_project.@etl_dataset.cdm_person fk
        ON cdm.person_id = fk.person_id
WHERE
    fk.person_id IS NULL -- FK target
;

-- -------------------------------------------------------------------
-- FK to @etl_project.@etl_dataset.cdm_visit_occurrence.visit_occurrence_id
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_condition_occurrence'          AS table_id,
    'foreign key'                       AS test_type, -- unique, not null, concept etc.
    'visit_occurrence_id'                         AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(cdm.visit_occurrence_id) = 0)          AS test_passed -- FK source
FROM
    @etl_project.@etl_dataset.cdm_condition_occurrence cdm
LEFT JOIN
    @etl_project.@etl_dataset.cdm_visit_occurrence fk
        ON cdm.visit_occurrence_id = fk.visit_occurrence_id
WHERE
    fk.visit_occurrence_id IS NULL -- FK target
;

-- -------------------------------------------------------------------
-- cdm_procedure_occurrence
-- -------------------------------------------------------------------



-- -------------------------------------------------------------------
-- unique
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_procedure_occurrence'          AS table_id,
    'unique'                            AS test_type, -- unique, not null, concept etc.
    'procedure_occurrence_id'            AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(procedure_occurrence_id) - COUNT(DISTINCT procedure_occurrence_id) = 0) AS test_passed
FROM
    @etl_project.@etl_dataset.cdm_procedure_occurrence
;

-- -------------------------------------------------------------------
-- FK to @etl_project.@etl_dataset.cdm_person.person_id
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_procedure_occurrence'          AS table_id,
    'foreign key'                       AS test_type, -- unique, not null, concept etc.
    'person_id'                         AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(cdm.person_id) = 0)          AS test_passed -- FK source
FROM
    @etl_project.@etl_dataset.cdm_procedure_occurrence cdm
LEFT JOIN
    @etl_project.@etl_dataset.cdm_person fk
        ON cdm.person_id = fk.person_id
WHERE
    fk.person_id IS NULL -- FK target
;

-- -------------------------------------------------------------------
-- FK to @etl_project.@etl_dataset.cdm_visit_occurrence.visit_occurrence_id
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_procedure_occurrence'          AS table_id,
    'foreign key'                       AS test_type, -- unique, not null, concept etc.
    'visit_occurrence_id'                         AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(cdm.visit_occurrence_id) = 0)          AS test_passed -- FK source
FROM
    @etl_project.@etl_dataset.cdm_procedure_occurrence cdm
LEFT JOIN
    @etl_project.@etl_dataset.cdm_visit_occurrence fk
        ON cdm.visit_occurrence_id = fk.visit_occurrence_id
WHERE
    fk.visit_occurrence_id IS NULL -- FK target
;

-- -------------------------------------------------------------------
-- cdm_observation
-- -------------------------------------------------------------------



-- -------------------------------------------------------------------
-- unique
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_observation'          AS table_id,
    'unique'                            AS test_type, -- unique, not null, concept etc.
    'observation_id'            AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(observation_id) - COUNT(DISTINCT observation_id) = 0) AS test_passed
FROM
    @etl_project.@etl_dataset.cdm_observation
;

-- -------------------------------------------------------------------
-- FK to @etl_project.@etl_dataset.cdm_person.person_id
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_observation'          AS table_id,
    'foreign key'                       AS test_type, -- unique, not null, concept etc.
    'person_id'                         AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(cdm.person_id) = 0)          AS test_passed -- FK source
FROM
    @etl_project.@etl_dataset.cdm_observation cdm
LEFT JOIN
    @etl_project.@etl_dataset.cdm_person fk
        ON cdm.person_id = fk.person_id
WHERE
    fk.person_id IS NULL -- FK target
;

-- -------------------------------------------------------------------
-- FK to @etl_project.@etl_dataset.cdm_visit_occurrence.visit_occurrence_id
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_observation'          AS table_id,
    'foreign key'                       AS test_type, -- unique, not null, concept etc.
    'visit_occurrence_id'                         AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(cdm.visit_occurrence_id) = 0)          AS test_passed -- FK source
FROM
    @etl_project.@etl_dataset.cdm_observation cdm
LEFT JOIN
    @etl_project.@etl_dataset.cdm_visit_occurrence fk
        ON cdm.visit_occurrence_id = fk.visit_occurrence_id
WHERE
    fk.visit_occurrence_id IS NULL -- FK target
;

-- -------------------------------------------------------------------
-- cdm_measurement
-- -------------------------------------------------------------------



-- -------------------------------------------------------------------
-- unique
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_measurement'          AS table_id,
    'unique'                            AS test_type, -- unique, not null, concept etc.
    'measurement_id'            AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(measurement_id) - COUNT(DISTINCT measurement_id) = 0) AS test_passed
FROM
    @etl_project.@etl_dataset.cdm_measurement
;

-- -------------------------------------------------------------------
-- FK to @etl_project.@etl_dataset.cdm_person.person_id
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_measurement'          AS table_id,
    'foreign key'                       AS test_type, -- unique, not null, concept etc.
    'person_id'                         AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(cdm.person_id) = 0)          AS test_passed -- FK source
FROM
    @etl_project.@etl_dataset.cdm_measurement cdm
LEFT JOIN
    @etl_project.@etl_dataset.cdm_person fk
        ON cdm.person_id = fk.person_id
WHERE
    fk.person_id IS NULL -- FK target
;

-- -------------------------------------------------------------------
-- FK to @etl_project.@etl_dataset.cdm_visit_occurrence.visit_occurrence_id
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_measurement'          AS table_id,
    'foreign key'                       AS test_type, -- unique, not null, concept etc.
    'visit_occurrence_id'                         AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(cdm.visit_occurrence_id) = 0)          AS test_passed -- FK source
FROM
    @etl_project.@etl_dataset.cdm_measurement cdm
LEFT JOIN
    @etl_project.@etl_dataset.cdm_visit_occurrence fk
        ON cdm.visit_occurrence_id = fk.visit_occurrence_id
WHERE
    fk.visit_occurrence_id IS NULL -- FK target
;

-- -------------------------------------------------------------------
-- cdm_device_exposure
-- -------------------------------------------------------------------



-- -------------------------------------------------------------------
-- unique
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_device_exposure'          AS table_id,
    'unique'                            AS test_type, -- unique, not null, concept etc.
    'device_exposure_id'            AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(device_exposure_id) - COUNT(DISTINCT device_exposure_id) = 0) AS test_passed
FROM
    @etl_project.@etl_dataset.cdm_device_exposure
;

-- -------------------------------------------------------------------
-- FK to @etl_project.@etl_dataset.cdm_person.person_id
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_device_exposure'          AS table_id,
    'foreign key'                       AS test_type, -- unique, not null, concept etc.
    'person_id'                         AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(cdm.person_id) = 0)          AS test_passed -- FK source
FROM
    @etl_project.@etl_dataset.cdm_device_exposure cdm
LEFT JOIN
    @etl_project.@etl_dataset.cdm_person fk
        ON cdm.person_id = fk.person_id
WHERE
    fk.person_id IS NULL -- FK target
;

-- -------------------------------------------------------------------
-- FK to @etl_project.@etl_dataset.cdm_visit_occurrence.visit_occurrence_id
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_device_exposure'          AS table_id,
    'foreign key'                       AS test_type, -- unique, not null, concept etc.
    'visit_occurrence_id'                         AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(cdm.visit_occurrence_id) = 0)          AS test_passed -- FK source
FROM
    @etl_project.@etl_dataset.cdm_device_exposure cdm
LEFT JOIN
    @etl_project.@etl_dataset.cdm_visit_occurrence fk
        ON cdm.visit_occurrence_id = fk.visit_occurrence_id
WHERE
    fk.visit_occurrence_id IS NULL -- FK target
;

-- -------------------------------------------------------------------
-- cdm_drug_exposure
-- -------------------------------------------------------------------



-- -------------------------------------------------------------------
-- unique
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_drug_exposure'          AS table_id,
    'unique'                            AS test_type, -- unique, not null, concept etc.
    'drug_exposure_id'            AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(drug_exposure_id) - COUNT(DISTINCT drug_exposure_id) = 0) AS test_passed
FROM
    @etl_project.@etl_dataset.cdm_drug_exposure
;

-- -------------------------------------------------------------------
-- FK to @etl_project.@etl_dataset.cdm_person.person_id
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_drug_exposure'          AS table_id,
    'foreign key'                       AS test_type, -- unique, not null, concept etc.
    'person_id'                         AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(cdm.person_id) = 0)          AS test_passed -- FK source
FROM
    @etl_project.@etl_dataset.cdm_drug_exposure cdm
LEFT JOIN
    @etl_project.@etl_dataset.cdm_person fk
        ON cdm.person_id = fk.person_id
WHERE
    fk.person_id IS NULL -- FK target
;

-- -------------------------------------------------------------------
-- FK to @etl_project.@etl_dataset.cdm_visit_occurrence.visit_occurrence_id
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_drug_exposure'          AS table_id,
    'foreign key'                       AS test_type, -- unique, not null, concept etc.
    'visit_occurrence_id'                         AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(cdm.visit_occurrence_id) = 0)          AS test_passed -- FK source
FROM
    @etl_project.@etl_dataset.cdm_drug_exposure cdm
LEFT JOIN
    @etl_project.@etl_dataset.cdm_visit_occurrence fk
        ON cdm.visit_occurrence_id = fk.visit_occurrence_id
WHERE
    fk.visit_occurrence_id IS NULL -- FK target
;

-- -------------------------------------------------------------------
-- cdm_condition_era
-- -------------------------------------------------------------------



-- -------------------------------------------------------------------
-- unique
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_condition_era'          AS table_id,
    'unique'                            AS test_type, -- unique, not null, concept etc.
    'condition_era_id'            AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(condition_era_id) - COUNT(DISTINCT condition_era_id) = 0) AS test_passed
FROM
    @etl_project.@etl_dataset.cdm_condition_era
;

-- -------------------------------------------------------------------
-- FK to @etl_project.@etl_dataset.cdm_person.person_id
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_condition_era'          AS table_id,
    'foreign key'                       AS test_type, -- unique, not null, concept etc.
    'person_id'                         AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(cdm.person_id) = 0)          AS test_passed -- FK source
FROM
    @etl_project.@etl_dataset.cdm_condition_era cdm
LEFT JOIN
    @etl_project.@etl_dataset.cdm_person fk
        ON cdm.person_id = fk.person_id
WHERE
    fk.person_id IS NULL -- FK target
;

-- -------------------------------------------------------------------
-- cdm_drug_era
-- -------------------------------------------------------------------



-- -------------------------------------------------------------------
-- unique
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_drug_era'          AS table_id,
    'unique'                            AS test_type, -- unique, not null, concept etc.
    'drug_era_id'            AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(drug_era_id) - COUNT(DISTINCT drug_era_id) = 0) AS test_passed
FROM
    @etl_project.@etl_dataset.cdm_drug_era
;

-- -------------------------------------------------------------------
-- FK to @etl_project.@etl_dataset.cdm_person.person_id
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_drug_era'          AS table_id,
    'foreign key'                       AS test_type, -- unique, not null, concept etc.
    'person_id'                         AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(cdm.person_id) = 0)          AS test_passed -- FK source
FROM
    @etl_project.@etl_dataset.cdm_drug_era cdm
LEFT JOIN
    @etl_project.@etl_dataset.cdm_person fk
        ON cdm.person_id = fk.person_id
WHERE
    fk.person_id IS NULL -- FK target
;

-- -------------------------------------------------------------------
-- cdm_dose_era
-- -------------------------------------------------------------------



-- -------------------------------------------------------------------
-- unique
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_dose_era'          AS table_id,
    'unique'                            AS test_type, -- unique, not null, concept etc.
    'dose_era_id'            AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(dose_era_id) - COUNT(DISTINCT dose_era_id) = 0) AS test_passed
FROM
    @etl_project.@etl_dataset.cdm_dose_era
;

-- -------------------------------------------------------------------
-- FK to @etl_project.@etl_dataset.cdm_person.person_id
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_dose_era'          AS table_id,
    'foreign key'                       AS test_type, -- unique, not null, concept etc.
    'person_id'                         AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(cdm.person_id) = 0)          AS test_passed -- FK source
FROM
    @etl_project.@etl_dataset.cdm_dose_era cdm
LEFT JOIN
    @etl_project.@etl_dataset.cdm_person fk
        ON cdm.person_id = fk.person_id
WHERE
    fk.person_id IS NULL -- FK target
;

-- -------------------------------------------------------------------
-- cdm_specimen
-- -------------------------------------------------------------------



-- -------------------------------------------------------------------
-- unique
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_specimen'          AS table_id,
    'unique'                            AS test_type, -- unique, not null, concept etc.
    'specimen_id'            AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(specimen_id) - COUNT(DISTINCT specimen_id) = 0) AS test_passed
FROM
    @etl_project.@etl_dataset.cdm_specimen
;

-- -------------------------------------------------------------------
-- FK to @etl_project.@etl_dataset.cdm_person.person_id
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_specimen'          AS table_id,
    'foreign key'                       AS test_type, -- unique, not null, concept etc.
    'person_id'                         AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(cdm.person_id) = 0)          AS test_passed -- FK source
FROM
    @etl_project.@etl_dataset.cdm_specimen cdm
LEFT JOIN
    @etl_project.@etl_dataset.cdm_person fk
        ON cdm.person_id = fk.person_id
WHERE
    fk.person_id IS NULL -- FK target
;

-- -------------------------------------------------------------------
-- cdm_fact_relationship
-- -------------------------------------------------------------------

