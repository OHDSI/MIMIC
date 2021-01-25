-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Unit tests for cdm_death table
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- FK to `@source_project`.@core_dataset.admissions.deathtime
-- -------------------------------------------------------------------

INSERT INTO `@metrics_project`.@metrics_dataset.report_unit_test
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_death'          AS table_id,
    'foreign key'                       AS test_type, -- unique, not null, concept etc.
    'death_date'                         AS field_name,
    CAST(NULL AS STRING)                AS criteria_json,
    (COUNT(cdm.death_date) = 0)          AS test_passed -- FK source
FROM
    `@etl_project`.@etl_dataset.cdm_death cdm
LEFT JOIN
(
    SELECT deathtime FROM `@source_project`.@core_dataset.admissions
    UNION DISTINCT
    SELECT dischtime FROM `@source_project`.@core_dataset.admissions
) fk
    ON cdm.death_date = fk.deathtime
WHERE
    fk.deathtime IS NULL -- FK target
;
