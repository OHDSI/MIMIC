-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- standard_concept - allows 0 param
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Unit tests report table
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @metrics_project.@metrics_dataset.report_unit_test AS
SELECT
    CAST(NULL AS STRING)                AS report_id, -- task_id, run_id, target_dataset etc.
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    CAST(NULL AS STRING)                AS table_id,
    CAST(NULL AS STRING)                AS test_type, -- unique, not null, concept etc.
    CAST(NULL AS STRING)                AS field_name,
    CAST(NULL AS STRING)                AS criteria_json, -- for vocabulary, domain, range and FK
    CAST(NULL AS BOOL)                  AS test_passed
;

-- -------------------------------------------------------------------
-- Unit tests for cdm_... - template for individual tables
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- unique
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- not null
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- standard_concept
-- -------------------------------------------------------------------

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
