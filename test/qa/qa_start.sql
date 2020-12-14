-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- add field sql_text
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- QA tests report table
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@metrics_project`.@metrics_dataset.report_qa_test AS
SELECT
    CAST(NULL AS STRING)                AS report_id, -- task_id, run_id, target_dataset etc.
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    CAST(NULL AS STRING)                AS table_id,
    CAST(NULL AS STRING)                AS unit_id,
    CAST(NULL AS STRING)                AS field_name,
    CAST(NULL AS STRING)                AS test_description, -- count, ...
    CAST(NULL AS STRING)                AS criteria_json, -- for some complex conditions
    CAST(NULL AS BOOL)                  AS test_passed
;

-- -------------------------------------------------------------------
-- QA tests for cdm_... - template for individual tables
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- count by trace_id
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- count with filters
-- -------------------------------------------------------------------
