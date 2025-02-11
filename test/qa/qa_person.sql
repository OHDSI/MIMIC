-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- QA tests for cdm_person
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- count (count by trace_id?)
-- -------------------------------------------------------------------

INSERT INTO @metrics_project.@metrics_dataset.report_qa_test
SELECT
    CAST(NULL AS STRING)                AS report_id, -- task_id, run_id, target_dataset etc.
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_person'                        AS table_id,
    src.unit_id                         AS unit_id,
    CAST(NULL AS STRING)                AS field_name,
    'row_counts are equal in src and cdm'           AS test_description, -- count, ...
    CAST(NULL AS STRING)                AS condition_json, -- for some complex conditions
    (src.row_count - cdm.row_count = 0) AS test_passed
FROM
(
    SELECT unit_id, row_count
    FROM
        @metrics_project.@metrics_dataset.report_qa_row_count
    WHERE
        table_id = 'src_patients'
) src
LEFT JOIN
(
    SELECT unit_id, row_count
    FROM
        @metrics_project.@metrics_dataset.report_qa_row_count
    WHERE
        table_id = 'cdm_person'
) cdm
    ON src.unit_id = cdm.unit_id
;


-- -------------------------------------------------------------------
-- count with filters
-- -------------------------------------------------------------------
