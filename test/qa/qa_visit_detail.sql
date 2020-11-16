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

-- -------------------------------------------------------------------
-- Rule 1, visit_detail.transfers
-- -------------------------------------------------------------------

INSERT INTO `@metrics_project`.@metrics_dataset.report_qa_test
SELECT
    CAST(NULL AS STRING)                AS report_id, -- task_id, run_id, target_dataset etc.
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_visit_detail'                  AS table_id,
    src.unit_id                         AS unit_id,
    CAST(NULL AS STRING)                AS field_name,
    'row_counts are equal in src and cdm'           AS test_description, -- count, ...
    CAST(NULL AS STRING)                AS condition_json, -- for some complex conditions
    (src.row_count - cdm.row_count = 0) AS test_passed
FROM
(
    SELECT 
        'visit_detail.transfers'    AS unit_id,
        COUNT(*)                    AS row_count
    FROM
        `@etl_project`.@etl_dataset.src_transfers tr
    WHERE 
        tr.eventtype != 'discharge' -- these are not useful
) src
LEFT JOIN
(
    SELECT unit_id, row_count
    FROM
        `@metrics_project`.@metrics_dataset.report_qa_row_count
    WHERE
        table_id = 'cdm_visit_detail'
        AND unit_id = 'visit_detail.transfers'
) cdm
    ON src.unit_id = cdm.unit_id
;

-- -------------------------------------------------------------------
-- Rule 2, visit_detail.admissions
-- -------------------------------------------------------------------

INSERT INTO `@metrics_project`.@metrics_dataset.report_qa_test
SELECT
    CAST(NULL AS STRING)                AS report_id, -- task_id, run_id, target_dataset etc.
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_visit_detail'              AS table_id,
    src.unit_id                         AS unit_id,
    CAST(NULL AS STRING)                AS field_name,
    'row_counts are equal in src and cdm'           AS test_description, -- count, ...
    CAST(NULL AS STRING)                AS condition_json, -- for some complex conditions
    (src.row_count - cdm.row_count = 0) AS test_passed
FROM
(
    SELECT 
        'visit_detail.admissions'    AS unit_id,
        COUNT(*)                    AS row_count
    FROM 
        `@etl_project`.@etl_dataset.src_admissions adm
    WHERE 
        adm.edregtime IS NOT NULL -- only those having a emergency timestamped
) src
LEFT JOIN
(
    SELECT unit_id, row_count
    FROM
        `@metrics_project`.@metrics_dataset.report_qa_row_count
    WHERE
        table_id = 'cdm_visit_detail'
        AND unit_id = 'visit_detail.admissions'
) cdm
    ON src.unit_id = cdm.unit_id
;

-- -------------------------------------------------------------------
-- Rule 3, visit_detail.services
-- -------------------------------------------------------------------

INSERT INTO `@metrics_project`.@metrics_dataset.report_qa_test
SELECT
    CAST(NULL AS STRING)                AS report_id, -- task_id, run_id, target_dataset etc.
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm_visit_detail'              AS table_id,
    src.unit_id                         AS unit_id,
    CAST(NULL AS STRING)                AS field_name,
    'row_counts are equal in src and cdm'           AS test_description, -- count, ...
    CAST(NULL AS STRING)                AS condition_json, -- for some complex conditions
    (src.row_count - cdm.row_count = 0) AS test_passed
FROM
(
    SELECT 
        'visit_detail.services'     AS unit_id,
        COUNT(*)                    AS row_count
    FROM 
        `@etl_project`.@etl_dataset.src_services serv
    INNER JOIN 
        `@target_project`.@target_dataset.cdm_visit_occurrence vis 
            ON CAST(serv.hadm_id AS STRING) = vis.visit_source_value
) src
LEFT JOIN
(
    SELECT unit_id, row_count
    FROM
        `@metrics_project`.@metrics_dataset.report_qa_row_count
    WHERE
        table_id = 'cdm_visit_detail'
        AND unit_id = 'visit_detail.services'
) cdm
    ON src.unit_id = cdm.unit_id
;

-- -------------------------------------------------------------------
-- count with filters
-- -------------------------------------------------------------------
