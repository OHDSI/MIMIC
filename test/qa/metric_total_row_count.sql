-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Total row count by tables for QA and Metric reports(tbd)
--
-- Dependencies: run before other qa tests
-- -------------------------------------------------------------------

-- duration on Demo
-- Person, Care_site: 7.1 sec

-- -------------------------------------------------------------------
-- Total row count table
-- unique:
--      table_subset_id || unit_id
--      table_id || unit_id
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.report_qa_row_count AS
SELECT
    CAST(NULL AS STRING)                AS report_id, -- task_id, run_id, target_dataset etc.
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    CAST(NULL AS STRING)                AS table_subset_id, -- source, cdm, lk(?), src_core, src_hadm?
    CAST(NULL AS STRING)                AS table_id,
    CAST(NULL AS STRING)                AS unit_id,
    CAST(NULL AS INT64)                 AS row_count
;

-- -------------------------------------------------------------------
-- src_patients
-- -------------------------------------------------------------------

INSERT INTO `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.report_qa_row_count
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'src'                               AS table_subset_id,
    'src_patients'                      AS table_id,
    'person.patients'                   AS unit_id,
    COUNT(*)                            AS row_count
FROM
    `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.src_patients
;

-- -------------------------------------------------------------------
-- cdm_person
-- -------------------------------------------------------------------

INSERT INTO `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.report_qa_row_count
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm'                               AS table_subset_id,
    'cdm_person'                        AS table_id,
    'person.patients'                   AS unit_id,
    COUNT(*)                            AS row_count
FROM
    `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.cdm_person
;

-- -------------------------------------------------------------------
-- src_transfers (careunit)
-- -------------------------------------------------------------------

INSERT INTO `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.report_qa_row_count
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'src'                               AS table_subset_id,
    'src_transfers'                     AS table_id,
    'care_site.transfers'               AS unit_id,
    COUNT(DISTINCT careunit)            AS row_count
FROM
    `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.src_transfers
;

-- -------------------------------------------------------------------
-- cdm_care_site
-- -------------------------------------------------------------------

INSERT INTO `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.report_qa_row_count
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm'                               AS table_subset_id,
    'cdm_care_site'                     AS table_id,
    'care_site.transfers'               AS unit_id,
    COUNT(*)                            AS row_count
FROM
    `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.cdm_care_site
;

-- -------------------------------------------------------------------
-- cdm_visit_occurrence
-- -------------------------------------------------------------------

INSERT INTO `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.report_qa_row_count
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm'                               AS table_subset_id,
    'cdm_visit_occurrence'              AS table_id,
    'visit.admissions'                  AS unit_id,
    COUNT(*)                            AS row_count
FROM
    `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.cdm_visit_occurrence
;

-- -------------------------------------------------------------------
-- src_admissions
-- -------------------------------------------------------------------

INSERT INTO `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.report_qa_row_count
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'src'                               AS table_subset_id,
    'src_admissions'                    AS table_id,
    'visit.admissions'                  AS unit_id,
    COUNT(*)                            AS row_count
FROM
    `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.src_admissions
;

-- -------------------------------------------------------------------
-- cdm_visit_detail
-- -------------------------------------------------------------------

INSERT INTO `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.report_qa_row_count
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm'                               AS table_subset_id,
    'cdm_visit_detail'                  AS table_id,
    unit_id                             AS unit_id,
    COUNT(*)                            AS row_count
FROM
    `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.cdm_visit_detail
GROUP BY
    unit_id
;

-- -------------------------------------------------------------------
-- src_diagnoses_icd
-- -------------------------------------------------------------------

INSERT INTO `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.report_qa_row_count
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'src'                               AS table_subset_id,
    'src_diagnoses_icd'                 AS table_id,
    'condition.diagnoses_icd'           AS unit_id,
    COUNT(*)                            AS row_count
FROM
    `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.src_diagnoses_icd
;

-- -------------------------------------------------------------------
-- cdm_condition_occurrence
-- -------------------------------------------------------------------

INSERT INTO `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.report_qa_row_count
SELECT
    CAST(NULL AS STRING)                AS report_id,
    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS
    'cdm'                               AS table_subset_id,
    'cdm_condition_occurrence'          AS table_id,
    unit_id                             AS unit_id,
    COUNT(*)                            AS row_count
FROM
    `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.cdm_condition_occurrence
GROUP BY
    unit_id
;
