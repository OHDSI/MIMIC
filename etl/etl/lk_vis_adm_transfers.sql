-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Populate lookups for cdm_visit_occurrence and cdm_visit_detail
-- 
-- Dependencies: run after 
--      st_core.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize create or replace
-- negative unique id from FARM_FINGERPRINT(GENERATE_UUID())
--
-- custom mapping 
--      Both visit_detail_concept_id and place_of_service_concept_id accept concepts from Visit domain. 
--          Then we can use the same mapping for both fields.
--      gcpt_admission_type_to_concept -> mimiciv_vis_admission_type
--      gcpt_admission_location_to_concept -> mimiciv_vis_admission_location
--      gcpt_discharge_location_to_concept -> mimiciv_vis_discharge_location
--      gcpt_care_site -> mimiciv_cs_place_of_service
--      brand new vocabulary -> mimiciv_vis_service -- to map
--
-- src.callout - is there any derived table in MIMIC IV?
--
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- lk_visit_clean
-- skip "mapped"
--
-- All to visit_occurrence
-- ER admissions to visit_detail too
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_visit_clean AS
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())               AS visit_occurrence_id,
    src.subject_id                                  AS subject_id,
    src.hadm_id                                     AS hadm_id,
    IF(src.edregtime < src.admittime,
        src.edregtime, src.admittime
    )                                               AS start_datetime, -- the earliest of
    src.dischtime                                   AS end_datetime,
    src.admission_type                              AS admission_type, -- current location
    src.admission_location                          AS admission_location, -- to hospital
    src.discharge_location                          AS discharge_location, -- from hospital
    IF(src.edregtime IS NULL, FALSE, TRUE)          AS is_er_admission, -- create visit_detail if TRUE
    IF(src.deathtime IS NULL, FALSE, TRUE)          AS could_be_post_mortem_donor, 
                                                            -- instead of analysing diagnosis in III?
    CONCAT(
        CAST(src.subject_id AS STRING), '|',
        COALESCE(CAST(src.hadm_id AS STRING), 'None')
    )                                               AS source_value,
    -- 
    'admissions'                    AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    `@etl_project`.@etl_dataset.src_admissions src -- adm
;

-- -------------------------------------------------------------------
-- lk_visit_detail_clean
--
-- Rule 1. 
-- from transfers without discharges to visit_detail
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_visit_detail_clean AS
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())               AS visit_detail_id,
    src.subject_id                                  AS subject_id,
    src.hadm_id                                     AS hadm_id,
    src.intime                                      AS start_datetime,
    src.outtime                                     AS end_datetime, -- if null, populate with adm.dischtime (below)
    CONCAT(
        CAST(src.subject_id AS STRING), '|',
        COALESCE(CAST(src.hadm_id AS STRING), 'None'), '|',
        CAST(src.transfer_id AS STRING)
    )                                               AS source_value,
    src.careunit                                    AS current_location, -- find prev and next for adm and disch location
    -- 
    'transfers'                     AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM 
    `@etl_project`.@etl_dataset.src_transfers src
WHERE 
    src.eventtype != 'discharge' -- these are not useful
;

-- -------------------------------------------------------------------
-- lk_visit_detail_clean
--
-- Rule 2.
-- ER admissions
-- -------------------------------------------------------------------

INSERT INTO `@etl_project`.@etl_dataset.lk_visit_detail_clean
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())               AS visit_detail_id,
    src.subject_id                                  AS subject_id,
    src.hadm_id                                     AS hadm_id,
    src.start_datetime                              AS start_datetime,
    CAST(NULL AS DATETIME)                          AS end_datetime,  -- if null, populate with next start_datetime
    CONCAT(
        CAST(src.subject_id AS STRING), '|',
        COALESCE(CAST(src.hadm_id AS STRING), 'None')
    )                                               AS source_value,
    src.admission_type                              AS current_location, -- find prev and next for adm and disch location
    -- 
    'admissions'                    AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM 
    `@etl_project`.@etl_dataset.lk_visit_clean src
WHERE
    src.is_er_admission
;

-- -------------------------------------------------------------------
-- lk_visit_detail_clean
--
-- Rule 3.
-- SERVICES information
-- -------------------------------------------------------------------

INSERT INTO `@etl_project`.@etl_dataset.lk_visit_detail_clean
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())               AS visit_detail_id,
    src.subject_id                                  AS subject_id,
    src.hadm_id                                     AS hadm_id,
    src.transfertime                                AS start_datetime,
    LEAD(src.transfertime) OVER (
        PARTITION BY src.subject_id, src.hadm_id 
        ORDER BY src.transfertime
    )                                               AS end_datetime,  -- if null, populate with adm.dischtime (below)
    CONCAT(
        CAST(src.subject_id AS STRING), '|',
        COALESCE(CAST(src.hadm_id AS STRING), 'None'), '|',
        CAST(src.transfertime AS STRING)
    )                                               AS source_value,
    src.curr_service                                AS current_location, -- find prev and next for adm and disch location
    -- 
    'services'                      AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM 
    `@etl_project`.@etl_dataset.src_services src
;

-- -------------------------------------------------------------------
-- lk_visit_detail_prev_next
-- skip "mapped"
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_visit_detail_prev_next AS
SELECT 
    src.visit_detail_id                             AS visit_detail_id,
    src.subject_id                                  AS subject_id,
    src.hadm_id                                     AS hadm_id,
    src.start_datetime                              AS start_datetime,
    COALESCE(
        src.end_datetime,
        LEAD(src.start_datetime) OVER (
            PARTITION BY src.subject_id, src.hadm_id -- should be transfer, but can we encouter service?
            ORDER BY src.start_datetime ASC
        ),
        adm.end_datetime
    )                                               AS end_datetime,
    src.source_value                                AS source_value,
    --
    src.current_location                            AS current_location,
    LAG(src.visit_detail_id) OVER (
        PARTITION BY src.subject_id, src.hadm_id, src.unit_id
        ORDER BY src.start_datetime ASC
    )                                                AS preceding_visit_detail_id,
    COALESCE(
        LAG(src.current_location) OVER (
            PARTITION BY src.subject_id, src.hadm_id, src.unit_id -- double-check if chains follow each other or intercept
            ORDER BY src.start_datetime ASC
        ),
        adm.admission_location
    )                                               AS admission_location,
    COALESCE(
        LEAD(src.current_location) OVER (
            PARTITION BY src.subject_id, src.hadm_id, src.unit_id
            ORDER BY src.start_datetime ASC
        ),
        adm.discharge_location
    )                                               AS discharge_location,
    --
    src.unit_id                       AS unit_id,
    src.load_table_id                 AS load_table_id,
    src.load_row_id                   AS load_row_id,
    src.trace_id                      AS trace_id
FROM 
    `@etl_project`.@etl_dataset.lk_visit_detail_clean src
LEFT JOIN 
    `@etl_project`.@etl_dataset.lk_visit_clean adm
        ON  src.subject_id = adm.subject_id
        AND src.hadm_id = adm.hadm_id
;

-- -------------------------------------------------------------------
-- additional visit for visit_detail without hadm_id
-- or missing from admissions
-- -------------------------------------------------------------------

-- INSERT INTO `@etl_project`.@etl_dataset.lk_visit_clean
-- SELECT
--     FARM_FINGERPRINT(GENERATE_UUID())               AS visit_occurrence_id,
--     src.subject_id                                  AS subject_id,
--     src.hadm_id                                     AS hadm_id,
--     src.start_datetime                              AS start_datetime, -- the earliest of
--     src.end_datetime                                AS end_datetime,
--     src.current_location                            AS admission_type, -- current location
--     src.admission_location                          AS admission_location, -- to hospital
--     src.discharge_location                          AS discharge_location, -- from hospital
--     NULL                                            AS is_er_admission, -- create visit_detail if TRUE
--     NULL                                            AS could_be_post_mortem_donor, 
--                                                             -- instead of analysing diagnosis in III?
--     CONCAT(
--         CAST(src.subject_id AS STRING), '|',
--         COALESCE(CAST(src.hadm_id AS STRING), 'None')
--     )                                               AS source_value,
--     -- 
--     src.unit_id                     AS unit_id,
--     src.load_table_id               AS load_table_id,
--     src.load_row_id                 AS load_row_id,
--     src.trace_id                    AS trace_id
-- FROM
--     `@etl_project`.@etl_dataset.lk_visit_detail_prev_next src
-- LEFT JOIN
--     `@etl_project`.@etl_dataset.src_admissions adm
--         ON src.hadm_id = adm.hadm_id 
--         AND src.subject_id = adm.subject_id
-- WHERE
--     adm.hadm_id IS NULL


-- -------------------------------------------------------------------
-- lk_visit_concept
--
-- gcpt_admission_type_to_concept -> mimiciv_vis_admission_type
-- gcpt_admission_location_to_concept -> mimiciv_vis_admission_location
-- gcpt_discharge_location_to_concept -> mimiciv_vis_discharge_location
-- brand new vocabulary -> mimiciv_vis_service
-- gcpt_care_site -> mimiciv_cs_place_of_service
--
-- keep exact values of admission type etc as custom concepts, 
-- then map it to standard Visit concepts
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_visit_concept AS
SELECT 
    vc.concept_code     AS source_code,
    vc.concept_id       AS source_concept_id,
    vc2.concept_id      AS target_concept_id,
    vc.vocabulary_id    AS source_vocabulary_id
FROM 
    `@etl_project`.@etl_dataset.voc_concept vc
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept_relationship vcr
        ON  vc.concept_id = vcr.concept_id_1 
        and vcr.relationship_id = 'Maps to'
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept vc2
        ON vc2.concept_id = vcr.concept_id_2
        AND vc2.standard_concept = 'S'
        AND vc2.invalid_reason IS NULL
WHERE
    vc.vocabulary_id IN (
        'mimiciv_vis_admission_location',   -- for admission_location_concept_id (visit and visit_detail)
        'mimiciv_vis_discharge_location',   -- for discharge_location_concept_id 
        'mimiciv_vis_service',              -- for admisstion_location_concept_id (visit_detail)
                                            -- and for discharge_location_concept_id 
        'mimiciv_vis_admission_type',       -- for visit_concept_id
        'mimiciv_cs_place_of_service'       -- for visit_detail_concept_id
    )
;
