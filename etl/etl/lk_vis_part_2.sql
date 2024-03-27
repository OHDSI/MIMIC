-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Populate lookups for cdm_visit_occurrence and cdm_visit_detail
-- 
-- Dependencies: run after 
--      st_core.sql
--      lk_vis_part_1.sql
--      lk_meas_labevents.sql
--      lk_meas_specimen.sql
--      lk_meas_waveform.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- negative unique id from FARM_FINGERPRINT(GENERATE_UUID())
--
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- lk_visit_no_hadm_all
--
-- collect rows without hadm_id from all tables affected by this case:
--      lk_meas_labevents_mapped
--      lk_meas_organism_mapped
--      lk_meas_ab_mapped
--      lk_meas_waveform_mapped
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.lk_visit_no_hadm_all AS
-- labevents
SELECT
    src.subject_id                                  AS subject_id,
    CAST(src.start_datetime AS DATE)                AS date_id,
    src.start_datetime                              AS start_datetime,
    -- 
    src.unit_id                     AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    @etl_project.@etl_dataset.lk_meas_labevents_mapped src
WHERE
    src.hadm_id IS NULL
UNION ALL
-- specimen
SELECT
    src.subject_id                                  AS subject_id,
    CAST(src.start_datetime AS DATE)                AS date_id,
    src.start_datetime                              AS start_datetime,
    -- 
    src.unit_id                     AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    @etl_project.@etl_dataset.lk_specimen_mapped src
WHERE
    src.hadm_id IS NULL
UNION ALL
-- organism
SELECT
    src.subject_id                                  AS subject_id,
    CAST(src.start_datetime AS DATE)                AS date_id,
    src.start_datetime                              AS start_datetime,
    -- 
    src.unit_id                     AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    @etl_project.@etl_dataset.lk_meas_organism_mapped src
WHERE
    src.hadm_id IS NULL
UNION ALL
-- antibiotics
SELECT
    src.subject_id                                  AS subject_id,
    CAST(src.start_datetime AS DATE)                AS date_id,
    src.start_datetime                              AS start_datetime,
    -- 
    src.unit_id                     AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    @etl_project.@etl_dataset.lk_meas_ab_mapped src
WHERE
    src.hadm_id IS NULL
;
-- waveforms


-- -------------------------------------------------------------------
-- lk_visit_no_hadm_dist
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.lk_visit_no_hadm_dist AS
SELECT
    src.subject_id                                  AS subject_id,
    src.date_id                                     AS date_id,
    MIN(src.start_datetime)                         AS start_datetime,
    MAX(src.start_datetime)                         AS end_datetime,
    'AMBULATORY OBSERVATION'                        AS admission_type, -- outpatient visit
    CAST(NULL AS STRING)                            AS admission_location, -- to hospital
    CAST(NULL AS STRING)                            AS discharge_location, -- from hospital
    -- 
    'no_hadm'                       AS unit_id,
    'lk_visit_no_hadm_all'          AS load_table_id,
    0                               AS load_row_id,
    TO_JSON_STRING(STRUCT(
        src.subject_id AS subject_id,
        src.date_id AS date_id
    ))                                  AS trace_id
FROM
    @etl_project.@etl_dataset.lk_visit_no_hadm_all src
GROUP BY
    src.subject_id,
    src.date_id
;


-- -------------------------------------------------------------------
-- lk_visit_detail_waveform_dist
--
-- collect rows without hadm_id from all tables affected by this case:
--      lk_meas_waveform_mapped
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- lk_visit_clean
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.lk_visit_clean AS
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())               AS visit_occurrence_id,
    src.subject_id                                  AS subject_id,
    src.hadm_id                                     AS hadm_id,
    CAST(NULL AS DATE)                              AS date_id,
    src.start_datetime                              AS start_datetime,
    src.end_datetime                                AS end_datetime,
    src.admission_type                              AS admission_type, -- current location
    src.admission_location                          AS admission_location, -- to hospital
    src.discharge_location                          AS discharge_location, -- from hospital
    CONCAT(
        CAST(src.subject_id AS STRING), '|',
        CAST(src.hadm_id AS STRING)
    )                                               AS source_value,
    -- 
    src.unit_id                     AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    @etl_project.@etl_dataset.lk_admissions_clean src -- adm
UNION ALL
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())               AS visit_occurrence_id,
    src.subject_id                                  AS subject_id,
    CAST(NULL AS INT64)                             AS hadm_id,
    src.date_id                                     AS date_id,
    src.start_datetime                              AS start_datetime,
    src.end_datetime                                AS end_datetime,
    src.admission_type                              AS admission_type, -- current location
    src.admission_location                          AS admission_location, -- to hospital
    src.discharge_location                          AS discharge_location, -- from hospital
    CONCAT(
        CAST(src.subject_id AS STRING), '|',
        CAST(src.date_id AS STRING)
    )                                               AS source_value,
    -- 
    src.unit_id                     AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    @etl_project.@etl_dataset.lk_visit_no_hadm_dist src -- adm
;

-- -------------------------------------------------------------------
-- lk_visit_detail_clean
--
-- Rule 1. 
-- transfers with valid hadm_id
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.lk_visit_detail_clean AS
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())               AS visit_detail_id,
    src.subject_id                                  AS subject_id,
    src.hadm_id                                     AS hadm_id,
    src.date_id                                     AS date_id,
    src.start_datetime                              AS start_datetime,
    src.end_datetime                                AS end_datetime,  -- if null, populate with next start_datetime
    CONCAT(
        CAST(src.subject_id AS STRING), '|',
        COALESCE(CAST(src.hadm_id AS STRING), CAST(src.date_id AS STRING)), '|',
        CAST(src.transfer_id AS STRING)
    )                                               AS source_value,
    src.current_location                            AS current_location, -- find prev and next for adm and disch location
    -- 
    src.unit_id                     AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM 
    @etl_project.@etl_dataset.lk_transfers_clean src
WHERE
    src.hadm_id IS NOT NULL -- some ER transfers are excluded because not all of them fit to additional single day visits
;

-- -------------------------------------------------------------------
-- lk_visit_detail_clean
--
-- Rule 2.
-- ER admissions
-- -------------------------------------------------------------------
INSERT INTO @etl_project.@etl_dataset.lk_visit_detail_clean
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())               AS visit_detail_id,
    src.subject_id                                  AS subject_id,
    src.hadm_id                                     AS hadm_id,
    CAST(src.start_datetime AS DATE)                AS date_id,
    src.start_datetime                              AS start_datetime,
    CAST(NULL AS DATETIME)                          AS end_datetime,  -- if null, populate with next start_datetime
    CONCAT(
        CAST(src.subject_id AS STRING), '|',
        CAST(src.hadm_id AS STRING)
    )                                               AS source_value,
    src.admission_type                              AS current_location, -- find prev and next for adm and disch location
    -- 
    src.unit_id                     AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM 
    @etl_project.@etl_dataset.lk_admissions_clean src
WHERE
    src.is_er_admission
;

-- -------------------------------------------------------------------
-- lk_visit_detail_clean
--
-- Rule 3.
-- services
-- -------------------------------------------------------------------
INSERT INTO @etl_project.@etl_dataset.lk_visit_detail_clean
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())               AS visit_detail_id,
    src.subject_id                                  AS subject_id,
    src.hadm_id                                     AS hadm_id,
    CAST(src.start_datetime AS DATE)                AS date_id,
    src.start_datetime                              AS start_datetime,
    src.end_datetime                                AS end_datetime,
    CONCAT(
        CAST(src.subject_id AS STRING), '|',
        CAST(src.hadm_id AS STRING), '|',
        CAST(src.start_datetime AS STRING)
    )                                               AS source_value,
    src.curr_service                                AS current_location,
    -- 
    src.unit_id                     AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM 
    @etl_project.@etl_dataset.lk_services_clean src
WHERE
    src.prev_service = src.lag_service -- ensure that the services sequence is still consistent after removing duplicates
;

-- -------------------------------------------------------------------
-- lk_visit_detail_clean
--
-- Rule 4.
-- waveforms
-- -------------------------------------------------------------------


-- -------------------------------------------------------------------
-- lk_visit_detail_prev_next
-- skip "mapped"
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.lk_visit_detail_prev_next AS
SELECT 
    src.visit_detail_id                             AS visit_detail_id,
    src.subject_id                                  AS subject_id,
    src.hadm_id                                     AS hadm_id,
    src.date_id                                     AS date_id,
    src.start_datetime                              AS start_datetime,
    COALESCE(
        src.end_datetime,
        LEAD(src.start_datetime) OVER (
            PARTITION BY src.subject_id, src.hadm_id, src.date_id
            ORDER BY src.start_datetime ASC
        ),
        vis.end_datetime
    )                                               AS end_datetime,
    src.source_value                                AS source_value,
    --
    src.current_location                            AS current_location,
    LAG(src.visit_detail_id) OVER (
        PARTITION BY src.subject_id, src.hadm_id, src.date_id, src.unit_id
        ORDER BY src.start_datetime ASC
    )                                                AS preceding_visit_detail_id,
    COALESCE(
        LAG(src.current_location) OVER (
            PARTITION BY src.subject_id, src.hadm_id, src.date_id, src.unit_id -- double-check if chains follow each other or intercept
            ORDER BY src.start_datetime ASC
        ),
        vis.admission_location
    )                                               AS admission_location,
    COALESCE(
        LEAD(src.current_location) OVER (
            PARTITION BY src.subject_id, src.hadm_id, src.date_id, src.unit_id
            ORDER BY src.start_datetime ASC
        ),
        vis.discharge_location
    )                                               AS discharge_location,
    --
    src.unit_id                       AS unit_id,
    src.load_table_id                 AS load_table_id,
    src.load_row_id                   AS load_row_id,
    src.trace_id                      AS trace_id
FROM 
    @etl_project.@etl_dataset.lk_visit_detail_clean src
LEFT JOIN 
    @etl_project.@etl_dataset.lk_visit_clean vis
        ON  src.subject_id = vis.subject_id
        AND (
            src.hadm_id = vis.hadm_id
            OR src.hadm_id IS NULL AND src.date_id = vis.date_id
        )
;


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

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.lk_visit_concept AS
SELECT 
    vc.concept_code     AS source_code,
    vc.concept_id       AS source_concept_id,
    vc2.concept_id      AS target_concept_id,
    vc.vocabulary_id    AS source_vocabulary_id
FROM 
    @etl_project.@etl_dataset.voc_concept vc
LEFT JOIN
    @etl_project.@etl_dataset.voc_concept_relationship vcr
        ON  vc.concept_id = vcr.concept_id_1 
        and vcr.relationship_id = 'Maps to'
LEFT JOIN
    @etl_project.@etl_dataset.voc_concept vc2
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
