-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Populate lookups for cdm_observation table
-- 
-- Dependencies: run after 
--      st_core.sql,
--      st_hosp.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize create or replace
--
-- loaded custom mapping:
--      gcpt_insurance_to_concept -> mimiciv_obs_insurance
--      gcpt_marital_status_to_concept -> mimiciv_obs_marital
--      gcpt_drgcode_to_concept -> mimiciv_obs_drgcodes
--          source_code = gcpt.description
-- Cost containment drgcode should be in cost table apparently.... 
--      http://forums.ohdsi.org/t/most-appropriate-omop-table-to-house-drg-information/1591/9,
-- observation.proc.* (Achilless Heel report)
--      value_as_string IS NULL AND value_as_number IS NULL AND COALESCE(value_as_concept_id, 0) = 0
--      review custom mapping. if ok, use value_as_concept_id = 4188539 'Yes'?
-- -------------------------------------------------------------------

-- on demo: 1585 rows
-- -------------------------------------------------------------------
-- lk_observation_clean from admissions
-- rules 1-3
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.lk_observation_clean AS
-- rule 1, insurance
SELECT
    src.subject_id                  AS subject_id,
    src.hadm_id                     AS hadm_id,
    'Insurance'                     AS source_code,
    46235654                        AS target_concept_id, -- Primary insurance,
    src.admittime                   AS start_datetime,
    src.insurance                   AS value_as_string,
    'mimiciv_obs_insurance'         AS source_vocabulary_id,
    --
    'admissions.insurance'          AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    @etl_project.@etl_dataset.src_admissions src -- adm
WHERE
    src.insurance IS NOT NULL

UNION ALL
-- rule 2, marital_status
SELECT
    src.subject_id                  AS subject_id,
    src.hadm_id                     AS hadm_id,
    'Marital status'                AS source_code,
    40766231                        AS target_concept_id, -- Marital status,
    src.admittime                   AS start_datetime,
    src.marital_status              AS value_as_string,
    'mimiciv_obs_marital'           AS source_vocabulary_id,
    --
    'admissions.marital_status'     AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    @etl_project.@etl_dataset.src_admissions src -- adm
WHERE
    src.marital_status IS NOT NULL

UNION ALL
-- rule 3, language
SELECT
    src.subject_id                  AS subject_id,
    src.hadm_id                     AS hadm_id,
    'Language'                      AS source_code,
    40758030                        AS target_concept_id, -- Preferred language
    src.admittime                   AS start_datetime,
    src.language                    AS value_as_string,
    'mimiciv_obs_language'          AS source_vocabulary_id,
    --
    'admissions.language'           AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    @etl_project.@etl_dataset.src_admissions src -- adm
WHERE
    src.language IS NOT NULL
;

-- -------------------------------------------------------------------
-- lk_observation_clean
-- Rule 4, drgcodes
-- -------------------------------------------------------------------

INSERT INTO @etl_project.@etl_dataset.lk_observation_clean
SELECT
    src.subject_id                  AS subject_id,
    src.hadm_id                     AS hadm_id,
    -- 'DRG code' AS source_code,
    src.drg_code                    AS source_code,
    4296248                         AS target_concept_id, -- Cost containment
    COALESCE(adm.edregtime, adm.admittime)  AS start_datetime,
    src.description                 AS value_as_string,
    'mimiciv_obs_drgcodes'          AS source_vocabulary_id,
    --
    'drgcodes.description'          AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    @etl_project.@etl_dataset.src_drgcodes src -- drg
INNER JOIN
    @etl_project.@etl_dataset.src_admissions adm
        ON src.hadm_id = adm.hadm_id
WHERE
    src.description IS NOT NULL
;


-- on demo: 270 rows
-- -------------------------------------------------------------------
-- lk_obs_admissions_concept
-- Rules 1-4
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.lk_obs_admissions_concept AS
SELECT DISTINCT
    src.value_as_string         AS source_code,
    src.source_vocabulary_id    AS source_vocabulary_id,
    vc.domain_id                AS source_domain_id,
    vc.concept_id               AS source_concept_id,
    vc2.domain_id               AS target_domain_id,
    vc2.concept_id              AS target_concept_id
FROM
    @etl_project.@etl_dataset.lk_observation_clean src
LEFT JOIN
    @etl_project.@etl_dataset.voc_concept vc
        ON src.value_as_string = vc.concept_code
        AND src.source_vocabulary_id = vc.vocabulary_id
        -- valid period should be used to map drg_code, but due to the date shift it is not applicable
        -- AND src.start_datetime BETWEEN vc.valid_start_date AND vc.valid_end_date
LEFT JOIN
    @etl_project.@etl_dataset.voc_concept_relationship vcr
        ON  vc.concept_id = vcr.concept_id_1
        AND vcr.relationship_id = 'Maps to'
LEFT JOIN
    @etl_project.@etl_dataset.voc_concept vc2
        ON vc2.concept_id = vcr.concept_id_2
        AND vc2.standard_concept = 'S'
        AND vc2.invalid_reason IS NULL
;

-- -------------------------------------------------------------------
-- lk_observation_mapped
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.lk_observation_mapped AS
SELECT
    src.hadm_id                             AS hadm_id, -- to visit
    src.subject_id                          AS subject_id, -- to person
    COALESCE(src.target_concept_id, 0)      AS target_concept_id,
    src.start_datetime                      AS start_datetime,
    32817                                   AS type_concept_id, -- OMOP4976890 EHR, -- Rules 1-4
    src.source_code                         AS source_code,
    0                                       AS source_concept_id,
    src.value_as_string                     AS value_as_string,
    lc.target_concept_id                    AS value_as_concept_id,
    'Observation'                           AS target_domain_id, -- to join on src.target_concept_id?
    --
    src.unit_id                     AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    @etl_project.@etl_dataset.lk_observation_clean src
LEFT JOIN
    @etl_project.@etl_dataset.lk_obs_admissions_concept lc
        ON src.value_as_string = lc.source_code
        AND src.source_vocabulary_id = lc.source_vocabulary_id
;
