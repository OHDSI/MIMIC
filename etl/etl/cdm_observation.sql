-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Populate cdm_observation table
-- 
-- Dependencies: run after 
--      st_core.sql,
--      st_hosp.sql,
--      cdm_person.sql,
--      cdm_visit_occurrence,
--      cdm_visit_detail(?)
--      cdm_procedure_occurrence
--
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
-- src.unit_id         AS source_code, -- to add lk_observation_clean.source_code
-- Cost containment drgcode should be in cost table apparently.... 
--      http://forums.ohdsi.org/t/most-appropriate-omop-table-to-house-drg-information/1591/9,
-- Chartevents.text
--      to add from Observation III (see the draft below)
-- -------------------------------------------------------------------

-- on demo: 1585 rows
-- -------------------------------------------------------------------
-- lk_observation_clean from admissions
-- rules 1-3
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_observation_clean AS
-- rule 1, insurance
SELECT
    src.subject_id                  AS subject_id,
    src.hadm_id                     AS hadm_id,
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
    `@etl_project`.@etl_dataset.src_admissions src -- adm
WHERE
    src.insurance IS NOT NULL

UNION ALL
-- rule 2, marital_status
SELECT
    src.subject_id                  AS subject_id,
    src.hadm_id                     AS hadm_id,
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
    `@etl_project`.@etl_dataset.src_admissions src -- adm
WHERE
    src.marital_status IS NOT NULL

UNION ALL
-- rule 3, language
SELECT
    src.subject_id                  AS subject_id,
    src.hadm_id                     AS hadm_id,
    40758030                        AS target_concept_id, -- Language.preferred,
    src.admittime                   AS start_datetime,
    src.language                    AS value_as_string,
    CAST(NULL AS STRING)            AS source_vocabulary_id,
    --
    'admissions.language'           AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    `@etl_project`.@etl_dataset.src_admissions src -- adm
WHERE
    src.language IS NOT NULL
;

-- -------------------------------------------------------------------
-- lk_observation_clean
-- Rule 4, drgcodes
-- -------------------------------------------------------------------

INSERT INTO `@etl_project`.@etl_dataset.lk_observation_clean
SELECT
    src.subject_id                  AS subject_id,
    src.hadm_id                     AS hadm_id,
    4296248                         AS target_concept_id, -- Cost containment drgcode should be in cost table apparently...
    COALESCE(adm.edregtime, adm.admittime)  AS start_datetime,
    src.description                 AS value_as_string,
    'mimiciv_obs_drgcodes'          AS source_vocabulary_id,
    --
    'drgcodes.description'          AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    `@etl_project`.@etl_dataset.src_drgcodes src -- drg
INNER JOIN
    `@etl_project`.@etl_dataset.src_admissions adm
        ON src.hadm_id = adm.hadm_id
;


-- on demo: 270 rows
-- -------------------------------------------------------------------
-- lk_obs_admissions_concept (to rename table)
-- Rules 1-4
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_obs_admissions_concept AS
SELECT DISTINCT
    src.value_as_string         AS source_code,
    src.source_vocabulary_id    AS source_vocabulary_id,
    vc.domain_id                AS source_domain_id,
    vc.concept_id               AS source_concept_id,
    vc2.domain_id               AS target_domain_id,
    vc2.concept_id              AS target_concept_id
FROM
    `@etl_project`.@etl_dataset.lk_observation_clean src
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept vc
        ON src.value_as_string = vc.concept_code
        AND src.source_vocabulary_id = vc.vocabulary_id
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept_relationship vcr
        ON  vc.concept_id = vcr.concept_id_1
        AND vcr.relationship_id = 'Maps to'
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept vc2
        ON vc2.concept_id = vcr.concept_id_2
        AND vc2.standard_concept = 'S'
        AND vc2.invalid_reason IS NULL
;



-- -------------------------------------------------------------------
-- chartevents
-- Rule 5
-- to do, see cdm_observation_draft.sql
-- 581413 AS observation_type_concept_id -- Observation from Measurement,
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- lk_observation_mapped
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_observation_mapped AS
SELECT
    src.hadm_id                             AS hadm_id, -- to visit
    src.subject_id                          AS subject_id, -- to person
    COALESCE(src.target_concept_id, 0)      AS target_concept_id,
    src.start_datetime                      AS start_datetime,
    38000280                                AS type_concept_id, -- Observation recorded from EHR, -- Rules 1-4
    src.unit_id         AS source_code, -- to add lk_observation_clean.source_code
    0                                       AS source_concept_id,
    src.value_as_string                     AS value_as_string,
    lc.target_concept_id                    AS value_as_concept_id,
    -- visit_detail_assign.visit_detail_id     AS visit_detail_id,
    CAST(NULL AS INT64)                     AS visit_detail_id,
    'Observation'                           AS target_domain_id, -- to join on src.target_concept_id?
    --
    src.unit_id                     AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    `@etl_project`.@etl_dataset.lk_observation_clean src
LEFT JOIN
    `@etl_project`.@etl_dataset.lk_obs_admissions_concept lc
        ON src.value_as_string = lc.source_code
        AND src.source_vocabulary_id = lc.source_vocabulary_id
;

-- -------------------------------------------------------------------
-- cdm_observation
-- -------------------------------------------------------------------

--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.cdm_observation
(
    observation_id                INT64     not null ,
    person_id                     INT64     not null ,
    observation_concept_id        INT64     not null ,
    observation_date              DATE      not null ,
    observation_datetime          DATETIME           ,
    observation_type_concept_id   INT64     not null ,
    value_as_number               FLOAT64        ,
    value_as_string               STRING         ,
    value_as_concept_id           INT64          ,
    qualifier_concept_id          INT64          ,
    unit_concept_id               INT64          ,
    provider_id                   INT64          ,
    visit_occurrence_id           INT64          ,
    visit_detail_id               INT64          ,
    observation_source_value      STRING         ,
    observation_source_concept_id INT64          ,
    unit_source_value             STRING         ,
    qualifier_source_value        STRING         ,
    -- 
    unit_id                       STRING,
    load_table_id                 STRING,
    load_row_id                   INT64,
    trace_id                      STRING
)
;


-- -------------------------------------------------------------------
-- from lk_observation_mapped
-- Rules 1-4
-- -------------------------------------------------------------------

INSERT INTO `@etl_project`.@etl_dataset.cdm_observation
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())           AS observation_id,
    per.person_id                               AS person_id,
    src.target_concept_id                       AS observation_concept_id,
    CAST(src.start_datetime AS DATE)            AS observation_date,
    src.start_datetime                          AS observation_datetime,
    src.type_concept_id                         AS observation_type_concept_id,
    CAST(NULL AS FLOAT64)                       AS value_as_number,
    src.value_as_string                         AS value_as_string,
    src.value_as_concept_id                     AS value_as_concept_id,
    CAST(NULL AS INT64)                         AS qualifier_concept_id,
    CAST(NULL AS INT64)                         AS unit_concept_id,
    CAST(NULL AS INT64)                         AS provider_id,
    vis.visit_occurrence_id                     AS visit_occurrence_id,
    src.visit_detail_id                         AS visit_detail_id,
    src.source_code                             AS observation_source_value,
    src.source_concept_id                       AS observation_source_concept_id,
    CAST(NULL AS STRING)                        AS unit_source_value,
    CAST(NULL AS STRING)                        AS qualifier_source_value,
    -- 
    CONCAT('observation.', src.unit_id)         AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    `@etl_project`.@etl_dataset.lk_observation_mapped src
LEFT JOIN 
    `@etl_project`.@etl_dataset.cdm_person per
        ON CAST(src.subject_id AS STRING) = per.person_source_value
LEFT JOIN 
    `@etl_project`.@etl_dataset.cdm_visit_occurrence vis
        ON CAST(src.hadm_id AS STRING) = vis.visit_source_value
;

-- -------------------------------------------------------------------
-- from lk_procedure_mapped
-- Rule 6
-- -------------------------------------------------------------------

INSERT INTO `@etl_project`.@etl_dataset.cdm_observation
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())           AS observation_id,
    per.person_id                               AS person_id,
    src.target_concept_id                       AS observation_concept_id, -- to rename fields in *_mapped
    CAST(src.start_datetime AS DATE)            AS observation_date,
    src.start_datetime                          AS observation_datetime,
    src.type_concept_id                         AS observation_type_concept_id,
    CAST(NULL AS FLOAT64)                       AS value_as_number,
    CAST(NULL AS STRING)                        AS value_as_string,
    CAST(NULL AS INT64)                         AS value_as_concept_id,
    CAST(NULL AS INT64)                         AS qualifier_concept_id,
    CAST(NULL AS INT64)                         AS unit_concept_id,
    CAST(NULL AS INT64)                         AS provider_id,
    vis.visit_occurrence_id                     AS visit_occurrence_id,
    src.visit_detail_id                         AS visit_detail_id,
    src.source_code                             AS observation_source_value,
    src.source_concept_id                       AS observation_source_concept_id,
    CAST(NULL AS STRING)                        AS unit_source_value,
    CAST(NULL AS STRING)                        AS qualifier_source_value,
    -- 
    CONCAT('observation.', src.unit_id)         AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    `@etl_project`.@etl_dataset.lk_procedure_mapped src
LEFT JOIN 
    `@etl_project`.@etl_dataset.cdm_person per
        ON CAST(src.subject_id AS STRING) = per.person_source_value
LEFT JOIN 
    `@etl_project`.@etl_dataset.cdm_visit_occurrence vis
        ON CAST(src.hadm_id AS STRING) = vis.visit_source_value
WHERE
    src.target_domain_id = 'Observation'
;


-- -------------------------------------------------------------------
-- chartevents
-- Rule 5
-- -------------------------------------------------------------------


-- -- Chartevents.text
-- WITH
-- "chartevents_text" AS (
-- SELECT
--     chartevents.mimic_id AS observation_id,
--     subject_id,
--     hadm_id,
--     charttime AS observation_datetime,
--     value AS value_as_string,
--     valuenum AS value_as_number,
--     concept.concept_id AS observation_source_concept_id,
--     concept.concept_code AS observation_source_value
-- FROM chartevents
-- JOIN :OMOP_SCHEMA.concept ON  -- concept driven dispatcher
-- (           concept_code  = itemid
-- AND domain_id     = 'Observation'
-- AND vocabulary_id = 'MIMIC d_items'
-- )
-- WHERE
--     error IS NULL OR error= 0
-- ),
-- "row_to_insert" AS (
-- SELECT
--     observation_id,
--     person_id,
--     0 AS observation_concept_id,
--     observation_datetime::date observation_date,
--     observation_datetime,
--     581413 AS observation_type_concept_id -- Observation from Measurement,
--     value_as_number,
--     value_as_string,
--     null value_as_concept_id,
--     null qualifier_concept_id,
--     null unit_concept_id,
--     provider_id,
--     visit_occurrence_id,
--     null visit_detail_id,
--     observation_source_value,
--     observation_source_concept_id,
--     null unit_source_value,
--     null qualifier_source_value
-- FROM chartevents_text
-- LEFT JOIN admissions USING (hadm_id))
-- LEFT JOIN :OMOP_SCHEMA.visit_detail_ASsign
-- ON row_to_insert.visit_occurrence_id = visit_detail_ASsign.visit_occurrence_id
-- AND
-- (--only one visit_detail
-- (is_first IS TRUE AND is_lASt IS TRUE)
-- OR -- first
-- (is_first IS TRUE AND is_lASt IS FALSE AND row_to_insert.observation_datetime <= visit_detail_ASsign.visit_end_datetime)
-- OR -- lASt
-- (is_lASt IS TRUE AND is_first IS FALSE AND row_to_insert.observation_datetime > visit_detail_ASsign.visit_start_datetime)
-- OR -- middle
-- (is_lASt IS FALSE AND is_first IS FALSE AND row_to_insert.observation_datetime > visit_detail_ASsign.visit_start_datetime AND row_to_insert.observation_datetime <= visit_detail_ASsign.visit_end_datetime)
-- )
-- ;
