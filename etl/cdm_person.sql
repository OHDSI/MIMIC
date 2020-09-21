-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Populate cdm_person table
-- 
-- Dependencies: run after st_core.sql
-- on Demo: 12.4 sec
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize "create or replace"
-- `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.cdm_person;
--
-- negative unique id from FARM_FINGERPRINT(GENERATE_UUID())
--
-- Do custom mapping for Race and Ethnicity
--
-- Why don't we want to use subject_id as person_id and hadm_id as visit_occurrence_id?
--      ask analysts
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- tmp_subject_ethnicity
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.tmp_subject_ethnicity AS
SELECT DISTINCT
    src.subject_id                      AS subject_id,
    FIRST_VALUE(src.ethnicity) OVER (
        PARTITION BY src.subject_id 
        ORDER BY src.admittime ASC)     AS ethnicity_first
FROM
    `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.src_admissions src
;

-- -------------------------------------------------------------------
-- tmp_map_ethnicity
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.tmp_map_ethnicity AS
SELECT DISTINCT
    src.ethnicity_first     AS source_value,
    vc.concept_id           AS source_concept_id,
    vc.vocabulary_id        AS source_vocabulary_id,
    vc1.concept_id          AS target_concept_id,
    vc1.vocabulary_id       AS target_vocabulary_id
FROM
    `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.tmp_subject_ethnicity src
LEFT JOIN
    `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.voc_concept vc
        ON UPPER(vc.concept_code) = UPPER(src.ethnicity_first) -- do the custom mapping
LEFT JOIN
    `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.voc_concept_relationship cr1
        ON  cr1.concept_id_1 = vc.concept_id
        AND cr1.relationship_id = 'Maps to'
LEFT JOIN
    `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.voc_concept vc1
        ON  cr1.concept_id_2 = vc1.concept_id
        AND vc1.invalid_reason IS NULL
        AND vc1.standard_concept = 'S'
;

-- -------------------------------------------------------------------
-- cdm_person
-- -------------------------------------------------------------------

--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.cdm_person
(
    person_id                   INT64     not null ,
    gender_concept_id           INT64     not null ,
    year_of_birth               INT64     not null ,
    month_of_birth              INT64              ,
    day_of_birth                INT64              ,
    birth_datetime              DATETIME           ,
    race_concept_id             INT64     not null,
    ethnicity_concept_id        INT64     not null,
    location_id                 INT64              ,
    provider_id                 INT64              ,
    care_site_id                INT64              ,
    person_source_value         STRING             ,
    gender_source_value         STRING             ,
    gender_source_concept_id    INT64              ,
    race_source_value           STRING             ,
    race_source_concept_id      INT64              ,
    ethnicity_source_value      STRING             ,
    ethnicity_source_concept_id INT64              ,
    -- 
    unit_id                       STRING,
    load_table_id                 STRING,
    load_row_id                   INT64,
    trace_id                      STRING
)
;

INSERT INTO `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.cdm_person
SELECT
    FARM_FINGERPRINT(GENERATE_UUID()) AS person_id,
    CASE 
        WHEN p.gender_source_value = 'F' THEN 8532 
        WHEN p.gender_source_value = 'M' THEN 8507 
        ELSE 0 
    END                             AS gender_concept_id,
    p.year_of_birth                 AS year_of_birth,
    CAST(NULL AS INT64)             AS month_of_birth,
    CAST(NULL AS INT64)             AS day_of_birth,
    CAST(NULL AS DATETIME)          AS birth_datetime,
    COALESCE(
        CASE
            WHEN map_eth.source_vocabulary_id = 'Race'
                THEN map_eth.target_concept_id
            ELSE NULL
        END, 0)                               AS race_concept_id,
    COALESCE(
        CASE
            WHEN map_eth.source_vocabulary_id = 'Ethnicity'
                THEN map_eth.target_concept_id
            ELSE NULL
        END, 0)                     AS ethnicity_concept_id,
    CAST(NULL AS INT64)             AS location_id,
    CAST(NULL AS INT64)             AS provider_id,
    CAST(NULL AS INT64)             AS care_site_id,
    CAST(p.subject_id AS STRING)    AS person_source_value,
    p.gender_source_value           AS gender_source_value,
    0                               AS gender_source_concept_id,
    CASE
        WHEN map_eth.source_vocabulary_id = 'Race'
            THEN eth.ethnicity_first
        ELSE NULL
    END                             AS race_source_value,
    0                               AS race_source_concept_id,
    CASE
        WHEN map_eth.source_vocabulary_id = 'Ethnicity'
            THEN eth.ethnicity_first
        ELSE NULL
    END                             AS ethnicity_source_value,
    0                               AS ethnicity_source_concept_id,
    -- 
    'person.patients'               AS unit_id,
    p.load_table_id                 AS load_table_id,
    p.load_row_id                   AS load_row_id,
    p.trace_id                      AS trace_id
FROM 
    `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.src_patients p
LEFT JOIN 
    `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.tmp_subject_ethnicity eth 
        ON  p.subject_id = eth.subject_id
LEFT JOIN
    `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.tmp_map_ethnicity map_eth
        ON  eth.ethnicity_first = map_eth.source_value
;


-- gcpt_ethnicity_to_concept AS (
--     SELECT
--         concept_code AS ethnicity,
--         concept_id AS ethnicity_concept_id 
--     FROM 
--         `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.voc_concept
--     WHERE
--         vocabulary_id = 'Ethnicity'
-- )


-- DROP TABLE IF EXISTS `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.tmp_subject_ethnicity;
-- DROP TABLE IF EXISTS `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.tmp_map_ethnicity;
