-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Populate cdm_person table
-- 
-- Dependencies: run after st_core.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- negative unique id from FARM_FINGERPRINT()
--
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- tmp_subject_race
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.tmp_subject_race AS
SELECT DISTINCT
    src.subject_id                      AS subject_id,
    FIRST_VALUE(src.race) OVER (
        PARTITION BY src.subject_id 
        ORDER BY src.admittime ASC)     AS race_first
FROM
    @etl_project.@etl_dataset.src_admissions src
;

-- -------------------------------------------------------------------
-- lk_pat_race_concept
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.lk_pat_race_concept AS
SELECT DISTINCT
    src.race_first     AS source_code,
    vc.concept_id           AS source_concept_id,
    vc.domain_id            AS domain_id,
    vc.vocabulary_id        AS source_vocabulary_id,
    vc1.concept_id          AS target_concept_id,
    vc1.vocabulary_id       AS target_vocabulary_id
FROM
    @etl_project.@etl_dataset.tmp_subject_race src
LEFT JOIN
    @etl_project.@etl_dataset.voc_concept vc
        ON UPPER(vc.concept_name) = UPPER(src.race_first) -- do the custom mapping
        AND vc.domain_id IN ('Race', 'Ethnicity')
        AND vc.concept_class_id = 'mimic-race'
LEFT JOIN
    @etl_project.@etl_dataset.voc_concept_relationship cr1
        ON  cr1.concept_id_1 = vc.concept_id
        AND cr1.relationship_id = 'Maps to'
LEFT JOIN
    @etl_project.@etl_dataset.voc_concept vc1
        ON  cr1.concept_id_2 = vc1.concept_id
        AND vc1.invalid_reason IS NULL
        AND vc1.standard_concept = 'S'
;

-- -------------------------------------------------------------------
-- cdm_person
-- -------------------------------------------------------------------

--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_person
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

INSERT INTO @etl_project.@etl_dataset.cdm_person
SELECT
    FARM_FINGERPRINT(CAST(p.subject_id AS STRING)) AS person_id,
    CASE 
        WHEN p.gender = 'F' THEN 8532 -- FEMALE
        WHEN p.gender = 'M' THEN 8507 -- MALE
        ELSE 0 
    END                             AS gender_concept_id,
    p.anchor_year-p.anchor_age      AS year_of_birth,
    CAST(NULL AS INT64)             AS month_of_birth,
    CAST(NULL AS INT64)             AS day_of_birth,
    CAST(NULL AS DATETIME)          AS birth_datetime,
    COALESCE(
        CASE
            WHEN map_rc.domain_id = 'Race'
                THEN map_rc.target_concept_id
            ELSE NULL
        END, 0)                     AS race_concept_id,
    -- MIMIC only has a race column. For HISPANIC OR LATINO the domain_id = 'Ethnicity'. For other "races" which contain
    -- HISPANIC/LATINO map ethnicity_concept_id to 38003563.
    COALESCE(
        CASE
            WHEN map_rc.domain_id = 'Ethnicity' THEN map_rc.target_concept_id
            WHEN map_rc.domain_id = 'Race' AND map_rc.source_code LIKE '%HISPANIC/LATINO%' THEN 38003563
            ELSE NULL
        END, 38003564)              AS ethnicity_concept_id,
    CAST(NULL AS INT64)             AS location_id,
    CAST(NULL AS INT64)             AS provider_id,
    CAST(NULL AS INT64)             AS care_site_id,
    CAST(p.subject_id AS STRING)    AS person_source_value,
    p.gender                        AS gender_source_value,
    0                               AS gender_source_concept_id,
    rc.race_first                   AS race_source_value,
    map_rc.source_concept_id        AS race_source_concept_id,
    CAST(NULL AS STRING)            AS ethnicity_source_value,
    CAST(NULL AS INT64)             AS ethnicity_source_concept_id,
    --
    'person.patients'               AS unit_id,
    p.load_table_id                 AS load_table_id,
    p.load_row_id                   AS load_row_id,
    p.trace_id                      AS trace_id
FROM 
    @etl_project.@etl_dataset.src_patients p
LEFT JOIN 
    @etl_project.@etl_dataset.tmp_subject_race rc
        ON  p.subject_id = rc.subject_id
LEFT JOIN
    @etl_project.@etl_dataset.lk_pat_race_concept map_rc
        ON  rc.race_first = map_rc.source_code
;


-- -------------------------------------------------------------------
-- cleanup
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS @etl_project.@etl_dataset.tmp_subject_race;
