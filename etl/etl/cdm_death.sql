-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Populate cdm_death table
-- 
-- Dependencies: run after 
--      st_core.sql,
--      cdm_person.sql
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.tmp_death_adm AS
SELECT 
    subject_id, 
    least(deathtime, dischtime) as death_datetime, 
    38003569 as death_type_concept_id
FROM 
(
    SELECT
        first_value(deathtime) OVER(PARTITION BY subject_id ORDER BY admittime ASC) as deathtime, 
        subject_id, 
        dischtime 
    FROM 
        admissions 
    WHERE 
        deathtime IS NOT NULL
    GROUP BY
        subject_id
) a --donor organs
WHERE 
    deathtime IS NOT NULL
;

-- to review implemented
-- "death_adm" AS (
--     SELECT patients.mimic_id as person_id, least(deathtime, dischtime) as death_datetime, 38003569 as death_type_concept_id
--     FROM (SELECT distinct on (subject_id) first_value(deathtime) OVER(PARTITION BY subject_id ORDER BY admittime ASC) as deathtime, subject_id, dischtime FROM admissions WHERE deathtime IS NOT NULL) a --donor organs
--     LEFT JOIN patients USING (subject_id)
--     WHERE deathtime IS NOT NULL),


death_ssn AS (
    SELECT mimic_id as person_id, dod_ssn as death_datetime, 261 as death_type_concept_id
    FROM patients LEFT JOIN death_adm ON (mimic_id = person_id)
    WHERE dod_ssn IS NOT NULL AND death_adm.person_id IS NULL)


-- -------------------------------------------------------------------
-- cdm_death
-- -------------------------------------------------------------------

--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.cdm_death
(
    person_id               INT64     not null ,
    death_date              DATE      not null ,
    death_datetime          DATETIME           ,
    death_type_concept_id   INT64     not null ,
    cause_concept_id        INT64              ,
    cause_source_value      STRING             ,
    cause_source_concept_id INT64              ,
    -- 
    unit_id                       STRING,
    load_table_id                 STRING,
    load_row_id                   INT64,
    trace_id                      STRING
)
;

INSERT INTO `@etl_project`.@etl_dataset.cdm_death
SELECT 
    person_id, death_datetime::date, (death_datetime), death_type_concept_id
FROM  death_adm
UNION ALL
SELECT person_id, death_datetime::date, (death_datetime), death_type_concept_id
FROM  death_ssn;
