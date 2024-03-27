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

-- -------------------------------------------------------------------
-- lk_death_adm_mapped
-- Rule 1, admissionss
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.lk_death_adm_mapped AS
SELECT DISTINCT
    src.subject_id, 
    FIRST_VALUE(src.deathtime) OVER(
        PARTITION BY src.subject_id 
        ORDER BY src.admittime ASC
    )                                   AS deathtime, 
    FIRST_VALUE(src.dischtime) OVER(
        PARTITION BY src.subject_id 
        ORDER BY src.admittime ASC
    )                                   AS dischtime,
    32817                               AS type_concept_id, -- OMOP4976890 EHR
    --
    'admissions'                        AS unit_id,
    src.load_table_id                   AS load_table_id,
    FIRST_VALUE(src.load_row_id) OVER(
        PARTITION BY src.subject_id 
        ORDER BY src.admittime ASC
    )                                   AS load_row_id,
    FIRST_VALUE(src.trace_id) OVER(
        PARTITION BY src.subject_id 
        ORDER BY src.admittime ASC
    )                                   AS trace_id
FROM 
    @etl_project.@etl_dataset.src_admissions src -- adm
WHERE 
    src.deathtime IS NOT NULL
;

-- -------------------------------------------------------------------
-- cdm_death
-- -------------------------------------------------------------------

--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_death
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

INSERT INTO @etl_project.@etl_dataset.cdm_death
SELECT
    per.person_id       AS person_id,
    CAST(IF(
        src.deathtime <= src.dischtime, 
            src.deathtime, src.dischtime
    ) AS DATE)                              AS death_date,
    IF(
        src.deathtime <= src.dischtime, 
            src.deathtime, src.dischtime
    )                                       AS death_datetime,
    src.type_concept_id                     AS death_type_concept_id,
    0                                       AS cause_concept_id,
    CAST(NULL AS STRING)                    AS cause_source_value,
    0                                       AS cause_source_concept_id,
    --
    CONCAT('death.', src.unit_id)           AS unit_id,
    src.load_table_id       AS load_table_id,
    src.load_row_id         AS load_row_id,
    src.trace_id            AS trace_id
FROM
    @etl_project.@etl_dataset.lk_death_adm_mapped src
INNER JOIN
    @etl_project.@etl_dataset.cdm_person per
        ON CAST(src.subject_id AS STRING) = per.person_source_value
;