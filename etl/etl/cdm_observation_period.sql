-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Populate cdm_observation_period table
-- 
-- Dependencies: run after 
--      cdm_visit_occurrence
--
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize create or replace
-- -------------------------------------------------------------------


-- -------------------------------------------------------------------
-- tmp_observation_period
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.tmp_observation_period AS
SELECT
    src.person_id               AS person_id,
    MIN(src.visit_start_date)   AS start_date,
    MAX(src.visit_end_date)     AS end_date,
    src.load_table_id           AS load_table_id
FROM
    `@etl_project`.@etl_dataset.cdm_visit_occurrence src
GROUP BY
    src.person_id,
    src.load_table_id
;

-- -------------------------------------------------------------------
-- cdm_observation_period
-- -------------------------------------------------------------------

--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.cdm_observation_period
(
    observation_period_id             INT64   not null ,
    person_id                         INT64   not null ,
    observation_period_start_date     DATE    not null ,
    observation_period_end_date       DATE    not null ,
    period_type_concept_id            INT64   not null ,
    -- 
    unit_id                       STRING,
    load_table_id                 STRING,
    load_row_id                   INT64,
    trace_id                      STRING
)
;

INSERT INTO `@etl_project`.@etl_dataset.cdm_observation_period
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())           AS observation_period_id,
    src.person_id                               AS person_id,
    src.start_date                              AS observation_period_start_date,
    src.end_date                                AS observation_period_end_date,
    44814724                                    AS period_type_concept_id,  --  Period covering healthcare encounters
    --
    'observation_period'                        AS unit_id,
    src.load_table_id                           AS load_table_id,
    0                                           AS load_row_id,
    TO_JSON_STRING(STRUCT(
        src.person_id AS person_id
    ))                                          AS trace_id -- to get trace_id having start_date = min(start_date)
FROM 
    `@etl_project`.@etl_dataset.tmp_observation_period src
;

-- -------------------------------------------------------------------
-- cleanup
-- -------------------------------------------------------------------

-- DROP TABLE IF EXISTS `@etl_project`.@etl_dataset.tmp_observation_period;
