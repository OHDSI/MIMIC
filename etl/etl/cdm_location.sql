-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Populate cdm_care_site table
-- 
-- Dependencies: run after st_core.sql
-- on Demo: 
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize "create or replace"
--
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- cdm_location
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_location
(
    location_id           INT64     not null ,
    address_1             STRING             ,
    address_2             STRING             ,
    city                  STRING             ,
    state                 STRING             ,
    zip                   STRING             ,
    county                STRING             ,
    location_source_value STRING             ,
    country_concept_id    INT64              ,
    country_source_value  STRING             ,
    latitude              FLOAT64            ,
    longitude             FLOAT64            ,
    -- 
    unit_id                       STRING,
    load_table_id                 STRING,
    load_row_id                   INT64,
    trace_id                      STRING
)
;

INSERT INTO @etl_project.@etl_dataset.cdm_location
SELECT
    1                           AS location_id,
    CAST(NULL AS STRING)        AS address_1,
    CAST(NULL AS STRING)        AS address_2,
    CAST(NULL AS STRING)        AS city,
    'MA'                        AS state,
    CAST(NULL AS STRING)        AS zip,
    CAST(NULL AS STRING)        AS county,
    'Beth Israel Hospital'      AS location_source_value,
    '4330442'                   AS country_concept_id,
    'United States of America'  AS country_source_value,
    '42.33979'                  AS latitude,
    '-71.10488'                 AS longitude,
    -- 
    'location.null'             AS unit_id,
    'null'                      AS load_table_id,
    0                           AS load_row_id,
    CAST(NULL AS STRING)        AS trace_id
;
