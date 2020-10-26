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
-- negative unique id from FARM_FINGERPRINT(GENERATE_UUID())
--
-- Populate place_of_service_concept_id (see gcpt_care_site and actual mapping) 
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- tmp_careunit
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.tmp_careunit AS
SELECT
    src.careunit                        AS careunit,
    src.load_table_id                   AS load_table_id,
    0                                   AS load_row_id,
    MIN(src.trace_id)                   AS trace_id
FROM 
    `@etl_project`.@etl_dataset.src_transfers src
WHERE
    src.careunit IS NOT NULL
GROUP BY
    careunit,
    load_table_id
;



-- -------------------------------------------------------------------
-- cdm_care_site
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.cdm_care_site
(
    care_site_id                  INT64       not null ,
    care_site_name                STRING               ,
    place_of_service_concept_id   INT64                ,
    location_id                   INT64                ,
    care_site_source_value        STRING               ,
    place_of_service_source_value STRING               ,
    -- 
    unit_id                       STRING,
    load_table_id                 STRING,
    load_row_id                   INT64,
    trace_id                      STRING
)
;

INSERT INTO `@etl_project`.@etl_dataset.cdm_care_site
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())   AS care_site_id,
    src.careunit                        AS care_site_name,
    0                                   AS place_of_service_concept_id,
    1                                   AS location_id,  -- hard-coded BIDMC
    src.careunit                        AS care_site_source_value,
    CAST(NULL AS STRING)                AS place_of_service_source_value,
    'care_site.transfers'       AS unit_id,
    src.load_table_id           AS load_table_id,
    src.load_row_id             AS load_row_id,
    src.trace_id                AS trace_id
FROM 
    `@etl_project`.@etl_dataset.tmp_careunit src
-- LEFT JOIN vocabularies
-- including custom mapping: gcpt_care_site -> vocabulary_id = 'mimiciv_cs_place_of_service'
-- mimic-omop/extras/concept/care_site.csv
WHERE
    src.careunit IS NOT NULL
;



-- gcpt_care_site AS (
--     SELECT
--     nextval('mimic_id_seq') as care_site_id
--     , CASE
--     WHEN wardid.curr_careunit IS NOT NULL THEN format_ward(care_site_name, curr_wardid)
--     ELSE care_site_name end as care_site_name
--     , place_of_service_concept_id as place_of_service_concept_id
--     , care_site_name as care_site_source_value
--     , place_of_service_source_value
--     FROM gcpt_care_site
--     left join wardid on care_site_name = curr_careunit
-- ),
