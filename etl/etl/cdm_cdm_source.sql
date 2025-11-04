-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Populate source table
-- 
-- Dependencies: no 
--      run in the end of the ETL workflow
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize create or replace
-- 
-- To define source release date as (?)
--      SELECT MAX(creation_time)
--      FROM (loop through source datasets).INFORMATION_SCHEMA.TABLES
-- Add second row for Waveform POC?
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.source
(
    source_name                 STRING        not null ,
    source_abbreviation         STRING             ,
    holder                      STRING             ,
    source_description              STRING             ,
    source_documentation_reference  STRING             ,
    etl_reference               STRING             ,
    source_release_date             DATE               ,
    release_date                DATE               ,
    version                     STRING             ,
    vocabulary_version              STRING             ,
    -- 
    unit_id                       STRING,
    load_table_id                 STRING,
    load_row_id                   INT64,
    trace_id                      STRING
)
;

INSERT INTO @etl_project.@etl_dataset.source
SELECT
    'MIMIC IV'                              AS source_name,
    'mimiciv'                               AS source_abbreviation,
    'PhysioNet'                             AS holder,
    CONCAT('MIMIC-IV is a publicly available database of patients ',
        'admitted to the Beth Israel Deaconess Medical Center in Boston, MA, USA.') AS source_description,
    'https://mimic-iv.mit.edu/docs/'        AS source_documentation_reference,
    'https://github.com/OHDSI/MIMIC/'       AS etl_reference,
    PARSE_DATE('%Y-%m-%d', '2020-09-01')    AS source_release_date, -- to look up
    CURRENT_DATE()                          AS release_date,
    '5.3.1'                                 AS version,
    v.vocabulary_version                    AS vocabulary_version,
    -- 
    'cdm.source'            AS unit_id,
    'none'                  AS load_table_id,
    1                       AS load_row_id,
    TO_JSON_STRING(STRUCT(
        'mimiciv' AS trace_id
    ))                                  AS trace_id

FROM 
    @etl_project.@etl_dataset.voc_vocabulary v
WHERE
    v.vocabulary_id = 'None'
;

