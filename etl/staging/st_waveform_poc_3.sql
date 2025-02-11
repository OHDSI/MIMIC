-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- dependency, run after:
--      st_core.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- A draft to apply Wave Forms
-- 
-- 3 chunks from a trending data CSV file, and from a summarized CSV file
--      
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- open points:
--      parse XML to create src_* or raw_* tables
--
-- POC source tables:
/*
    created from trending data and summarized data in source csv files
    case_id = subject_id, case_id is string
*/
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- staging tables
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- src_waveform_header
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.src_waveform_header_3
(       
    reference_id            STRING,
    raw_files_path          STRING,
    case_id                 STRING,
    subject_id              INT64,
    start_datetime          DATETIME,
    end_datetime            DATETIME,
    --
    load_table_id           STRING,
    load_row_id             INT64,
    trace_id                STRING
);

-- parsed codes to be targeted to table cdm_measurement

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.src_waveform_mx_3
(
    case_id                 STRING,  -- FK to the header
    segment_name            STRING, -- two digits of case_id, 5 digits of internal sequence number
    mx_datetime             DATETIME, -- time of measurement
    source_code             STRING,   -- type of measurement
    value_as_number         FLOAT64,
    unit_source_value       STRING, -- measurement unit "BPM", "MS", "UV" (microvolt) etc.
                                    -- map these labels and populate unit_concept_id
    --
    Visit_Detail___Source               STRING,
    Visit_Detail___Start_from_minutes   INT64,
    Visit_Detail___Report_minutes       INT64,
    Visit_Detail___Sumarize_minutes     INT64,
    Visit_Detail___Method               STRING,
    --
    load_table_id           STRING,
    load_row_id             INT64,
    trace_id                STRING
);


-- parse xml from Manlik? -> src_waveform
-- src_waveform -> visit_detail (visit_detail_source_value = <reference ID>)

-- finding the visit 
-- create visit_detail
-- create measurement -> link visit_detail using visit_detail_source_value = meas_source_value 
-- (start with Manlik's proposal)


-- -------------------------------------------------------------------
-- insert sample data
-- -------------------------------------------------------------------


INSERT INTO @etl_project.@etl_dataset.src_waveform_header_3
SELECT
    subj.short_reference_id             AS reference_id,
    subj.long_reference_id              AS raw_files_path,
    subj.case_id                                        AS case_id, -- string
    CAST(REPLACE(subj.case_id, 'p', '') AS INT64)       AS subject_id, -- int
    CAST(subj.start_datetime AS DATETIME)               AS start_datetime,
    CAST(subj.end_datetime AS DATETIME)                 AS end_datetime,
    --
    'poc_3_header'                      AS load_table_id,
    0                                   AS load_row_id,
    TO_JSON_STRING(STRUCT(
        subj.case_id AS case_id,
        subj.short_reference_id AS reference_id
    ))                                  AS trace_id
FROM
    @wf_project.@wf_dataset.poc_3_header subj
;

-- Chunk 1
-- 25-second interval, mass data

INSERT INTO @etl_project.@etl_dataset.src_waveform_mx_3
SELECT
    src.case_id                         AS case_id, -- FK to the header
    src.segment_name                    AS segment_name,
    --
    CAST(src.date_time AS DATETIME)     AS mx_datetime,
    src.src_name                        AS source_code,
    CAST(src.value AS FLOAT64)          AS value_as_number,
    src.unit_concept_name               AS unit_source_value,
    'csv'                               AS Visit_Detail___Source,
    CAST(NULL AS INT64)                 AS Visit_Detail___Start_from_minutes,
    CAST(NULL AS INT64)                 AS Visit_Detail___Report_minutes,
    CAST(NULL AS INT64)                 AS Visit_Detail___Sumarize_minutes,
    'NONE'                              AS Visit_Detail___Method,
    --
    'poc_3_chunk_1'                     AS load_table_id,
    FARM_FINGERPRINT(GENERATE_UUID())   AS load_row_id,
    TO_JSON_STRING(STRUCT(
            src.case_id AS case_id,
            CAST(src.date_time AS STRING) AS date_time,
            src.src_name AS src_name
        )) AS trace_id -- 
FROM
    @wf_project.@wf_dataset.poc_3_chunk_1 src
INNER JOIN
    @etl_project.@etl_dataset.src_patients pat
        ON  CAST(REPLACE(src.case_id, 'p', '') AS INT64) = pat.subject_id    -- filter out mass data in demo dataset
;


-- Chunk 2
-- 5-minute interval, summarized data for Full set and Demo

INSERT INTO @etl_project.@etl_dataset.src_waveform_mx_3
SELECT
    src.case_id                         AS case_id, -- FK to the header
    src.segment_name                    AS segment_name,
    --
    CAST(src.date_time AS DATETIME)     AS mx_datetime,
    src.src_name                        AS source_code,
    CAST(src.value AS FLOAT64)          AS value_as_number,
    src.unit_concept_name               AS unit_source_value,
    Visit_Detail___Source               AS Visit_Detail___Source,
    Visit_Detail___Start_from_minutes   AS Visit_Detail___Start_from_minutes,
    Visit_Detail___Report_minutes       AS Visit_Detail___Report_minutes,
    Visit_Detail___Sumarize_minutes     AS Visit_Detail___Sumarize_minutes,
    Visit_Detail___Method               AS Visit_Detail___Method,
    --
    'poc_3_chunk_2'                     AS load_table_id,
    FARM_FINGERPRINT(GENERATE_UUID())   AS load_row_id,
    TO_JSON_STRING(STRUCT(
            src.case_id AS case_id,
            CAST(src.date_time AS STRING) AS date_time,
            src.src_name AS src_name
        )) AS trace_id -- 
FROM
    @wf_project.@wf_dataset.poc_3_chunk_2 src
;


-- Chunk 3
-- 25-second interval, tiny mass data for Demo

INSERT INTO @etl_project.@etl_dataset.src_waveform_mx_3
SELECT
    src.case_id                         AS case_id, -- FK to the header
    src.segment_name                    AS segment_name,
    --
    CAST(src.date_time AS DATETIME)     AS mx_datetime,
    src.src_name                        AS source_code,
    CAST(src.value AS FLOAT64)          AS value_as_number,
    src.unit_concept_name               AS unit_source_value,
    Visit_Detail___Source               AS Visit_Detail___Source,
    Visit_Detail___Start_from_minutes   AS Visit_Detail___Start_from_minutes,
    Visit_Detail___Report_minutes       AS Visit_Detail___Report_minutes,
    Visit_Detail___Sumarize_minutes     AS Visit_Detail___Sumarize_minutes,
    Visit_Detail___Method               AS Visit_Detail___Method,
    --
    'poc_3_chunk_3'                     AS load_table_id,
    FARM_FINGERPRINT(GENERATE_UUID())   AS load_row_id,
    TO_JSON_STRING(STRUCT(
            src.case_id AS case_id,
            CAST(src.date_time AS STRING) AS date_time,
            src.src_name AS src_name
        )) AS trace_id -- 
FROM
    @wf_project.@wf_dataset.poc_3_chunk_3 src
;


