-- This SQL script updates the `note_nlp` table generated from the OHNLP tool to conform 
-- with the OMOP Common Data Model (CDM) specifications. It also removes data from 
-- sensitive fields.

-- First, rename the existing OHNLP table to preserve original data
-- CREATE OR REPLACE TABLE @etl_project.@etl_dataset.note_nlp_ohnlp_source AS
-- SELECT * FROM @etl_project.@etl_dataset.note_nlp;

-- Drop the original table to recreate with proper OMOP CDM structure
DROP TABLE IF EXISTS @etl_project.@etl_dataset.note_nlp;

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.note_nlp
(
  note_nlp_id                 INT64                ,
  note_id                     INT64                ,
  section_concept_id          INT64                ,
  snippet                     STRING               ,
  offset                      STRING               ,
  lexical_variant             STRING      not null ,
  note_nlp_concept_id         INT64                ,
  note_nlp_source_concept_id  INT64                ,
  nlp_system                  STRING               ,
  nlp_date                    DATE        not null ,
  nlp_datetime                DATETIME             ,
  term_exists                 STRING               ,
  term_temporal               STRING               ,
  term_modifiers              STRING
)
;

-- Insert transformed data from OHNLP source to OMOP CDM format
INSERT INTO @etl_project.@etl_dataset.note_nlp
SELECT
    `@etl_project.@etl_dataset`.obf_id_str(CONCAT(
        note_id, '|', 
        COALESCE(CAST(section_concept_id AS STRING), '0'), '|',
        COALESCE(CAST(offset AS STRING), '0')
    ), 32)                                                              AS note_nlp_id,
    `@etl_project.@etl_dataset`.obf_id_str(note_id, 32)                 AS note_id,
    section_concept_id                                                  AS section_concept_id,
    CAST(NULL AS STRING)                                                AS snippet,
    CAST(offset AS STRING)                                              AS offset,
    'removed'                                                           AS lexical_variant,
    note_nlp_concept_id                                                 AS note_nlp_concept_id,
    note_nlp_source_concept_id                                          AS note_nlp_source_concept_id,
    nlp_system                                                          AS nlp_system,
    DATE(nlp_datetime)                                                  AS nlp_date,
    DATETIME(nlp_datetime)                                              AS nlp_datetime,
    CAST(NULL AS STRING)                                                AS term_exists,
    CAST(NULL AS STRING)                                                AS term_temporal,
    term_modifiers                                                      AS term_modifiers

FROM @etl_project.@etl_dataset.note_nlp_ohnlp_source
WHERE note_id IS NOT NULL
;

