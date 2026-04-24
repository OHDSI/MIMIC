-- -------------------------------------------------------------------
-- MIMIC Waveform ETL
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_waveform_occurrence
(
  waveform_occurrence_id                  INT64     not null,
  waveform_occurrence_concept_id          INT64     not null,
  person_id                               INT64     not null,
  waveform_occurrence_start_datetime      DATETIME  not null,
  waveform_occurrence_end_datetime        DATETIME  not null,
  visit_occurrence_id                     INT64     not null,
  visit_detail_id                         INT64             ,
  preceding_waveform_occurrence_id        INT64             ,
  waveform_format_concept_id              INT64             ,
  waveform_occurrence_source_value        STRING            ,
  num_of_files                            INT64             ,
  waveform_format_source_value            STRING
)
;

INSERT INTO @etl_project.@etl_dataset.cdm_waveform_occurrence
SELECT
  `@etl_project.@etl_dataset`.obf_id(f.group_id, 32)  AS waveform_occurrence_id,
  -- Physiological monitoring from TuftsCTSI
  2081500001                                          AS waveform_occurrence_concept_id,
  ANY_VALUE(f.person_id)                              AS person_id,
  DATETIME(ANY_VALUE(f.session_start))                AS waveform_occurrence_start_datetime,
  DATETIME(ANY_VALUE(f.session_end))                  AS waveform_occurrence_end_datetime,
  ANY_VALUE(f.visit_occurrence_id)                    AS visit_occurrence_id,
  ANY_VALUE(f.visit_detail_id)                        AS visit_detail_id,
  0                                                   AS preceding_waveform_occurrence_id,
  -- WFDB from TuftsCTSI
  2082499975                                          AS waveform_format_concept_id,
  CAST(f.group_id AS STRING)                          AS waveform_occurrence_source_value,
  COUNT(*)                                            AS num_of_files,
  'WFDB'                                              AS waveform_format_source_value

  FROM @etl_project.@etl_dataset.waveform_files_all f
  GROUP BY
    f.group_id
;
