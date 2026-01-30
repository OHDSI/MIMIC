-- -------------------------------------------------------------------
-- MIMIC Waveform ETL
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_waveform_registry
(
  waveform_registry_id                    INT64     not null,
  waveform_occurrence_id                  INT64     not null,
  waveform_feature_id                     INT64             ,
  person_id                               INT64     not null,
  waveform_file_start_datetime	          DATETIME  not null,
  waveform_file_end_datetime	          DATETIME  not null,
  visit_occurrence_id                     INT64     not null,
  visit_detail_id                         INT64             ,
  file_extension_concept_id               INT64             ,
  file_extension_source_value             STRING    not null,
  waveform_source_file_uri                STRING            ,
  waveform_target_file_uri                STRING    not null
)
;

-- DEBUG: Comment out INSERT and uncomment SELECT to test the CTE
INSERT INTO @etl_project.@etl_dataset.cdm_waveform_registry
WITH files_with_extension AS (
  SELECT
    f.*,
    COALESCE(UPPER(REGEXP_EXTRACT(TRIM(f.trg_file), r'\.([^.]+)$')), 'UNKNOWN') AS extracted_extension
  FROM @etl_project.@etl_dataset.waveform_files f
)
SELECT
  `@etl_project.@etl_dataset.obf_id_str`(f.trg_file, 32)    AS waveform_registry_id,
  `@etl_project.@etl_dataset`.obf_id(f.group_id, 32)        AS waveform_occurrence_id,
  CAST(NULL AS INT64)                                       AS waveform_feature_id,
  f.person_id                                               AS person_id,
  DATETIME(f.file_start)                                    AS waveform_file_start_datetime,
  DATETIME(f.file_end)                                      AS waveform_file_end_datetime,
  f.visit_occurrence_id                                     AS visit_occurrence_id,
  f.visit_detail_id                                         AS visit_detail_id,
  wc1.concept_id                                            AS file_extension_concept_id,
  f.extracted_extension                                     AS file_extension_source_value,
  f.src_file                                                AS waveform_source_file_uri,
  f.trg_file                                                AS waveform_target_file_uri
  
  FROM files_with_extension f
  LEFT JOIN
    @wf_project.@wf_dataset.concept wc
      ON UPPER(wc.concept_name) = f.extracted_extension
      AND wc.domain_id IN ('Waveform Metadata')
  LEFT JOIN
    @wf_project.@wf_dataset.concept_relationship cr1
      ON cr1.concept_id_1 = wc.concept_id
      AND cr1.relationship_id = 'Maps to'
  LEFT JOIN
    @wf_project.@wf_dataset.concept wc1
      ON cr1.concept_id_2 = wc1.concept_id
      AND wc1.invalid_reason IS NULL
    AND wc1.standard_concept = 'S'
;

