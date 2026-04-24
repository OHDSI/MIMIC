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

INSERT INTO @etl_project.@etl_dataset.cdm_waveform_registry
-- Make (file_extension_source_value, file_extension_concept_id) entries for both the .hea and .csv version of the numerics files
WITH files_with_extensions AS (
  SELECT
    f.*,
    COALESCE(UPPER(REGEXP_EXTRACT(TRIM(f.src_file), r'\.([^.]+)$')), 'UNKNOWN') AS src_extension,
    COALESCE(UPPER(REGEXP_EXTRACT(TRIM(f.trg_file), r'\.([^.]+)$')), 'UNKNOWN') AS trg_extension
  FROM @etl_project.@etl_dataset.waveform_files_all f
),
file_rows AS (
  -- Row 1 for numerics files: HEA extension metadata pointing to CSV target
  SELECT
    `@etl_project.@etl_dataset.obf_id_str`(f.src_file, 32)    AS waveform_registry_id,
    `@etl_project.@etl_dataset.obf_id`(f.group_id, 32)        AS waveform_occurrence_id,
    CAST(NULL AS INT64)                                       AS waveform_feature_id,
    f.person_id                                               AS person_id,
    DATETIME(f.file_start)                                    AS waveform_file_start_datetime,
    DATETIME(f.file_end)                                      AS waveform_file_end_datetime,
    f.visit_occurrence_id                                     AS visit_occurrence_id,
    f.visit_detail_id                                         AS visit_detail_id,
    f.src_extension                                           AS extracted_extension,
    f.src_file                                                AS waveform_source_file_uri,
    REGEXP_REPLACE(f.src_file, r'\.hea$', '.csv')             AS waveform_target_file_uri
  FROM files_with_extensions f
  WHERE REGEXP_CONTAINS(LOWER(f.src_file), r'n\.hea$')
    AND REGEXP_CONTAINS(LOWER(f.trg_file), r'n\.csv$')
  
  UNION ALL
  
  -- Row 2 for numerics files: CSV extension metadata
  SELECT
    `@etl_project.@etl_dataset.obf_id_str`(f.trg_file, 32)    AS waveform_registry_id,
    `@etl_project.@etl_dataset.obf_id`(f.group_id, 32)        AS waveform_occurrence_id,
    CAST(NULL AS INT64)                                       AS waveform_feature_id,
    f.person_id                                               AS person_id,
    DATETIME(f.file_start)                                    AS waveform_file_start_datetime,
    DATETIME(f.file_end)                                      AS waveform_file_end_datetime,
    f.visit_occurrence_id                                     AS visit_occurrence_id,
    f.visit_detail_id                                         AS visit_detail_id,
    f.trg_extension                                           AS extracted_extension,
    REGEXP_REPLACE(f.src_file, r'\.hea$', '.csv')             AS waveform_source_file_uri,
    f.trg_file                                                AS waveform_target_file_uri
  FROM files_with_extensions f
  WHERE REGEXP_CONTAINS(LOWER(f.src_file), r'n\.hea$')
    AND REGEXP_CONTAINS(LOWER(f.trg_file), r'n\.csv$')
  
  UNION ALL
  
  -- Base row for non-numerics files
  SELECT
    `@etl_project.@etl_dataset.obf_id_str`(f.trg_file, 32)    AS waveform_registry_id,
    `@etl_project.@etl_dataset.obf_id`(f.group_id, 32)        AS waveform_occurrence_id,
    CAST(NULL AS INT64)                                       AS waveform_feature_id,
    f.person_id                                               AS person_id,
    DATETIME(f.file_start)                                    AS waveform_file_start_datetime,
    DATETIME(f.file_end)                                      AS waveform_file_end_datetime,
    f.visit_occurrence_id                                     AS visit_occurrence_id,
    f.visit_detail_id                                         AS visit_detail_id,
    f.trg_extension                                           AS extracted_extension,
    f.src_file                                                AS waveform_source_file_uri,
    f.trg_file                                                AS waveform_target_file_uri
  FROM files_with_extensions f
  WHERE NOT (REGEXP_CONTAINS(LOWER(f.src_file), r'n\.hea$')
    AND REGEXP_CONTAINS(LOWER(f.trg_file), r'n\.csv$'))
)
SELECT
  fr.waveform_registry_id                                   AS waveform_registry_id,
  fr.waveform_occurrence_id,
  fr.waveform_feature_id,
  fr.person_id,
  fr.waveform_file_start_datetime,
  fr.waveform_file_end_datetime,
  fr.visit_occurrence_id,
  fr.visit_detail_id,
  wc1.concept_id                                            AS file_extension_concept_id,
  fr.extracted_extension                                    AS file_extension_source_value,
  fr.waveform_source_file_uri                               AS waveform_source_file_uri,
  fr.waveform_target_file_uri                               AS waveform_target_file_uri
  
  FROM file_rows fr
  LEFT JOIN
    @etl_project.@etl_dataset.voc_concept wc
      ON UPPER(wc.concept_name) = fr.extracted_extension
      AND wc.domain_id IN ('Waveform Metadata')
  LEFT JOIN
    @etl_project.@etl_dataset.voc_concept_relationship cr1
      ON cr1.concept_id_1 = wc.concept_id
      AND cr1.relationship_id = 'Maps to'
  LEFT JOIN
    @etl_project.@etl_dataset.voc_concept wc1
      ON cr1.concept_id_2 = wc1.concept_id
      AND wc1.invalid_reason IS NULL
    AND wc1.standard_concept = 'S'
;
