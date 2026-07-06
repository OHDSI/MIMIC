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

-- Preflight: trg_file must be present for all rows
DECLARE missing_trg INT64;
SET missing_trg = (
  SELECT COUNT(*)
  FROM @etl_project.@etl_dataset.waveform_files_all
  WHERE trg_file IS NULL OR TRIM(trg_file) = ''
);
ASSERT missing_trg = 0 AS 'staging contains rows with empty trg_file; canonical target URI is required'
;

INSERT INTO @etl_project.@etl_dataset.cdm_waveform_registry
-- Make one registry row per target file (trg_file); preserve raw extension; map via normalized extension
WITH files_with_extensions AS (
  SELECT
    f.*,
    REGEXP_EXTRACT(TRIM(f.trg_file), r'(.[^\.]+)$') AS raw_trg_ext,
    UPPER(REGEXP_EXTRACT(TRIM(f.trg_file), r'.([^\.]+)$')) AS norm_trg_ext
  FROM @etl_project.@etl_dataset.waveform_files_all f
),
file_rows AS (
  SELECT
    `@etl_project.@etl_dataset.obf_id_str`(f.trg_file, 32)    AS waveform_registry_id,
    `@etl_project.@etl_dataset.obf_id`(f.group_id, 32)        AS waveform_occurrence_id,
    CAST(NULL AS INT64)                                       AS waveform_feature_id,
    f.person_id                                               AS person_id,
    DATETIME(f.file_start)                                    AS waveform_file_start_datetime,
    DATETIME(f.file_end)                                      AS waveform_file_end_datetime,
    f.visit_occurrence_id                                     AS visit_occurrence_id,
    f.visit_detail_id                                         AS visit_detail_id,
    f.raw_trg_ext                                             AS extracted_extension,   -- raw (with dot, case-preserving)
    f.norm_trg_ext                                            AS normalized_extension,  -- uppercase, no dot (for mapping only)
    f.src_file                                                AS waveform_source_file_uri,
    f.trg_file                                                AS waveform_target_file_uri
  FROM files_with_extensions f
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
  LEFT JOIN (
    SELECT concept_id, UPPER(concept_name) AS concept_name_u
    FROM @etl_project.@etl_dataset.voc_concept
    WHERE domain_id = 'Waveform Metadata'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(concept_name) ORDER BY concept_id) = 1
  ) wc
    ON wc.concept_name_u = fr.normalized_extension
  LEFT JOIN ( SELECT concept_id_1, concept_id_2 
    FROM @etl_project.@etl_dataset.voc_concept_relationship 
    WHERE relationship_id = 'Maps to' 
    AND invalid_reason IS NULL 
    QUALIFY ROW_NUMBER() OVER (PARTITION BY concept_id_1 ORDER BY concept_id_2) = 1 
  ) cr1 
    ON cr1.concept_id_1 = wc.concept_id
  LEFT JOIN @etl_project.@etl_dataset.voc_concept wc1 
    ON wc1.concept_id = cr1.concept_id_2 
    AND wc1.invalid_reason IS NULL 
    AND wc1.standard_concept = 'S'
;
