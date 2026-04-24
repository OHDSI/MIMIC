-- -------------------------------------------------------------------
-- MIMIC Waveform ETL
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_waveform_channel_metadata
(
  waveform_channel_metadata_id            INT64     not null,
  waveform_registry_id                    INT64     not null,
  procedure_occurrence_id                 INT64             ,
  device_exposure_id                      INT64             ,
  waveform_channel_source_value           STRING            ,
  channel_concept_id                      INT64     not null,
  metadata_source_value                   STRING    not null,
  metadata_concept_id                     INT64     not null,
  value_as_number                         FLOAT64           ,
  value_as_concept_id                     INT64             ,
  value_as_string                         STRING            ,
  unit_concept_id                         INT64             ,
  unit_source_value                       STRING
)
;

INSERT INTO @etl_project.@etl_dataset.cdm_waveform_channel_metadata
WITH channel_metadata_unpivoted AS (
  SELECT
    person_id, visit_occurrence_id, group_id, trg_file, channel_name, sample_units,
    'AMPLITUDE' AS metadata_type,
    CAST(NULL AS FLOAT64) AS value_as_number,
    CAST(NULL AS INT64) AS value_as_concept_id,
    CAST(NULL AS STRING) AS value_as_string,
    sample_units AS unit_source_value
  FROM @etl_project.@etl_dataset.waveform_channels_all
  
  UNION ALL
  
  SELECT
    person_id, visit_occurrence_id, group_id, trg_file, channel_name, sample_units,
    'SAMPLERATE' AS metadata_type,
    sample_rate AS value_as_number,
    CAST(NULL AS INT64) AS value_as_concept_id,
    CAST(NULL AS STRING) AS value_as_string,
    sample_rate_units AS unit_source_value
  FROM @etl_project.@etl_dataset.waveform_channels_all

  UNION ALL

  SELECT 
    person_id, visit_occurrence_id, group_id, trg_file, channel_name, sample_units,
    'RESOLUTION' AS metadata_type,
    gain AS value_as_number,
    CAST(NULL AS INT64) AS value_as_concept_id,
    CAST(NULL AS STRING) AS value_as_string,
    gain_units AS unit_source_value
  FROM @etl_project.@etl_dataset.waveform_channels_all
  
  UNION ALL

  SELECT 
    person_id, visit_occurrence_id, group_id, trg_file, channel_name, sample_units,
    'SEGMENTLENGTH' AS metadata_type,
    segment_length AS value_as_number,
    CAST(NULL AS INT64) AS value_as_number,
    CAST(NULL AS STRING) AS value_as_string,
    'samples' AS unit_source_value
  FROM @etl_project.@etl_dataset.waveform_channels_all
)
SELECT
  `@etl_project.@etl_dataset.obf_id_str`(CONCAT(meta.trg_file, meta.channel_name, meta.metadata_type), 32)  AS waveform_channel_metadata_id,
  `@etl_project.@etl_dataset.obf_id_str`(meta.trg_file, 32)  AS waveform_registry_id,
  CAST(NULL AS INT64)                                        AS procedure_occurrence_id,
  CAST(NULL AS INT64)                                        AS device_exposure_id,
  meta.channel_name                                          AS waveform_channel_source_value,
  
  -- Map channel_name to channel_concept_id (prefer custom concept_code, then custom concept_name, then standard Athena mappings)
  COALESCE(vc_channel_custom_code.concept_id, vc_channel_custom_name.concept_id, vc_channel_voc.concept_id, vc_channel_syn_full.concept_id, vc_channel_syn_parsed.concept_id, 0)  AS channel_concept_id,
  
  -- Map metadata_type to metadata_concept_id
  meta.metadata_type                                         AS metadata_source_value,
  COALESCE(vc_metadata.concept_id, 0)                        AS metadata_concept_id,
  
  meta.value_as_number                                       AS value_as_number,
  meta.value_as_concept_id                                   AS value_as_concept_id,
  meta.value_as_string                                       AS value_as_string,
  
  -- Map unit_source_value to unit_concept_id (hardcoded for 'samples', then try name/code)
  COALESCE(
    CASE WHEN UPPER(meta.unit_source_value) = 'SAMPLES' THEN 2061509816 END,
    vc_unit_name.concept_id, 
    vc_unit_code.concept_id
  ) AS unit_concept_id,
  meta.unit_source_value                                          AS unit_source_value
  
FROM
    channel_metadata_unpivoted meta

-- Join 1w: Map channel_name to channel_concept_id from custom vocab concept_code (preferred)
LEFT JOIN
  (SELECT DISTINCT concept_id, concept_code
   FROM @etl_project.@etl_dataset.voc_concept
   WHERE vocabulary_id IN ('WAVEFORM', 'MIMIC4')
     AND domain_id IN ('Waveform Metadata', 'Measurement', 'Observation')
   QUALIFY ROW_NUMBER() OVER (PARTITION BY concept_code ORDER BY concept_id) = 1
  ) vc_channel_custom_code
      ON UPPER(vc_channel_custom_code.concept_code) = UPPER(meta.channel_name)

-- Join 1x: Map channel_name to channel_concept_id from custom vocab concept_name
LEFT JOIN
  (SELECT DISTINCT concept_id, concept_name
   FROM @etl_project.@etl_dataset.voc_concept
   WHERE vocabulary_id IN ('WAVEFORM', 'MIMIC4')
     AND domain_id IN ('Waveform Metadata', 'Measurement', 'Observation')
   QUALIFY ROW_NUMBER() OVER (PARTITION BY concept_name ORDER BY concept_id) = 1
  ) vc_channel_custom_name
      ON UPPER(vc_channel_custom_name.concept_name) = UPPER(meta.channel_name)
    
-- Join 1a: Map channel_name to channel_concept_id by concept_name (Athena vocabulary)
LEFT JOIN
  (SELECT DISTINCT concept_id, concept_name
   FROM @etl_project.@etl_dataset.voc_concept
   WHERE domain_id IN ('Waveform Metadata', 'Measurement', 'Observation')
     AND standard_concept = 'S'
   QUALIFY ROW_NUMBER() OVER (PARTITION BY concept_name ORDER BY concept_id) = 1
  ) vc_channel_voc
      ON UPPER(vc_channel_voc.concept_name) = UPPER(meta.channel_name)
      
-- Join 1b: Map channel_name to channel_concept_id via full concept_synonym match (Athena vocabulary)
LEFT JOIN
    (SELECT DISTINCT 
            UPPER(syn.concept_synonym_name) AS concept_synonym_name,
            c.concept_id
     FROM @etl_project.@etl_dataset.voc_concept_synonym syn
     INNER JOIN @etl_project.@etl_dataset.voc_concept c
       ON c.concept_id = syn.concept_id
       AND c.domain_id IN ('Waveform Metadata', 'Measurement', 'Observation')
       AND c.standard_concept = 'S'
     QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(syn.concept_synonym_name) ORDER BY c.concept_id) = 1
    ) vc_channel_syn_full
      ON vc_channel_syn_full.concept_synonym_name = UPPER(meta.channel_name)
      
-- Join 1c: Map channel_name to channel_concept_id via parsed concept_synonym (Athena vocabulary)
-- Extracts abbreviation from synonyms like "ABP - Arterial blood pressure" by matching before the " - "
LEFT JOIN
    (SELECT DISTINCT 
            UPPER(TRIM(SPLIT(syn.concept_synonym_name, ' - ')[OFFSET(0)])) AS parsed_value,
            c.concept_id
     FROM @etl_project.@etl_dataset.voc_concept_synonym syn
     INNER JOIN @etl_project.@etl_dataset.voc_concept c
       ON c.concept_id = syn.concept_id
       AND c.domain_id IN ('Waveform Metadata', 'Measurement', 'Observation')
       AND c.standard_concept = 'S'
     QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(SPLIT(syn.concept_synonym_name, ' - ')[OFFSET(0)])) ORDER BY c.concept_id) = 1
    ) vc_channel_syn_parsed
      ON vc_channel_syn_parsed.parsed_value = UPPER(meta.channel_name)
      
-- Join 2: Map metadata_type (SAMPLERATE, GAIN, etc) to metadata_concept_id
LEFT JOIN
  (SELECT DISTINCT concept_id, concept_name
   FROM @etl_project.@etl_dataset.voc_concept
   WHERE domain_id = 'Waveform Metadata'
   QUALIFY ROW_NUMBER() OVER (PARTITION BY concept_name ORDER BY concept_id) = 1
  ) vc_metadata
      ON UPPER(vc_metadata.concept_name) = UPPER(meta.metadata_type)
      
-- Join 3a: Map unit_source_value to unit_concept_id by concept_name
LEFT JOIN
  (SELECT DISTINCT concept_id, UPPER(concept_name) AS concept_name
   FROM @etl_project.@etl_dataset.voc_concept
   WHERE vocabulary_id IN ('WAVEFORM', 'MIMIC4', 'UCUM', 'SNOMED')
   QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(concept_name) ORDER BY concept_id) = 1
  ) vc_unit_name
      ON vc_unit_name.concept_name = UPPER(meta.unit_source_value)
      
-- Join 3b: Map unit_source_value to unit_concept_id by concept_code
LEFT JOIN
  (SELECT DISTINCT concept_id, UPPER(concept_code) AS concept_code
   FROM @etl_project.@etl_dataset.voc_concept
   WHERE vocabulary_id IN ('WAVEFORM', 'MIMIC4', 'UCUM', 'SNOMED')
   QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(concept_code) ORDER BY concept_id) = 1
  ) vc_unit_code
      ON vc_unit_code.concept_code = UPPER(meta.unit_source_value)
;
