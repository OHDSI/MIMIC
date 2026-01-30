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
    'SAMPLERATE' AS metadata_type,
    sample_rate AS value_as_number,
    NULL AS value_as_concept_id,
    NULL AS value_as_string
  FROM @etl_project.@etl_dataset.waveform_channels
  
  UNION ALL

  SELECT
    person_id, visit_occurrence_id, group_id, trg_file, channel_name, sample_units,
    'SAMPLERATEUNITS' AS metadata_type,
    NULL AS value_as_number,
    NULL AS value_as_concept_id,
    sample_rate_units AS value_as_string
  FROM @etl_project.@etl_dataset.waveform_channels

  UNION ALL

  SELECT 
    person_id, visit_occurrence_id, group_id, trg_file, channel_name, sample_units,
    'GAIN' AS metadata_type,
    gain AS value_as_number,
    NULL AS value_as_concept_id,
    NULL AS value_as_string
  FROM @etl_project.@etl_dataset.waveform_channels
  
  UNION ALL

  SELECT
    person_id, visit_occurrence_id, group_id, trg_file, channel_name, sample_units,
    'GAINUNITS' AS metadata_type,
    NULL AS value_as_number,
    NULL AS value_as_concept_id,
    gain_units AS value_as_string
  FROM @etl_project.@etl_dataset.waveform_channels

  UNION ALL
  
  SELECT
    person_id, visit_occurrence_id, group_id, trg_file, channel_name, sample_units,
    'SEGMENTLENGTH' AS metadata_type,
    segment_length AS value_as_number,
    NULL AS value_as_concept_id,
    NULL AS value_as_string
  FROM @etl_project.@etl_dataset.waveform_channels
)
SELECT
  `@etl_project.@etl_dataset.obf_id_str`(CONCAT(cmd.trg_file, cmd.channel_name), 32)  AS waveform_channel_metadata_id,
  `@etl_project.@etl_dataset.obf_id_str`(cmd.trg_file, 32)  AS waveform_registry_id,
  CAST(NULL AS INT64)                                        AS procedure_occurrence_id,
  CAST(NULL AS INT64)                                        AS device_exposure_id,
  cmd.channel_name                                           AS waveform_channel_source_value,
  
  -- Map channel_name to channel_concept_id
  COALESCE(vc_channel.concept_id, 0)                         AS channel_concept_id,
  
  -- Map metadata_type to metadata_concept_id
  cmd.metadata_type                                          AS metadata_source_value,
  COALESCE(vc_metadata.concept_id, 0)                        AS metadata_concept_id,
  
  cmd.value_as_number                                        AS value_as_number,
  cmd.value_as_concept_id                                    AS value_as_concept_id,
  cmd.value_as_string                                        AS value_as_string,
  
  -- Map sample_units to unit_concept_id (prefer Athena UCUM, fallback to Waveform)
  COALESCE(vc_unit_athena.concept_id, vc_unit_waveform.concept_id) AS unit_concept_id,
  cmd.sample_units                                           AS unit_source_value
  
FROM
    channel_metadata_unpivoted cmd
    
-- Join 1: Map channel_name to channel_concept_id
LEFT JOIN
    @wf_project.@wf_dataset.concept vc_channel
      ON UPPER(vc_channel.concept_name) = UPPER(cmd.channel_name)
      AND vc_channel.domain_id = 'Waveform Metadata'
      
-- Join 2: Map metadata_type (SAMPLERATE, GAIN, etc) to metadata_concept_id
LEFT JOIN
    @wf_project.@wf_dataset.concept vc_metadata
      ON UPPER(vc_metadata.concept_name) = UPPER(cmd.metadata_type)
      AND vc_metadata.domain_id = 'Waveform Metadata'
      
-- Join 3: Map sample_units to unit_concept_id (try Athena UCUM first, then Waveform)
LEFT JOIN
    @voc_project.@voc_dataset.voc_concept vc_unit_athena
      ON UPPER(vc_unit_athena.concept_code) = UPPER(cmd.sample_units)
      AND vc_unit_athena.vocabulary_id IN ('UCUM', 'SNOMED')
LEFT JOIN
    @wf_project.@wf_dataset.concept vc_unit_waveform
      ON UPPER(vc_unit_waveform.concept_name) = UPPER(cmd.sample_units)
      AND vc_unit_waveform.domain_id = 'Unit'
;