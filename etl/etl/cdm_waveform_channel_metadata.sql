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

-- Preflight 1: each channel trg_file must resolve to exactly one registry row (1:1)
DECLARE bad_registry_link INT64;
SET bad_registry_link = (
  SELECT COUNT(*) FROM (
    SELECT meta.trg_file
    FROM @etl_project.@etl_dataset.waveform_channels meta
    LEFT JOIN @etl_project.@etl_dataset.cdm_waveform_registry r
      ON r.waveform_target_file_uri = meta.trg_file
    GROUP BY meta.trg_file
    HAVING COUNTIF(r.waveform_registry_id IS NULL) > 0
       OR COUNT(DISTINCT r.waveform_registry_id) != 1
  )
);
ASSERT bad_registry_link = 0 AS 'each channel trg_file must resolve to exactly one registry row';

-- Preflight 2: channel person/visit must be consistent with occurrence via registry
DECLARE person_visit_mismatch INT64;
SET person_visit_mismatch = (
  SELECT COUNT(*) FROM (
    SELECT 1
    FROM @etl_project.@etl_dataset.waveform_channels meta
    JOIN @etl_project.@etl_dataset.cdm_waveform_registry r
      ON r.waveform_target_file_uri = meta.trg_file
    JOIN @etl_project.@etl_dataset.cdm_waveform_occurrence o
      ON o.waveform_occurrence_id = r.waveform_occurrence_id
    WHERE meta.person_id != o.person_id
       OR meta.visit_occurrence_id != o.visit_occurrence_id
  )
);
ASSERT person_visit_mismatch = 0 AS 'channel staging person/visit must match occurrence via registry';

---- Preflight 3: channel_index must uniquely identify channels within each registry file
DECLARE duplicate_channel_index INT64;
SET duplicate_channel_index = (
  SELECT COUNT(*) FROM (
    SELECT
      reg.waveform_registry_id,
      m.channel_index
    FROM @etl_project.@etl_dataset.waveform_channels m
    JOIN @etl_project.@etl_dataset.cdm_waveform_registry reg
      ON reg.waveform_target_file_uri = m.trg_file
    GROUP BY
      reg.waveform_registry_id,
      m.channel_index
    HAVING COUNT(*) > 1
  )
);
ASSERT duplicate_channel_index = 0 AS 'duplicate channel_index within registry file';


-- Build all allowed channel concept candidates once, without applying vocabulary precedence.
-- The tier-specific ASSERTs and final COALESCE below apply WAVEFORM -> MIMIC4 -> Athena precedence.
CREATE TEMP TABLE tmp_channel_candidates_distinct AS
WITH channel_names AS (
  SELECT DISTINCT UPPER(channel_name) AS channel_name_u
  FROM @etl_project.@etl_dataset.waveform_channels
),
channel_candidates AS (
  SELECT
    cn.channel_name_u,
    c.concept_id,
    c.vocabulary_id
  FROM channel_names cn
  JOIN @etl_project.@etl_dataset.voc_concept c
    ON UPPER(c.concept_code) = cn.channel_name_u
   AND c.domain_id IN ('Waveform Metadata', 'Measurement', 'Observation')
   AND c.standard_concept = 'S'
   AND c.invalid_reason IS NULL

  UNION ALL

  SELECT
    cn.channel_name_u,
    c.concept_id,
    c.vocabulary_id
  FROM channel_names cn
  JOIN @etl_project.@etl_dataset.voc_concept c
    ON UPPER(c.concept_name) = cn.channel_name_u
   AND c.domain_id IN ('Waveform Metadata', 'Measurement', 'Observation')
   AND c.standard_concept = 'S'
   AND c.invalid_reason IS NULL

  UNION ALL

  SELECT
    cn.channel_name_u,
    c.concept_id,
    c.vocabulary_id
  FROM channel_names cn
  JOIN @etl_project.@etl_dataset.voc_concept_synonym syn
    ON UPPER(syn.concept_synonym_name) = cn.channel_name_u
  JOIN @etl_project.@etl_dataset.voc_concept c
    ON c.concept_id = syn.concept_id
   AND c.domain_id IN ('Waveform Metadata', 'Measurement', 'Observation')
   AND c.standard_concept = 'S'
   AND c.invalid_reason IS NULL
)
SELECT DISTINCT
  channel_name_u,
  concept_id,
  vocabulary_id
FROM channel_candidates
;

-- Summarize candidate counts by vocabulary tier. The selected tier is the first tier with any candidates.
CREATE TEMP TABLE tmp_channel_tier_summary AS
SELECT
  channel_name_u,
  CASE
    WHEN vocabulary_id = 'WAVEFORM' THEN 'WAVEFORM'
    WHEN vocabulary_id = 'MIMIC4' THEN 'MIMIC4'
    ELSE 'ATHENA'
  END AS vocabulary_tier,
  ARRAY_AGG(DISTINCT concept_id ORDER BY concept_id) AS concept_ids,
  COUNT(DISTINCT concept_id) AS concept_count
FROM tmp_channel_candidates_distinct
GROUP BY
  channel_name_u,
  vocabulary_tier
;

CREATE TEMP TABLE tmp_channel_selected_tier AS
WITH channel_names AS (
  SELECT DISTINCT UPPER(channel_name) AS channel_name_u
  FROM @etl_project.@etl_dataset.waveform_channels
)
SELECT
  cn.channel_name_u,
  CASE
    WHEN COALESCE(waveform.concept_count, 0) > 0 THEN 'WAVEFORM'
    WHEN COALESCE(mimic4.concept_count, 0) > 0 THEN 'MIMIC4'
    WHEN COALESCE(athena.concept_count, 0) > 0 THEN 'ATHENA'
  END AS selected_tier
FROM channel_names cn
LEFT JOIN tmp_channel_tier_summary waveform
  ON waveform.channel_name_u = cn.channel_name_u
 AND waveform.vocabulary_tier = 'WAVEFORM'
LEFT JOIN tmp_channel_tier_summary mimic4
  ON mimic4.channel_name_u = cn.channel_name_u
 AND mimic4.vocabulary_tier = 'MIMIC4'
LEFT JOIN tmp_channel_tier_summary athena
  ON athena.channel_name_u = cn.channel_name_u
 AND athena.vocabulary_tier = 'ATHENA'
;

-- Preflight 4: selected vocabulary tier must not contain ambiguous channel mappings
DECLARE selected_channel_ambiguity INT64;
SET selected_channel_ambiguity = (
  SELECT COUNT(*)
  FROM tmp_channel_selected_tier selected
  JOIN tmp_channel_tier_summary summary
    ON summary.channel_name_u = selected.channel_name_u
   AND summary.vocabulary_tier = selected.selected_tier
  WHERE summary.concept_count > 1
);
ASSERT selected_channel_ambiguity = 0 AS 'ambiguous selected-tier channel mappings';

INSERT INTO @etl_project.@etl_dataset.cdm_waveform_channel_metadata
WITH channel_metadata_unpivoted AS (
  -- channel_index is source-derived from WFDB channel order and used to disambiguate 
  -- duplicate labels within a file
  SELECT
    person_id, visit_occurrence_id, group_id, trg_file, channel_index, channel_name, sample_units,
    'AMPLITUDE' AS metadata_type,
    CAST(NULL AS FLOAT64) AS value_as_number,
    CAST(NULL AS INT64) AS value_as_concept_id,
    CAST(NULL AS STRING) AS value_as_string,
    sample_units AS unit_source_value
  FROM @etl_project.@etl_dataset.waveform_channels
  WHERE sample_units IS NOT NULL
  
  UNION ALL
  
  SELECT
    person_id, visit_occurrence_id, group_id, trg_file, channel_index, channel_name, sample_units,
    'SAMPLERATE' AS metadata_type,
    sample_rate AS value_as_number,
    CAST(NULL AS INT64) AS value_as_concept_id,
    CAST(NULL AS STRING) AS value_as_string,
    sample_rate_units AS unit_source_value
  FROM @etl_project.@etl_dataset.waveform_channels
  WHERE sample_rate IS NOT NULL

  UNION ALL

  SELECT 
    person_id, visit_occurrence_id, group_id, trg_file, channel_index, channel_name, sample_units,
    'RESOLUTION' AS metadata_type,
    gain AS value_as_number,
    CAST(NULL AS INT64) AS value_as_concept_id,
    CAST(NULL AS STRING) AS value_as_string,
    gain_units AS unit_source_value
  FROM @etl_project.@etl_dataset.waveform_channels
  WHERE gain IS NOT NULL
  
  UNION ALL

  SELECT 
    person_id, visit_occurrence_id, group_id, trg_file, channel_index, channel_name, sample_units,
    'SEGMENTLENGTH' AS metadata_type,
    segment_length AS value_as_number,
    CAST(NULL AS INT64) AS value_as_concept_id,
    CAST(NULL AS STRING) AS value_as_string,
    'samples' AS unit_source_value
  FROM @etl_project.@etl_dataset.waveform_channels
  WHERE segment_length IS NOT NULL
),
channel_map AS (
  SELECT
    selected.channel_name_u,
    summary.concept_ids[SAFE_OFFSET(0)] AS concept_id
  FROM tmp_channel_selected_tier selected
  JOIN tmp_channel_tier_summary summary
    ON summary.channel_name_u = selected.channel_name_u
   AND summary.vocabulary_tier = selected.selected_tier
  WHERE summary.concept_count = 1
)
SELECT
  `@etl_project.@etl_dataset.obf_id_str`(
    TO_JSON_STRING(STRUCT(
      r.waveform_registry_id AS waveform_registry_id,
      meta.channel_index AS channel_index,
      meta.metadata_type AS metadata_type
    )),
    64
  )                                                          AS waveform_channel_metadata_id,
  r.waveform_registry_id                                     AS waveform_registry_id,
  CAST(NULL AS INT64)                                        AS procedure_occurrence_id,
  CAST(NULL AS INT64)                                        AS device_exposure_id,
  meta.channel_name                                          AS waveform_channel_source_value,
  
  -- Map channel_name to channel_concept_id using selected vocabulary tier precedence WAVEFORM -> MIMIC4 -> Athena
  channel_map.concept_id                                   AS channel_concept_id,
  
  -- Map metadata_type to metadata_concept_id
  meta.metadata_type                                         AS metadata_source_value,
  vc_metadata.concept_id                                     AS metadata_concept_id,
  
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

-- Join 0: Resolve waveform_registry_id from populated WAVEFORM_REGISTRY by target URI 
-- (1:1 on trg_file).
JOIN @etl_project.@etl_dataset.cdm_waveform_registry r
  ON r.waveform_target_file_uri = meta.trg_file

-- Join 1: Resolve channel_name to channel_concept_id from selected vocabulary tier
LEFT JOIN channel_map
  ON channel_map.channel_name_u = UPPER(meta.channel_name)
      
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
