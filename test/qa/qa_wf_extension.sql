-- 1. Duplicate primary keys. All three queries must return zero rows.
DECLARE duplicate_waveform_occurrence_ids INT64;
SET duplicate_waveform_occurrence_ids = (
  SELECT COUNT(*) FROM (
    SELECT waveform_occurrence_id
    FROM @etl_project.@etl_dataset.cdm_waveform_occurrence
    GROUP BY waveform_occurrence_id
    HAVING COUNT(*) > 1
  )
);
ASSERT duplicate_waveform_occurrence_ids = 0 AS 'duplicate waveform_occurrence_id values';

DECLARE duplicate_waveform_registry_ids INT64;
SET duplicate_waveform_registry_ids = (
  SELECT COUNT(*) FROM (
    SELECT waveform_registry_id
    FROM @etl_project.@etl_dataset.cdm_waveform_registry
    GROUP BY waveform_registry_id
    HAVING COUNT(*) > 1
  )
);
ASSERT duplicate_waveform_registry_ids = 0 AS 'duplicate waveform_registry_id values';

DECLARE duplicate_waveform_channel_metadata_ids INT64;
SET duplicate_waveform_channel_metadata_ids = (
  SELECT COUNT(*) FROM (
    SELECT waveform_channel_metadata_id
    FROM @etl_project.@etl_dataset.cdm_waveform_channel_metadata
    GROUP BY waveform_channel_metadata_id
    HAVING COUNT(*) > 1
  )
);
ASSERT duplicate_waveform_channel_metadata_ids = 0 AS 'duplicate waveform_channel_metadata_id values';

-- 2. Missing occurrence foreign keys. Must return zero rows.
DECLARE orphan_registry_rows INT64;
SET orphan_registry_rows = (
  SELECT COUNT(*)
  FROM @etl_project.@etl_dataset.cdm_waveform_registry r
  LEFT JOIN @etl_project.@etl_dataset.cdm_waveform_occurrence o
    ON o.waveform_occurrence_id = r.waveform_occurrence_id
  WHERE o.waveform_occurrence_id IS NULL
);
ASSERT orphan_registry_rows = 0 AS 'orphan registry rows';

-- 3. Missing registry foreign keys. Must return zero rows.
DECLARE orphan_channel_metadata_rows INT64;
SET orphan_channel_metadata_rows = (
  SELECT COUNT(*)
  FROM @etl_project.@etl_dataset.cdm_waveform_channel_metadata m
  LEFT JOIN @etl_project.@etl_dataset.cdm_waveform_registry r
    ON r.waveform_registry_id = m.waveform_registry_id
  WHERE r.waveform_registry_id IS NULL
);
ASSERT orphan_channel_metadata_rows = 0 AS 'orphan channel metadata rows';

-- 4. Invalid preceding occurrence references. Must return zero rows.
DECLARE invalid_preceding_occurrence_refs INT64;
SET invalid_preceding_occurrence_refs = (
  SELECT COUNT(*)
  FROM @etl_project.@etl_dataset.cdm_waveform_occurrence o
  LEFT JOIN @etl_project.@etl_dataset.cdm_waveform_occurrence p
    ON p.waveform_occurrence_id = o.preceding_waveform_occurrence_id
  WHERE o.preceding_waveform_occurrence_id IS NOT NULL
    AND p.waveform_occurrence_id IS NULL
);
ASSERT invalid_preceding_occurrence_refs = 0 AS 'invalid preceding waveform occurrence references';

-- 5. Invalid occurrence intervals. Must return zero rows.
DECLARE invalid_occurrence_intervals INT64;
SET invalid_occurrence_intervals = (
  SELECT COUNT(*)
  FROM @etl_project.@etl_dataset.cdm_waveform_occurrence
  WHERE waveform_occurrence_start_datetime IS NULL
     OR waveform_occurrence_end_datetime IS NULL
     OR waveform_occurrence_end_datetime < waveform_occurrence_start_datetime
);
ASSERT invalid_occurrence_intervals = 0 AS 'invalid waveform occurrence intervals';

-- 6. Invalid registry intervals. Must return zero rows.
DECLARE invalid_registry_intervals INT64;
SET invalid_registry_intervals = (
  SELECT COUNT(*)
  FROM @etl_project.@etl_dataset.cdm_waveform_registry
  WHERE waveform_file_start_datetime IS NULL
     OR waveform_file_end_datetime IS NULL
     OR waveform_file_end_datetime < waveform_file_start_datetime
);
ASSERT invalid_registry_intervals = 0 AS 'invalid waveform registry intervals';

-- 7. Registry timestamps outside the parent occurrence. Must return zero rows unless a documented tolerance or exception policy applies.
DECLARE registry_outside_occurrence_intervals INT64;
SET registry_outside_occurrence_intervals = (
  SELECT COUNT(*)
  FROM @etl_project.@etl_dataset.cdm_waveform_registry r
  JOIN @etl_project.@etl_dataset.cdm_waveform_occurrence o
    ON o.waveform_occurrence_id = r.waveform_occurrence_id
  WHERE r.waveform_file_start_datetime < o.waveform_occurrence_start_datetime
     OR r.waveform_file_end_datetime > o.waveform_occurrence_end_datetime
);
ASSERT registry_outside_occurrence_intervals = 0 AS 'registry timestamps outside parent occurrence window';

-- 8. Person and visit inconsistencies. Must return zero rows when registry context is intended to be inherited directly.
DECLARE registry_context_mismatches INT64;
SET registry_context_mismatches = (
  SELECT COUNT(*)
  FROM @etl_project.@etl_dataset.cdm_waveform_registry r
  JOIN @etl_project.@etl_dataset.cdm_waveform_occurrence o
    ON o.waveform_occurrence_id = r.waveform_occurrence_id
  WHERE r.person_id != o.person_id
     OR r.visit_occurrence_id != o.visit_occurrence_id
     OR (r.visit_detail_id IS DISTINCT FROM o.visit_detail_id)
);
ASSERT registry_context_mismatches = 0 AS 'registry context mismatches parent occurrence';

-- 9. File-count inconsistencies. Must return zero rows.
DECLARE inconsistent_occurrence_file_counts INT64;
SET inconsistent_occurrence_file_counts = (
  SELECT COUNT(*) FROM (
    SELECT
      o.waveform_occurrence_id,
      o.num_of_files,
      COUNT(r.waveform_registry_id) AS registry_file_count
    FROM @etl_project.@etl_dataset.cdm_waveform_occurrence o
    LEFT JOIN @etl_project.@etl_dataset.cdm_waveform_registry r
      ON r.waveform_occurrence_id = o.waveform_occurrence_id
    GROUP BY o.waveform_occurrence_id, o.num_of_files
    HAVING o.num_of_files != COUNT(r.waveform_registry_id)
  )
);
ASSERT inconsistent_occurrence_file_counts = 0 AS 'waveform_occurrence.num_of_files inconsistent with registry row count';

-- 10. Duplicate target files. Must return zero rows unless duplicate logical registration of one physical file is explicitly intended and documented.
DECLARE duplicate_target_file_uris INT64;
SET duplicate_target_file_uris = (
  SELECT COUNT(*) FROM (
    SELECT waveform_target_file_uri
    FROM @etl_project.@etl_dataset.cdm_waveform_registry
    GROUP BY waveform_target_file_uri
    HAVING COUNT(*) > 1
  )
);
ASSERT duplicate_target_file_uris = 0 AS 'duplicate waveform_target_file_uri values';

-- 11a. Missing required occurrence concepts. Must return zero rows for the final production tables.
DECLARE missing_occurrence_required_concepts INT64;
SET missing_occurrence_required_concepts = (
  SELECT COUNT(*)
  FROM @etl_project.@etl_dataset.cdm_waveform_occurrence
  WHERE waveform_occurrence_concept_id IS NULL
     OR waveform_occurrence_concept_id = 0
);
ASSERT missing_occurrence_required_concepts = 0 AS 'missing required waveform occurrence concepts';

-- 11b. Missing required channel concepts. Temporarily disabled pending vocabulary updates:
-- CVB issue #31: https://github.com/TuftsCTSI/CVB/issues/31
-- DECLARE missing_channel_required_concepts INT64;
-- SET missing_channel_required_concepts = (
--   SELECT COUNT(*)
--   FROM @etl_project.@etl_dataset.cdm_waveform_channel_metadata
--   WHERE channel_concept_id IS NULL
--      OR channel_concept_id = 0
-- );
-- ASSERT missing_channel_required_concepts = 0 AS 'missing required channel concepts';

-- 11c. Missing required metadata concepts. Temporarily disabled pending vocabulary updates:
-- CVB issue #26: https://github.com/TuftsCTSI/CVB/issues/26
-- DECLARE missing_metadata_required_concepts INT64;
-- SET missing_metadata_required_concepts = (
--   SELECT COUNT(*)
--   FROM @etl_project.@etl_dataset.cdm_waveform_channel_metadata
--   WHERE metadata_concept_id IS NULL
--      OR metadata_concept_id = 0
-- );
-- ASSERT missing_metadata_required_concepts = 0 AS 'missing required metadata concepts';

-- 12. Invalid or missing vocabulary concepts. Must return zero rows.
DECLARE invalid_or_missing_vocabulary_concepts INT64;
SET invalid_or_missing_vocabulary_concepts = (
  SELECT COUNT(*)
  FROM (
    SELECT DISTINCT x.concept_id
    FROM (
      SELECT waveform_occurrence_concept_id AS concept_id
      FROM @etl_project.@etl_dataset.cdm_waveform_occurrence

    UNION DISTINCT

      SELECT waveform_format_concept_id
      FROM @etl_project.@etl_dataset.cdm_waveform_occurrence
      WHERE waveform_format_concept_id IS NOT NULL

    UNION DISTINCT

      SELECT file_extension_concept_id
      FROM @etl_project.@etl_dataset.cdm_waveform_registry
      WHERE file_extension_concept_id IS NOT NULL

    UNION DISTINCT

      SELECT channel_concept_id
      FROM @etl_project.@etl_dataset.cdm_waveform_channel_metadata

    UNION DISTINCT

      SELECT metadata_concept_id
      FROM @etl_project.@etl_dataset.cdm_waveform_channel_metadata

    UNION DISTINCT

      SELECT unit_concept_id
      FROM @etl_project.@etl_dataset.cdm_waveform_channel_metadata
      WHERE unit_concept_id IS NOT NULL
    ) x
    LEFT JOIN @etl_project.@etl_dataset.voc_concept c
      ON c.concept_id = x.concept_id
    WHERE x.concept_id != 0
      AND (
        c.concept_id IS NULL
        OR c.invalid_reason IS NOT NULL
      )
  )
);
ASSERT invalid_or_missing_vocabulary_concepts = 0 AS 'invalid or missing vocabulary concepts referenced by waveform extension tables';

-- 13. Invalid unit concepts. Must return zero rows unless a documented custom-unit exception exists.
DECLARE invalid_unit_concepts INT64;
SET invalid_unit_concepts = (
  SELECT COUNT(*)
  FROM @etl_project.@etl_dataset.cdm_waveform_channel_metadata m
  LEFT JOIN @etl_project.@etl_dataset.voc_concept c
    ON c.concept_id = m.unit_concept_id
  WHERE m.unit_concept_id IS NOT NULL
    AND (
      c.concept_id IS NULL
      OR c.domain_id != 'Unit'
      OR c.standard_concept != 'S'
      OR c.invalid_reason IS NOT NULL
    )
);
ASSERT invalid_unit_concepts = 0 AS 'invalid unit_concept_id values';

-- 14. Empty channel metadata records. Must return zero rows.
DECLARE empty_channel_metadata_records INT64;
SET empty_channel_metadata_records = (
  SELECT COUNT(*)
  FROM @etl_project.@etl_dataset.cdm_waveform_channel_metadata
  WHERE value_as_number IS NULL
    AND value_as_concept_id IS NULL
    AND value_as_string IS NULL
    AND unit_concept_id IS NULL
    AND unit_source_value IS NULL
);
ASSERT empty_channel_metadata_records = 0 AS 'empty channel metadata records';

-- 15. Duplicate channel metadata grain. Must return zero rows unless repeated metadata values are explicitly modeled.
DECLARE duplicate_channel_metadata_grain INT64;
SET duplicate_channel_metadata_grain = (
  SELECT COUNT(*) FROM (
    WITH staged_channel_metadata AS (
      SELECT
        r.waveform_registry_id,
        wc.channel_index,
        'AMPLITUDE' AS metadata_source_value
      FROM @etl_project.@etl_dataset.waveform_channels wc
      JOIN @etl_project.@etl_dataset.cdm_waveform_registry r
        ON r.waveform_target_file_uri = wc.trg_file
      WHERE wc.sample_units IS NOT NULL

      UNION ALL

      SELECT
        r.waveform_registry_id,
        wc.channel_index,
        'SAMPLERATE' AS metadata_source_value
      FROM @etl_project.@etl_dataset.waveform_channels wc
      JOIN @etl_project.@etl_dataset.cdm_waveform_registry r
        ON r.waveform_target_file_uri = wc.trg_file
      WHERE wc.sample_rate IS NOT NULL

      UNION ALL

      SELECT
        r.waveform_registry_id,
        wc.channel_index,
        'RESOLUTION' AS metadata_source_value
      FROM @etl_project.@etl_dataset.waveform_channels wc
      JOIN @etl_project.@etl_dataset.cdm_waveform_registry r
        ON r.waveform_target_file_uri = wc.trg_file
      WHERE wc.gain IS NOT NULL

      UNION ALL

      SELECT
        r.waveform_registry_id,
        wc.channel_index,
        'SEGMENTLENGTH' AS metadata_source_value
      FROM @etl_project.@etl_dataset.waveform_channels wc
      JOIN @etl_project.@etl_dataset.cdm_waveform_registry r
        ON r.waveform_target_file_uri = wc.trg_file
      WHERE wc.segment_length IS NOT NULL
    )
    SELECT
      waveform_registry_id,
      channel_index,
      metadata_source_value
    FROM staged_channel_metadata
    GROUP BY
      waveform_registry_id,
      channel_index,
      metadata_source_value
    HAVING COUNT(*) > 1
  )
);
ASSERT duplicate_channel_metadata_grain = 0 AS 'duplicate channel metadata at intended natural grain';
