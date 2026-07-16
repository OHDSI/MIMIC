-- waveform_feature QA checks. Enable in workflow_waveforms_qa.conf when waveform_feature ETL/output is available.

-- 1. Duplicate waveform_feature primary keys. Must return zero rows.
DECLARE duplicate_waveform_feature_ids INT64;
SET duplicate_waveform_feature_ids = (
  SELECT COUNT(*) FROM (
    SELECT waveform_feature_id
    FROM @etl_project.@etl_dataset.cdm_waveform_feature
    GROUP BY waveform_feature_id
    HAVING COUNT(*) > 1
  )
);
ASSERT duplicate_waveform_feature_ids = 0 AS 'duplicate waveform_feature_id values';

-- 2. Missing occurrence foreign keys from waveform_feature. Must return zero rows.
DECLARE orphan_feature_occurrence_rows INT64;
SET orphan_feature_occurrence_rows = (
  SELECT COUNT(*)
  FROM @etl_project.@etl_dataset.cdm_waveform_feature f
  LEFT JOIN @etl_project.@etl_dataset.cdm_waveform_occurrence o
    ON o.waveform_occurrence_id = f.waveform_occurrence_id
  WHERE o.waveform_occurrence_id IS NULL
);
ASSERT orphan_feature_occurrence_rows = 0 AS 'orphan waveform_feature rows by waveform_occurrence_id';

-- 3. Missing registry foreign keys from waveform_feature. Must return zero rows.
DECLARE orphan_feature_registry_rows INT64;
SET orphan_feature_registry_rows = (
  SELECT COUNT(*)
  FROM @etl_project.@etl_dataset.cdm_waveform_feature f
  LEFT JOIN @etl_project.@etl_dataset.cdm_waveform_registry r
    ON r.waveform_registry_id = f.waveform_registry_id
  WHERE r.waveform_registry_id IS NULL
);
ASSERT orphan_feature_registry_rows = 0 AS 'orphan waveform_feature rows by waveform_registry_id';

-- 4. Missing channel metadata foreign keys from waveform_feature. Must return zero rows.
DECLARE orphan_feature_channel_metadata_rows INT64;
SET orphan_feature_channel_metadata_rows = (
  SELECT COUNT(*)
  FROM @etl_project.@etl_dataset.cdm_waveform_feature f
  LEFT JOIN @etl_project.@etl_dataset.cdm_waveform_channel_metadata m
    ON m.waveform_channel_metadata_id = f.waveform_channel_metadata_id
  WHERE m.waveform_channel_metadata_id IS NULL
);
ASSERT orphan_feature_channel_metadata_rows = 0 AS 'orphan waveform_feature rows by waveform_channel_metadata_id';
