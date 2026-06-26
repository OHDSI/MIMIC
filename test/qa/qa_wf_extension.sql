-- 1. Duplicate primary keys. All three queries must return zero rows.
SELECT waveform_occurrence_id
FROM @etl_project.@etl_dataset.cdm_waveform_occurrence
GROUP BY waveform_occurrence_id
HAVING COUNT(*) > 1;
SELECT waveform_registry_id
FROM @etl_project.@etl_dataset.cdm_waveform_registry
GROUP BY waveform_registry_id
HAVING COUNT(*) > 1;
SELECT waveform_channel_metadata_id
FROM @etl_project.@etl_dataset.cdm_waveform_channel_metadata
GROUP BY waveform_channel_metadata_id
HAVING COUNT(*) > 1;

-- 2. Missing occurrence foreign keys. Must return zero rows.
SELECT r.*
FROM @etl_project.@etl_dataset.cdm_waveform_registry r
LEFT JOIN @etl_project.@etl_dataset.cdm_waveform_occurrence o
  ON o.waveform_occurrence_id = r.waveform_occurrence_id
WHERE o.waveform_occurrence_id IS NULL;

-- 3. Missing registry foreign keys. Must return zero rows.
SELECT m.*
FROM @etl_project.@etl_dataset.cdm_waveform_channel_metadata m
LEFT JOIN @etl_project.@etl_dataset.cdm_waveform_registry r
  ON r.waveform_registry_id = m.waveform_registry_id
WHERE r.waveform_registry_id IS NULL;

-- 4. Invalid preceding occurrence references. Must return zero rows.
SELECT o.*
FROM @etl_project.@etl_dataset.cdm_waveform_occurrence o
LEFT JOIN @etl_project.@etl_dataset.cdm_waveform_occurrence p
  ON p.waveform_occurrence_id =
     o.preceding_waveform_occurrence_id
WHERE o.preceding_waveform_occurrence_id IS NOT NULL
  AND p.waveform_occurrence_id IS NULL;

-- 5. Invalid occurrence intervals. Must return zero rows.
SELECT *
FROM @etl_project.@etl_dataset.cdm_waveform_occurrence
WHERE waveform_occurrence_start_datetime IS NULL
   OR waveform_occurrence_end_datetime IS NULL
   OR waveform_occurrence_end_datetime
      < waveform_occurrence_start_datetime;

-- 6. Invalid registry intervals. Must return zero rows.
SELECT *
FROM @etl_project.@etl_dataset.cdm_waveform_registry
WHERE waveform_file_start_datetime IS NULL
   OR waveform_file_end_datetime IS NULL
   OR waveform_file_end_datetime
      < waveform_file_start_datetime;

-- 7. Registry timestamps outside the parent occurrence. Must return zero rows unless a documented tolerance or exception policy applies.
SELECT
    r.*,
    o.waveform_occurrence_start_datetime,
    o.waveform_occurrence_end_datetime
FROM @etl_project.@etl_dataset.cdm_waveform_registry r
JOIN @etl_project.@etl_dataset.cdm_waveform_occurrence o
  ON o.waveform_occurrence_id = r.waveform_occurrence_id
WHERE r.waveform_file_start_datetime
          < o.waveform_occurrence_start_datetime
   OR r.waveform_file_end_datetime
          > o.waveform_occurrence_end_datetime;

-- 8. Person and visit inconsistencies. Must return zero rows when registry context is intended to be inherited directly.
SELECT r.*
FROM @etl_project.@etl_dataset.cdm_waveform_registry r
JOIN @etl_project.@etl_dataset.cdm_waveform_occurrence o
  ON o.waveform_occurrence_id = r.waveform_occurrence_id
WHERE r.person_id != o.person_id
   OR r.visit_occurrence_id != o.visit_occurrence_id
   OR (
       r.visit_detail_id IS DISTINCT FROM o.visit_detail_id
   );

9. File-count inconsistencies. Must return zero rows.
SELECT
    o.waveform_occurrence_id,
    o.num_of_files,
    COUNT(r.waveform_registry_id) AS registry_file_count
FROM @etl_project.@etl_dataset.cdm_waveform_occurrence o
LEFT JOIN @etl_project.@etl_dataset.cdm_waveform_registry r
  ON r.waveform_occurrence_id = o.waveform_occurrence_id
GROUP BY
    o.waveform_occurrence_id,
    o.num_of_files
HAVING o.num_of_files != COUNT(r.waveform_registry_id);

-- 10. Duplicate target files. Must return zero rows unless duplicate logical registration of one physical file is explicitly intended and documented.
SELECT
    waveform_target_file_uri,
    COUNT(*) AS registry_row_count
FROM @etl_project.@etl_dataset.cdm_waveform_registry
GROUP BY waveform_target_file_uri
HAVING COUNT(*) > 1;

-- 11. Missing required concepts. These should return zero rows for the final production tables.
SELECT *
FROM @etl_project.@etl_dataset.cdm_waveform_occurrence
WHERE waveform_occurrence_concept_id IS NULL
   OR waveform_occurrence_concept_id = 0;
SELECT *
FROM @etl_project.@etl_dataset.cdm_waveform_channel_metadata
WHERE channel_concept_id IS NULL
   OR channel_concept_id = 0
   OR metadata_concept_id IS NULL
   OR metadata_concept_id = 0;

-- 12. Invalid or missing vocabulary concepts. Must return zero rows.
SELECT DISTINCT
    x.concept_id
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
  );

-- 13. Invalid unit concepts. Must return zero rows unless a documented custom-unit exception exists.
SELECT DISTINCT
    m.unit_concept_id,
    c.concept_name,
    c.domain_id,
    c.standard_concept,
    c.invalid_reason
FROM @etl_project.@etl_dataset.cdm_waveform_channel_metadata m
LEFT JOIN @etl_project.@etl_dataset.voc_concept c
  ON c.concept_id = m.unit_concept_id
WHERE m.unit_concept_id IS NOT NULL
  AND (
      c.concept_id IS NULL
      OR c.domain_id != 'Unit'
      OR c.standard_concept != 'S'
      OR c.invalid_reason IS NOT NULL
  );

-- 14. Empty channel metadata records. Review all returned records. An amplitude record may legitimately have null value fields, but it should still contain its amplitude unit.
SELECT *
FROM @etl_project.@etl_dataset.cdm_waveform_channel_metadata
WHERE value_as_number IS NULL
  AND value_as_concept_id IS NULL
  AND value_as_string IS NULL
  AND unit_concept_id IS NULL
  AND unit_source_value IS NULL;

-- 15. Duplicate channel metadata grain. Must return zero rows unless repeated metadata values are explicitly modeled.
After introducing a stable channel index or identifier:
SELECT
    waveform_registry_id,
    channel_identifier,
    metadata_source_value,
    COUNT(*) AS metadata_row_count
FROM staged_or_final_channel_metadata
GROUP BY
    waveform_registry_id,
    channel_identifier,
    metadata_source_value
HAVING COUNT(*) > 1;
