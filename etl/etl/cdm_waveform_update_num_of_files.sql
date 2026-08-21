-- Set num_of_files from actual registry rows
UPDATE @etl_project.@etl_dataset.cdm_waveform_occurrence wo
SET num_of_files = r.cnt
FROM (
  SELECT waveform_occurrence_id, COUNT(*) AS cnt
  FROM @etl_project.@etl_dataset.cdm_waveform_registry
  GROUP BY waveform_occurrence_id
) r
WHERE wo.waveform_occurrence_id = r.waveform_occurrence_id;