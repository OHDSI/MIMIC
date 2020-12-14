-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- Refresh Vocabularies for OMOP CDM Conversion
-- -------------------------------------------------------------------

-- ---------------------------------------------------------------
-- custom concept stats
-- to sync with custom_mapping_csv/custom_mapping_list.csv
-- ---------------------------------------------------------------
SELECT 
  vocabulary_id AS source_vocabulary_id, 
  min(concept_id) AS min_concept_id, 
  max(concept_id) AS max_concept_id, 
  count(*) AS row_count
FROM `bq_target_project.bq_target_dataset.concept` 
WHERE concept_id >= 2000000000
GROUP BY vocabulary_id 
ORDER BY vocabulary_id 
;

