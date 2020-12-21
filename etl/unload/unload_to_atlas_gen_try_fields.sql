-- bq_cdm_to_atlas generated script --

-- Unload to ATLAS-- Copy Vocabulary tables

CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.concept AS 
SELECT * FROM `@etl_project`.@etl_dataset.voc_concept;

-- Copy CDM tables

CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.cohort_definition AS 
SELECT * FROM `@etl_project`.@etl_dataset.cdm_cohort_definition;
