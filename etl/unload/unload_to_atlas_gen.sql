-- bq_cdm_to_atlas generated script --

-- Unload to ATLAS-- Copy Vocabulary tables

CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.concept AS 
SELECT * FROM `@etl_project`.@etl_dataset.voc_concept;
CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.vocabulary AS 
SELECT * FROM `@etl_project`.@etl_dataset.voc_vocabulary;
CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.domain AS 
SELECT * FROM `@etl_project`.@etl_dataset.voc_domain;
CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.concept_class AS 
SELECT * FROM `@etl_project`.@etl_dataset.voc_concept_class;
CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.concept_relationship AS 
SELECT * FROM `@etl_project`.@etl_dataset.voc_concept_relationship;
CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.relationship AS 
SELECT * FROM `@etl_project`.@etl_dataset.voc_relationship;
CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.concept_synonym AS 
SELECT * FROM `@etl_project`.@etl_dataset.voc_concept_synonym;
CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.concept_ancestor AS 
SELECT * FROM `@etl_project`.@etl_dataset.voc_concept_ancestor;
CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.source_to_concept_map AS 
SELECT * FROM `@etl_project`.@etl_dataset.voc_source_to_concept_map;
CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.drug_strength AS 
SELECT * FROM `@etl_project`.@etl_dataset.voc_drug_strength;

-- Copy CDM tables

CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.cohort_definition AS 
SELECT * FROM `@etl_project`.@etl_dataset.cdm_cohort_definition;
CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.attribute_definition AS 
SELECT * FROM `@etl_project`.@etl_dataset.cdm_attribute_definition;
CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.cdm_source AS 
SELECT * FROM `@etl_project`.@etl_dataset.cdm_cdm_source;
CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.metadata AS 
SELECT * FROM `@etl_project`.@etl_dataset.cdm_metadata;
CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.person AS 
SELECT * FROM `@etl_project`.@etl_dataset.cdm_person;
CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.observation_period AS 
SELECT * FROM `@etl_project`.@etl_dataset.cdm_observation_period;
CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.specimen AS 
SELECT * FROM `@etl_project`.@etl_dataset.cdm_specimen;
CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.death AS 
SELECT * FROM `@etl_project`.@etl_dataset.cdm_death;
CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.visit_occurrence AS 
SELECT * FROM `@etl_project`.@etl_dataset.cdm_visit_occurrence;
CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.visit_detail AS 
SELECT * FROM `@etl_project`.@etl_dataset.cdm_visit_detail;
CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.procedure_occurrence AS 
SELECT * FROM `@etl_project`.@etl_dataset.cdm_procedure_occurrence;
CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.drug_exposure AS 
SELECT * FROM `@etl_project`.@etl_dataset.cdm_drug_exposure;
CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.device_exposure AS 
SELECT * FROM `@etl_project`.@etl_dataset.cdm_device_exposure;
CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.condition_occurrence AS 
SELECT * FROM `@etl_project`.@etl_dataset.cdm_condition_occurrence;
CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.measurement AS 
SELECT * FROM `@etl_project`.@etl_dataset.cdm_measurement;
CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.note AS 
SELECT * FROM `@etl_project`.@etl_dataset.cdm_note;
CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.note_nlp AS 
SELECT * FROM `@etl_project`.@etl_dataset.cdm_note_nlp;
CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.observation AS 
SELECT * FROM `@etl_project`.@etl_dataset.cdm_observation;
CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.fact_relationship AS 
SELECT * FROM `@etl_project`.@etl_dataset.cdm_fact_relationship;
CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.location AS 
SELECT * FROM `@etl_project`.@etl_dataset.cdm_location;
CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.care_site AS 
SELECT * FROM `@etl_project`.@etl_dataset.cdm_care_site;
CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.provider AS 
SELECT * FROM `@etl_project`.@etl_dataset.cdm_provider;
CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.payer_plan_period AS 
SELECT * FROM `@etl_project`.@etl_dataset.cdm_payer_plan_period;
CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.cost AS 
SELECT * FROM `@etl_project`.@etl_dataset.cdm_cost;
CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.cohort AS 
SELECT * FROM `@etl_project`.@etl_dataset.cdm_cohort;
CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.cohort_attribute AS 
SELECT * FROM `@etl_project`.@etl_dataset.cdm_cohort_attribute;
CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.drug_era AS 
SELECT * FROM `@etl_project`.@etl_dataset.cdm_drug_era;
CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.dose_era AS 
SELECT * FROM `@etl_project`.@etl_dataset.cdm_dose_era;
CREATE OR REPLACE TABLE `@atlas_project`.@atlas_dataset.condition_era AS 
SELECT * FROM `@etl_project`.@etl_dataset.cdm_condition_era;
