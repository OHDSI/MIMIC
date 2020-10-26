-- bq_cdm_to_atlas generated script --

-- Unload to ATLAS-- Copy Vocabulary tables

CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_202010_cdm_531.concept AS 
SELECT * FROM `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.voc_concept;
CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_202010_cdm_531.vocabulary AS 
SELECT * FROM `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.voc_vocabulary;
CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_202010_cdm_531.domain AS 
SELECT * FROM `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.voc_domain;
CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_202010_cdm_531.concept_class AS 
SELECT * FROM `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.voc_concept_class;
CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_202010_cdm_531.concept_relationship AS 
SELECT * FROM `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.voc_concept_relationship;
CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_202010_cdm_531.relationship AS 
SELECT * FROM `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.voc_relationship;
CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_202010_cdm_531.concept_synonym AS 
SELECT * FROM `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.voc_concept_synonym;
CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_202010_cdm_531.concept_ancestor AS 
SELECT * FROM `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.voc_concept_ancestor;
CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_202010_cdm_531.source_to_concept_map AS 
SELECT * FROM `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.voc_source_to_concept_map;
CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_202010_cdm_531.drug_strength AS 
SELECT * FROM `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.voc_drug_strength;

-- Copy CDM tables

CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_202010_cdm_531.cohort_definition AS 
SELECT * FROM `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.cdm_cohort_definition;
CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_202010_cdm_531.attribute_definition AS 
SELECT * FROM `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.cdm_attribute_definition;
CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_202010_cdm_531.cdm_source AS 
SELECT * FROM `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.cdm_cdm_source;
CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_202010_cdm_531.metadata AS 
SELECT * FROM `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.cdm_metadata;
CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_202010_cdm_531.person AS 
SELECT * FROM `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.cdm_person;
CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_202010_cdm_531.observation_period AS 
SELECT * FROM `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.cdm_observation_period;
CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_202010_cdm_531.specimen AS 
SELECT * FROM `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.cdm_specimen;
CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_202010_cdm_531.death AS 
SELECT * FROM `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.cdm_death;
CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_202010_cdm_531.visit_occurrence AS 
SELECT * FROM `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.cdm_visit_occurrence;
CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_202010_cdm_531.visit_detail AS 
SELECT * FROM `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.cdm_visit_detail;
CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_202010_cdm_531.procedure_occurrence AS 
SELECT * FROM `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.cdm_procedure_occurrence;
CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_202010_cdm_531.drug_exposure AS 
SELECT * FROM `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.cdm_drug_exposure;
CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_202010_cdm_531.device_exposure AS 
SELECT * FROM `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.cdm_device_exposure;
CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_202010_cdm_531.condition_occurrence AS 
SELECT * FROM `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.cdm_condition_occurrence;
CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_202010_cdm_531.measurement AS 
SELECT * FROM `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.cdm_measurement;
CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_202010_cdm_531.note AS 
SELECT * FROM `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.cdm_note;
CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_202010_cdm_531.note_nlp AS 
SELECT * FROM `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.cdm_note_nlp;
CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_202010_cdm_531.observation AS 
SELECT * FROM `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.cdm_observation;
CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_202010_cdm_531.fact_relationship AS 
SELECT * FROM `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.cdm_fact_relationship;
CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_202010_cdm_531.location AS 
SELECT * FROM `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.cdm_location;
CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_202010_cdm_531.care_site AS 
SELECT * FROM `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.cdm_care_site;
CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_202010_cdm_531.provider AS 
SELECT * FROM `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.cdm_provider;
CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_202010_cdm_531.payer_plan_period AS 
SELECT * FROM `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.cdm_payer_plan_period;
CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_202010_cdm_531.cost AS 
SELECT * FROM `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.cdm_cost;
CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_202010_cdm_531.cohort AS 
SELECT * FROM `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.cdm_cohort;
CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_202010_cdm_531.cohort_attribute AS 
SELECT * FROM `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.cdm_cohort_attribute;
CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_202010_cdm_531.drug_era AS 
SELECT * FROM `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.cdm_drug_era;
CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_202010_cdm_531.dose_era AS 
SELECT * FROM `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.cdm_dose_era;
CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_202010_cdm_531.condition_era AS 
SELECT * FROM `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.cdm_condition_era;
