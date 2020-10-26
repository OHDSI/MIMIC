-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Copy vocabulary tables from the master vocab dataset
-- (to apply custom mapping here?)
-- -------------------------------------------------------------------

-- check
-- SELECT 'VOC', COUNT(*) FROM `@voc_project`.@voc_dataset.concept
-- UNION ALL
-- SELECT 'TARGET', COUNT(*) FROM `@target_project`.@target_dataset.voc_concept
-- ;


CREATE OR REPLACE TABLE `@target_project`.@target_dataset.voc_concept AS
SELECT * FROM `@voc_project`.@voc_dataset.concept
;

CREATE OR REPLACE TABLE `@target_project`.@target_dataset.voc_concept_relationship AS
SELECT * FROM `@voc_project`.@voc_dataset.concept_relationship
;

CREATE OR REPLACE TABLE `@target_project`.@target_dataset.voc_vocabulary AS
SELECT * FROM `@voc_project`.@voc_dataset.vocabulary
;
