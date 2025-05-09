-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Copy vocabulary tables from the master vocab dataset
-- (to apply custom mapping here?)
-- -------------------------------------------------------------------

-- check
-- SELECT 'VOC', COUNT(*) FROM @voc_project.@voc_dataset.concept
-- UNION ALL
-- SELECT 'TARGET', COUNT(*) FROM @etl_project.@etl_dataset.voc_concept
-- ;

-- affected by custom mapping

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.voc_concept AS
SELECT * FROM @voc_project.@voc_dataset.concept
;

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.voc_concept_relationship AS
SELECT * FROM @voc_project.@voc_dataset.concept_relationship
;

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.voc_vocabulary AS
SELECT * FROM @voc_project.@voc_dataset.vocabulary
;

-- not affected by custom mapping

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.voc_domain AS
SELECT * FROM @voc_project.@voc_dataset.domain
;
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.voc_concept_class AS
SELECT * FROM @voc_project.@voc_dataset.concept_class
;
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.voc_relationship AS
SELECT * FROM @voc_project.@voc_dataset.relationship
;
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.voc_concept_synonym AS
SELECT * FROM @voc_project.@voc_dataset.concept_synonym
;
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.voc_concept_ancestor AS
SELECT * FROM @voc_project.@voc_dataset.concept_ancestor
;
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.voc_drug_strength AS
SELECT * FROM @voc_project.@voc_dataset.drug_strength
;

