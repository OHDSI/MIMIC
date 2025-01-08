-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- Refresh Vocabularies for OMOP CDM Conversion
-- -------------------------------------------------------------------

-- ------------------------------------------------------------------------------
-- Create vocabulary tables from tmp_*, loaded from CSV
-- add load_row_id fields and format dates
-- 
-- To generate custom concepts, run custom_vocabularies.sql
--      * concept
--      * concept_relationship
--      * vocabulary
-- ------------------------------------------------------------------------------

-- ------------------------------------------------------------------------------
-- tables which ARE affected in generating custom concepts 
-- ------------------------------------------------------------------------------

-- ------------------------------------------------------------------------------
-- concept
-- ------------------------------------------------------------------------------

CREATE OR REPLACE TABLE @bq_target_project.@bq_target_dataset.concept AS
SELECT
    concept_id,
    concept_name,
    domain_id,
    vocabulary_id,
    concept_class_id,
    standard_concept,
    concept_code,
    PARSE_DATE('%Y%m%d', valid_start_date) AS valid_start_date,
    PARSE_DATE('%Y%m%d', valid_end_date) AS valid_end_date,
    invalid_reason,
    'concept' AS load_table_id,
    FARM_FINGERPRINT(GENERATE_UUID()) AS load_row_id
FROM
    @bq_target_project.@bq_target_dataset.tmp_concept
;

DROP TABLE IF EXISTS @bq_target_project.@bq_target_dataset.tmp_concept
;

-- ------------------------------------------------------------------------------
-- concept_relationship
-- ------------------------------------------------------------------------------

CREATE OR REPLACE TABLE @bq_target_project.@bq_target_dataset.concept_relationship AS
SELECT
    concept_id_1,
    concept_id_2,
    relationship_id,
    PARSE_DATE('%Y%m%d', valid_start_date) AS valid_start_date,
    PARSE_DATE('%Y%m%d', valid_end_date) AS valid_end_date,
    invalid_reason,
    'concept_relationship' AS load_table_id,
    0 AS load_row_id
FROM
    @bq_target_project.@bq_target_dataset.tmp_concept_relationship
;

DROP TABLE IF EXISTS @bq_target_project.@bq_target_dataset.tmp_concept_relationship
;

-- ------------------------------------------------------------------------------
-- vocabulary
-- ------------------------------------------------------------------------------

CREATE OR REPLACE TABLE @bq_target_project.@bq_target_dataset.vocabulary AS
SELECT
    *,
    'vocabulary' AS load_table_id,
    0 AS load_row_id
FROM
    @bq_target_project.@bq_target_dataset.tmp_vocabulary
;

DROP TABLE IF EXISTS @bq_target_project.@bq_target_dataset.tmp_vocabulary
;

-- ------------------------------------------------------------------------------
-- tables which are NOT affected in generating custom concepts 
-- ------------------------------------------------------------------------------

-- ------------------------------------------------------------------------------
-- drug_strength
-- ------------------------------------------------------------------------------

CREATE OR REPLACE TABLE @bq_target_project.@bq_target_dataset.drug_strength AS
SELECT
    drug_concept_id,
    ingredient_concept_id,
    amount_value,
    amount_unit_concept_id,
    numerator_value,
    numerator_unit_concept_id,
    denominator_value,
    denominator_unit_concept_id,
    box_size,
    PARSE_DATE('%Y%m%d', valid_start_date) AS valid_start_date,
    PARSE_DATE('%Y%m%d', valid_end_date) AS valid_end_date,
    invalid_reason,
    'drug_strength' AS load_table_id,
    0 AS load_row_id
FROM
    @bq_target_project.@bq_target_dataset.tmp_drug_strength
;

DROP TABLE IF EXISTS @bq_target_project.@bq_target_dataset.tmp_drug_strength
;

-- ------------------------------------------------------------------------------
-- concept_class
-- ------------------------------------------------------------------------------

CREATE OR REPLACE TABLE @bq_target_project.@bq_target_dataset.concept_class AS
SELECT
    *,
    'concept_class' AS load_table_id,
    0 AS load_row_id
FROM
    @bq_target_project.@bq_target_dataset.tmp_concept_class
;

DROP TABLE IF EXISTS @bq_target_project.@bq_target_dataset.tmp_concept_class
;

-- ------------------------------------------------------------------------------
-- concept_ancestor
-- ------------------------------------------------------------------------------

CREATE OR REPLACE TABLE @bq_target_project.@bq_target_dataset.concept_ancestor AS
SELECT
    *,
    'concept_ancestor' AS load_table_id,
    0 AS load_row_id
FROM
    @bq_target_project.@bq_target_dataset.tmp_concept_ancestor
;

DROP TABLE IF EXISTS @bq_target_project.@bq_target_dataset.tmp_concept_ancestor
;

-- ------------------------------------------------------------------------------
-- concept_synonym
-- ------------------------------------------------------------------------------

CREATE OR REPLACE TABLE @bq_target_project.@bq_target_dataset.concept_synonym AS
SELECT
    *,
    'concept_synonym' AS load_table_id,
    0 AS load_row_id
FROM
    @bq_target_project.@bq_target_dataset.tmp_concept_synonym
;

DROP TABLE IF EXISTS @bq_target_project.@bq_target_dataset.tmp_concept_synonym
;

-- ------------------------------------------------------------------------------
-- domain
-- ------------------------------------------------------------------------------

CREATE OR REPLACE TABLE @bq_target_project.@bq_target_dataset.domain AS
SELECT
    *,
    'domain' AS load_table_id,
    0 AS load_row_id
FROM
    @bq_target_project.@bq_target_dataset.tmp_domain
;

DROP TABLE IF EXISTS @bq_target_project.@bq_target_dataset.tmp_domain
;

-- ------------------------------------------------------------------------------
-- relationship
-- ------------------------------------------------------------------------------

CREATE OR REPLACE TABLE @bq_target_project.@bq_target_dataset.relationship AS
SELECT
    *,
    'relationship' AS load_table_id,
    0 AS load_row_id
FROM
    @bq_target_project.@bq_target_dataset.tmp_relationship
;

DROP TABLE IF EXISTS @bq_target_project.@bq_target_dataset.tmp_relationship
;

