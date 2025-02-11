-- Unload to ATLAS-- 2 billion concepts from Vocabulary tables

CREATE OR REPLACE TABLE @atlas_project.@atlas_dataset.2b_concept AS 
SELECT
    concept_id,
    concept_name,
    domain_id,
    vocabulary_id,
    concept_class_id,
    standard_concept,
    concept_code,
    valid_start_DATE,
    valid_end_DATE,
    invalid_reason
FROM @etl_project.@etl_dataset.voc_concept
WHERE
    concept_id >= 2000000000
;

CREATE OR REPLACE TABLE @atlas_project.@atlas_dataset.2b_concept_relationship AS 
SELECT
    concept_id_1,
    concept_id_2,
    relationship_id,
    valid_start_DATE,
    valid_end_DATE,
    invalid_reason
FROM @etl_project.@etl_dataset.voc_concept_relationship
WHERE
    concept_id_1 >= 2000000000
    OR concept_id_2 >= 2000000000
;

CREATE OR REPLACE TABLE @atlas_project.@atlas_dataset.2b_vocabulary AS 
SELECT
    vocabulary_id,
    vocabulary_name,
    vocabulary_reference,
    vocabulary_version,
    vocabulary_concept_id
FROM @etl_project.@etl_dataset.voc_vocabulary
WHERE
    vocabulary_concept_id >= 2000000000
;

