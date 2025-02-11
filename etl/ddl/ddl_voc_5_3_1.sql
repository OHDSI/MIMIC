/*OMOP CDM v5.3.1 14June2018*/

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.voc_concept (
  concept_id          INT64       not null ,
  concept_name        STRING      not null ,
  domain_id           STRING      not null ,
  vocabulary_id       STRING      not null ,
  concept_class_id    STRING      not null ,
  standard_concept    STRING               ,
  concept_code        STRING      not null ,
  valid_start_DATE    DATE        not null ,
  valid_end_DATE      DATE        not null ,
  invalid_reason      STRING
)
;


CREATE OR REPLACE TABLE @etl_project.@etl_dataset.voc_vocabulary (
  vocabulary_id         STRING      not null,
  vocabulary_name       STRING      not null,
  vocabulary_reference  STRING      not null,
  vocabulary_version    STRING              ,
  vocabulary_concept_id INT64       not null
)
;


CREATE OR REPLACE TABLE @etl_project.@etl_dataset.voc_domain (
  domain_id         STRING      not null,
  domain_name       STRING      not null,
  domain_concept_id INT64       not null
)
;


CREATE OR REPLACE TABLE @etl_project.@etl_dataset.voc_concept_class (
  concept_class_id          STRING      not null,
  concept_class_name        STRING      not null,
  concept_class_concept_id  INT64       not null
)
;


CREATE OR REPLACE TABLE @etl_project.@etl_dataset.voc_concept_relationship (
  concept_id_1      INT64     not null,
  concept_id_2      INT64     not null,
  relationship_id   STRING    not null,
  valid_start_DATE  DATE      not null,
  valid_end_DATE    DATE      not null,
  invalid_reason    STRING
  )
;


CREATE OR REPLACE TABLE @etl_project.@etl_dataset.voc_relationship (
  relationship_id         STRING      not null,
  relationship_name       STRING      not null,
  is_hierarchical         STRING      not null,
  defines_ancestry        STRING      not null,
  reverse_relationship_id STRING      not null,
  relationship_concept_id INT64       not null
)
;


CREATE OR REPLACE TABLE @etl_project.@etl_dataset.voc_concept_synonym (
  concept_id            INT64       not null,
  concept_synonym_name  STRING      not null,
  language_concept_id   INT64       not null
)
;


CREATE OR REPLACE TABLE @etl_project.@etl_dataset.voc_concept_ancestor (
  ancestor_concept_id       INT64   not null,
  descendant_concept_id     INT64   not null,
  min_levels_of_separation  INT64   not null,
  max_levels_of_separation  INT64   not null
)
;


CREATE OR REPLACE TABLE @etl_project.@etl_dataset.voc_source_to_concept_map (
  source_code             STRING      not null,
  source_concept_id       INT64       not null,
  source_vocabulary_id    STRING      not null,
  source_code_description STRING              ,
  target_concept_id       INT64       not null,
  target_vocabulary_id    STRING      not null,
  valid_start_DATE        DATE        not null,
  valid_end_DATE          DATE        not null,
  invalid_reason          STRING
)
;


CREATE OR REPLACE TABLE @etl_project.@etl_dataset.voc_drug_strength (
  drug_concept_id             INT64     not null,
  ingredient_concept_id       INT64     not null,
  amount_value                FLOAT64           ,
  amount_unit_concept_id      INT64             ,
  numerator_value             FLOAT64           ,
  numerator_unit_concept_id   INT64             ,
  denominator_value           FLOAT64           ,
  denominator_unit_concept_id INT64             ,
  box_size                    INT64             ,
  valid_start_DATE            DATE       not null,
  valid_end_DATE              DATE       not null,
  invalid_reason              STRING
)
;


