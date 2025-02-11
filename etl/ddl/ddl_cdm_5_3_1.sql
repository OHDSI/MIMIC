-- ----------------------------------------------------------------------------
-- MIMIC IV to OMOP CDM
-- Odysseus, Sep-2020
-- ----------------------------------------------------------------------------

/*OMOP CDM v5.3.1 14June2018*/


CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_cohort_definition (
  cohort_definition_id            INT64       not null,
  cohort_definition_name          STRING      not null,
  cohort_definition_description   STRING              ,
  definition_type_concept_id      INT64       not null,
  cohort_definition_syntax        STRING              ,
  subject_concept_id              INT64       not null,
  cohort_initiation_date          DATE
)
;


CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_attribute_definition (
  attribute_definition_id     INT64       not null,
  attribute_name              STRING      not null,
  attribute_description       STRING              ,
  attribute_type_concept_id   INT64       not null,
  attribute_syntax            STRING
)
;


CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_cdm_source
(
  cdm_source_name                 STRING        not null ,
  cdm_source_abbreviation         STRING             ,
  cdm_holder                      STRING             ,
  source_description              STRING             ,
  source_documentation_reference  STRING             ,
  cdm_etl_reference               STRING             ,
  source_release_date             DATE               ,
  cdm_release_date                DATE               ,
  cdm_version                     STRING             ,
  vocabulary_version              STRING
)
;


CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_metadata
(
  metadata_concept_id       INT64       not null ,
  metadata_type_concept_id  INT64       not null ,
  name                      STRING      not null ,
  value_as_string           STRING               ,
  value_as_concept_id       INT64                ,
  metadata_date             DATE                 ,
  metadata_datetime         DATETIME
)
;



--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_person
(
  person_id                   INT64     not null ,
  gender_concept_id           INT64     not null ,
  year_of_birth               INT64     not null ,
  month_of_birth              INT64              ,
  day_of_birth                INT64              ,
  birth_datetime              DATETIME           ,
  race_concept_id             INT64     not null,
  ethnicity_concept_id        INT64     not null,
  location_id                 INT64              ,
  provider_id                 INT64              ,
  care_site_id                INT64              ,
  person_source_value         STRING             ,
  gender_source_value         STRING             ,
  gender_source_concept_id    INT64              ,
  race_source_value           STRING             ,
  race_source_concept_id      INT64              ,
  ethnicity_source_value      STRING             ,
  ethnicity_source_concept_id INT64
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_observation_period
(
  observation_period_id             INT64   not null ,
  person_id                         INT64   not null ,
  observation_period_start_date     DATE    not null ,
  observation_period_end_date       DATE    not null ,
  period_type_concept_id            INT64   not null
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_specimen
(
  specimen_id                 INT64     not null ,
  person_id                   INT64     not null ,
  specimen_concept_id         INT64     not null ,
  specimen_type_concept_id    INT64     not null ,
  specimen_date               DATE      not null ,
  specimen_datetime           DATETIME           ,
  quantity                    FLOAT64            ,
  unit_concept_id             INT64              ,
  anatomic_site_concept_id    INT64              ,
  disease_status_concept_id   INT64              ,
  specimen_source_id          STRING             ,
  specimen_source_value       STRING             ,
  unit_source_value           STRING             ,
  anatomic_site_source_value  STRING             ,
  disease_status_source_value STRING
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_death
(
  person_id               INT64     not null ,
  death_date              DATE      not null ,
  death_datetime          DATETIME           ,
  death_type_concept_id   INT64     not null ,
  cause_concept_id        INT64              ,
  cause_source_value      STRING             ,
  cause_source_concept_id INT64
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_visit_occurrence
(
  visit_occurrence_id           INT64     not null ,
  person_id                     INT64     not null ,
  visit_concept_id              INT64     not null ,
  visit_start_date              DATE      not null ,
  visit_start_datetime          DATETIME           ,
  visit_end_date                DATE      not null ,
  visit_end_datetime            DATETIME           ,
  visit_type_concept_id         INT64     not null ,
  provider_id                   INT64              ,
  care_site_id                  INT64              ,
  visit_source_value            STRING             ,
  visit_source_concept_id       INT64              ,
  admitting_source_concept_id   INT64              ,
  admitting_source_value        STRING             ,
  discharge_to_concept_id       INT64              ,
  discharge_to_source_value     STRING             ,
  preceding_visit_occurrence_id INT64
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_visit_detail
(
  visit_detail_id                    INT64     not null ,
  person_id                          INT64     not null ,
  visit_detail_concept_id            INT64     not null ,
  visit_detail_start_date            DATE      not null ,
  visit_detail_start_datetime        DATETIME           ,
  visit_detail_end_date              DATE      not null ,
  visit_detail_end_datetime          DATETIME           ,
  visit_detail_type_concept_id       INT64     not null ,
  provider_id                        INT64              ,
  care_site_id                       INT64              ,
  admitting_source_concept_id        INT64              ,
  discharge_to_concept_id            INT64              ,
  preceding_visit_detail_id          INT64              ,
  visit_detail_source_value          STRING             ,
  visit_detail_source_concept_id     INT64              ,
  admitting_source_value             STRING             ,
  discharge_to_source_value          STRING             ,
  visit_detail_parent_id             INT64              ,
  visit_occurrence_id                INT64     not null
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_procedure_occurrence
(
  procedure_occurrence_id     INT64     not null ,
  person_id                   INT64     not null ,
  procedure_concept_id        INT64     not null ,
  procedure_date              DATE      not null ,
  procedure_datetime          DATETIME           ,
  procedure_type_concept_id   INT64     not null ,
  modifier_concept_id         INT64              ,
  quantity                    INT64              ,
  provider_id                 INT64              ,
  visit_occurrence_id         INT64              ,
  visit_detail_id             INT64              ,
  procedure_source_value      STRING             ,
  procedure_source_concept_id INT64              ,
  modifier_source_value      STRING
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_drug_exposure
(
  drug_exposure_id              INT64       not null ,
  person_id                     INT64       not null ,
  drug_concept_id               INT64       not null ,
  drug_exposure_start_date      DATE        not null ,
  drug_exposure_start_datetime  DATETIME             ,
  drug_exposure_end_date        DATE        not null ,
  drug_exposure_end_datetime    DATETIME             ,
  verbatim_end_date             DATE                 ,
  drug_type_concept_id          INT64       not null ,
  stop_reason                   STRING               ,
  refills                       INT64                ,
  quantity                      FLOAT64              ,
  days_supply                   INT64                ,
  sig                           STRING               ,
  route_concept_id              INT64                ,
  lot_number                    STRING               ,
  provider_id                   INT64                ,
  visit_occurrence_id           INT64                ,
  visit_detail_id               INT64                ,
  drug_source_value             STRING               ,
  drug_source_concept_id        INT64                ,
  route_source_value            STRING               ,
  dose_unit_source_value        STRING
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_device_exposure
(
  device_exposure_id              INT64       not null ,
  person_id                       INT64       not null ,
  device_concept_id               INT64       not null ,
  device_exposure_start_date      DATE        not null ,
  device_exposure_start_datetime  DATETIME             ,
  device_exposure_end_date        DATE                 ,
  device_exposure_end_datetime    DATETIME             ,
  device_type_concept_id          INT64       not null ,
  unique_device_id                STRING               ,
  quantity                        INT64                ,
  provider_id                     INT64                ,
  visit_occurrence_id             INT64                ,
  visit_detail_id                 INT64                ,
  device_source_value             STRING               ,
  device_source_concept_id        INT64
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_condition_occurrence
(
  condition_occurrence_id       INT64     not null ,
  person_id                     INT64     not null ,
  condition_concept_id          INT64     not null ,
  condition_start_date          DATE      not null ,
  condition_start_datetime      DATETIME           ,
  condition_end_date            DATE               ,
  condition_end_datetime        DATETIME           ,
  condition_type_concept_id     INT64     not null ,
  stop_reason                   STRING             ,
  provider_id                   INT64              ,
  visit_occurrence_id           INT64              ,
  visit_detail_id               INT64              ,
  condition_source_value        STRING             ,
  condition_source_concept_id   INT64              ,
  condition_status_source_value STRING             ,
  condition_status_concept_id   INT64
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_measurement
(
  measurement_id                INT64     not null ,
  person_id                     INT64     not null ,
  measurement_concept_id        INT64     not null ,
  measurement_date              DATE      not null ,
  measurement_datetime          DATETIME           ,
  measurement_time              STRING             ,
  measurement_type_concept_id   INT64     not null ,
  operator_concept_id           INT64              ,
  value_as_number               FLOAT64            ,
  value_as_concept_id           INT64              ,
  unit_concept_id               INT64              ,
  range_low                     FLOAT64            ,
  range_high                    FLOAT64            ,
  provider_id                   INT64              ,
  visit_occurrence_id           INT64              ,
  visit_detail_id               INT64              ,
  measurement_source_value      STRING             ,
  measurement_source_concept_id INT64              ,
  unit_source_value             STRING             ,
  value_source_value            STRING
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_note
(
  note_id               INT64       not null ,
  person_id             INT64       not null ,
  note_date             DATE        not null ,
  note_datetime         DATETIME             ,
  note_type_concept_id  INT64       not null ,
  note_class_concept_id INT64       not null ,
  note_title            STRING               ,
  note_text             STRING               ,
  encoding_concept_id   INT64       not null ,
  language_concept_id   INT64       not null ,
  provider_id           INT64                ,
  visit_occurrence_id   INT64                ,
  visit_detail_id       INT64                ,
  note_source_value     STRING
)
;



CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_note_nlp
(
  note_nlp_id                 INT64                ,
  note_id                     INT64                ,
  section_concept_id          INT64                ,
  snippet                     STRING               ,
  offset                      STRING               ,
  lexical_variant             STRING      not null ,
  note_nlp_concept_id         INT64                ,
  note_nlp_source_concept_id  INT64                ,
  nlp_system                  STRING               ,
  nlp_date                    DATE        not null ,
  nlp_datetime                DATETIME             ,
  term_exists                 STRING               ,
  term_temporal               STRING               ,
  term_modifiers              STRING
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_observation
(
  observation_id                INT64     not null ,
  person_id                     INT64     not null ,
  observation_concept_id        INT64     not null ,
  observation_date              DATE      not null ,
  observation_datetime          DATETIME           ,
  observation_type_concept_id   INT64     not null ,
  value_as_number               FLOAT64        ,
  value_as_string               STRING         ,
  value_as_concept_id           INT64          ,
  qualifier_concept_id          INT64          ,
  unit_concept_id               INT64          ,
  provider_id                   INT64          ,
  visit_occurrence_id           INT64          ,
  visit_detail_id               INT64          ,
  observation_source_value      STRING         ,
  observation_source_concept_id INT64          ,
  unit_source_value             STRING         ,
  qualifier_source_value        STRING
)
;


CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_fact_relationship
(
  domain_concept_id_1     INT64     not null ,
  fact_id_1               INT64     not null ,
  domain_concept_id_2     INT64     not null ,
  fact_id_2               INT64     not null ,
  relationship_concept_id INT64     not null
)
;


CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_location
(
  location_id           INT64     not null ,
  address_1             STRING             ,
  address_2             STRING             ,
  city                  STRING             ,
  state                 STRING             ,
  zip                   STRING             ,
  county                STRING             ,
  location_source_value STRING
)
;


CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_care_site
(
  care_site_id                  INT64       not null ,
  care_site_name                STRING               ,
  place_of_service_concept_id   INT64                ,
  location_id                   INT64                ,
  care_site_source_value        STRING               ,
  place_of_service_source_value STRING
)
;


CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_provider
(
  provider_id                 INT64       not null ,
  provider_name               STRING               ,
  npi                         STRING               ,
  dea                         STRING               ,
  specialty_concept_id        INT64                ,
  care_site_id                INT64                ,
  year_of_birth               INT64                ,
  gender_concept_id           INT64                ,
  provider_source_value       STRING               ,
  specialty_source_value      STRING               ,
  specialty_source_concept_id INT64                ,
  gender_source_value         STRING               ,
  gender_source_concept_id    INT64
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_payer_plan_period
(
  payer_plan_period_id          INT64     not null ,
  person_id                     INT64     not null ,
  payer_plan_period_start_date  DATE      not null ,
  payer_plan_period_end_date    DATE      not null ,
  payer_concept_id              INT64              ,
  payer_source_value            STRING             ,
  payer_source_concept_id       INT64              ,
  plan_concept_id               INT64              ,
  plan_source_value             STRING             ,
  plan_source_concept_id        INT64              ,
  sponsor_concept_id            INT64              ,
  sponsor_source_value          STRING             ,
  sponsor_source_concept_id     INT64              ,
  family_source_value           STRING             ,
  stop_reason_concept_id        INT64              ,
  stop_reason_source_value      STRING             ,
  stop_reason_source_concept_id INT64
)
;


CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_cost
(
  cost_id                   INT64     not null ,
  cost_event_id             INT64     not null ,
  cost_domain_id            STRING    not null ,
  cost_type_concept_id      INT64     not null ,
  currency_concept_id       INT64              ,
  total_charge              FLOAT64            ,
  total_cost                FLOAT64            ,
  total_paid                FLOAT64            ,
  paid_by_payer             FLOAT64            ,
  paid_by_patient           FLOAT64            ,
  paid_patient_copay        FLOAT64            ,
  paid_patient_coinsurance  FLOAT64            ,
  paid_patient_deductible   FLOAT64            ,
  paid_by_primary           FLOAT64            ,
  paid_ingredient_cost      FLOAT64            ,
  paid_dispensing_fee       FLOAT64            ,
  payer_plan_period_id      INT64              ,
  amount_allowed            FLOAT64            ,
  revenue_code_concept_id   INT64              ,
  revenue_code_source_value  STRING            ,
  drg_concept_id            INT64              ,
  drg_source_value          STRING
)
;


--HINT DISTRIBUTE_ON_KEY(subject_id)
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_cohort
(
  cohort_definition_id  INT64   not null ,
  subject_id            INT64   not null ,
  cohort_start_date     DATE      not null ,
  cohort_end_date       DATE      not null
)
;


--HINT DISTRIBUTE_ON_KEY(subject_id)
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_cohort_attribute
(
  cohort_definition_id    INT64     not null ,
  subject_id              INT64     not null ,
  cohort_start_date       DATE      not null ,
  cohort_end_date         DATE      not null ,
  attribute_definition_id INT64     not null ,
  value_as_number         FLOAT64            ,
  value_as_concept_id     INT64
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_drug_era
(
  drug_era_id         INT64     not null ,
  person_id           INT64     not null ,
  drug_concept_id     INT64     not null ,
  drug_era_start_date DATE      not null ,
  drug_era_end_date   DATE      not null ,
  drug_exposure_count INT64              ,
  gap_days            INT64
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_dose_era
(
  dose_era_id           INT64     not null ,
  person_id             INT64     not null ,
  drug_concept_id       INT64     not null ,
  unit_concept_id       INT64     not null ,
  dose_value            FLOAT64   not null ,
  dose_era_start_date   DATE      not null ,
  dose_era_end_date     DATE      not null
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_condition_era
(
  condition_era_id            INT64     not null ,
  person_id                   INT64     not null ,
  condition_concept_id        INT64     not null ,
  condition_era_start_date    DATE      not null ,
  condition_era_end_date      DATE      not null ,
  condition_occurrence_count  INT64
)
;
