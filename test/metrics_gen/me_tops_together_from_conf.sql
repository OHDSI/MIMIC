DROP TABLE IF EXISTS @metrics_project.@metrics_dataset.me_tops_together;
CREATE TABLE @metrics_project.@metrics_dataset.me_tops_together
(
    table_name        STRING,
    concept_field     STRING,
    category          STRING,
    source_value      STRING,
    concept_id        INT64,
    concept_name      STRING,
    count             INT64,
    percent           FLOAT64
);

INSERT INTO @metrics_project.@metrics_dataset.me_tops_together
SELECT
    'cdm_care_site'      AS table_name,
    *
from @metrics_project.@metrics_dataset.me_top_cdm_care_site rt
order by concept_field, category, count desc
;

INSERT INTO @metrics_project.@metrics_dataset.me_tops_together
SELECT
    'cdm_provider'      AS table_name,
    *
from @metrics_project.@metrics_dataset.me_top_cdm_provider rt
order by concept_field, category, count desc
;

INSERT INTO @metrics_project.@metrics_dataset.me_tops_together
SELECT
    'cdm_person'      AS table_name,
    *
from @metrics_project.@metrics_dataset.me_top_cdm_person rt
order by concept_field, category, count desc
;

INSERT INTO @metrics_project.@metrics_dataset.me_tops_together
SELECT
    'cdm_death'      AS table_name,
    *
from @metrics_project.@metrics_dataset.me_top_cdm_death rt
order by concept_field, category, count desc
;

INSERT INTO @metrics_project.@metrics_dataset.me_tops_together
SELECT
    'cdm_observation_period'      AS table_name,
    *
from @metrics_project.@metrics_dataset.me_top_cdm_observation_period rt
order by concept_field, category, count desc
;

INSERT INTO @metrics_project.@metrics_dataset.me_tops_together
SELECT
    'cdm_payer_plan_period'      AS table_name,
    *
from @metrics_project.@metrics_dataset.me_top_cdm_payer_plan_period rt
order by concept_field, category, count desc
;

INSERT INTO @metrics_project.@metrics_dataset.me_tops_together
SELECT
    'cdm_visit_occurrence'      AS table_name,
    *
from @metrics_project.@metrics_dataset.me_top_cdm_visit_occurrence rt
order by concept_field, category, count desc
;

INSERT INTO @metrics_project.@metrics_dataset.me_tops_together
SELECT
    'cdm_visit_detail'      AS table_name,
    *
from @metrics_project.@metrics_dataset.me_top_cdm_visit_detail rt
order by concept_field, category, count desc
;

INSERT INTO @metrics_project.@metrics_dataset.me_tops_together
SELECT
    'cdm_condition_occurrence'      AS table_name,
    *
from @metrics_project.@metrics_dataset.me_top_cdm_condition_occurrence rt
order by concept_field, category, count desc
;

INSERT INTO @metrics_project.@metrics_dataset.me_tops_together
SELECT
    'cdm_procedure_occurrence'      AS table_name,
    *
from @metrics_project.@metrics_dataset.me_top_cdm_procedure_occurrence rt
order by concept_field, category, count desc
;

INSERT INTO @metrics_project.@metrics_dataset.me_tops_together
SELECT
    'cdm_observation'      AS table_name,
    *
from @metrics_project.@metrics_dataset.me_top_cdm_observation rt
order by concept_field, category, count desc
;

INSERT INTO @metrics_project.@metrics_dataset.me_tops_together
SELECT
    'cdm_measurement'      AS table_name,
    *
from @metrics_project.@metrics_dataset.me_top_cdm_measurement rt
order by concept_field, category, count desc
;

INSERT INTO @metrics_project.@metrics_dataset.me_tops_together
SELECT
    'cdm_device_exposure'      AS table_name,
    *
from @metrics_project.@metrics_dataset.me_top_cdm_device_exposure rt
order by concept_field, category, count desc
;

INSERT INTO @metrics_project.@metrics_dataset.me_tops_together
SELECT
    'cdm_drug_exposure'      AS table_name,
    *
from @metrics_project.@metrics_dataset.me_top_cdm_drug_exposure rt
order by concept_field, category, count desc
;

INSERT INTO @metrics_project.@metrics_dataset.me_tops_together
SELECT
    'cdm_cost'      AS table_name,
    *
from @metrics_project.@metrics_dataset.me_top_cdm_cost rt
order by concept_field, category, count desc
;

INSERT INTO @metrics_project.@metrics_dataset.me_tops_together
SELECT
    'cdm_condition_era'      AS table_name,
    *
from @metrics_project.@metrics_dataset.me_top_cdm_condition_era rt
order by concept_field, category, count desc
;

INSERT INTO @metrics_project.@metrics_dataset.me_tops_together
SELECT
    'cdm_drug_era'      AS table_name,
    *
from @metrics_project.@metrics_dataset.me_top_cdm_drug_era rt
order by concept_field, category, count desc
;

INSERT INTO @metrics_project.@metrics_dataset.me_tops_together
SELECT
    'cdm_dose_era'      AS table_name,
    *
from @metrics_project.@metrics_dataset.me_top_cdm_dose_era rt
order by concept_field, category, count desc
;

INSERT INTO @metrics_project.@metrics_dataset.me_tops_together
SELECT
    'cdm_specimen'      AS table_name,
    *
from @metrics_project.@metrics_dataset.me_top_cdm_specimen rt
order by concept_field, category, count desc
;

INSERT INTO @metrics_project.@metrics_dataset.me_tops_together
SELECT
    'cdm_note'      AS table_name,
    *
from @metrics_project.@metrics_dataset.me_top_cdm_note rt
order by concept_field, category, count desc
;

INSERT INTO @metrics_project.@metrics_dataset.me_tops_together
SELECT
    'cdm_note_nlp'      AS table_name,
    *
from @metrics_project.@metrics_dataset.me_top_cdm_note_nlp rt
order by concept_field, category, count desc
;

INSERT INTO @metrics_project.@metrics_dataset.me_tops_together
SELECT
    'cdm_fact_relationship'      AS table_name,
    *
from @metrics_project.@metrics_dataset.me_top_cdm_fact_relationship rt
order by concept_field, category, count desc
;

INSERT INTO @metrics_project.@metrics_dataset.me_tops_together
SELECT
    'cdm_cohort_attribute'      AS table_name,
    *
from @metrics_project.@metrics_dataset.me_top_cdm_cohort_attribute rt
order by concept_field, category, count desc
;

INSERT INTO @metrics_project.@metrics_dataset.me_tops_together
SELECT
    'cdm_metadata'      AS table_name,
    *
from @metrics_project.@metrics_dataset.me_top_cdm_metadata rt
order by concept_field, category, count desc
;

