SELECT *
from @metrics_project.@metrics_dataset.me_top_cdm_care_site rt
order by concept_field, category, count desc
;

SELECT *
from @metrics_project.@metrics_dataset.me_top_cdm_provider rt
order by concept_field, category, count desc
;

SELECT *
from @metrics_project.@metrics_dataset.me_top_cdm_person rt
order by concept_field, category, count desc
;

SELECT *
from @metrics_project.@metrics_dataset.me_top_cdm_death rt
order by concept_field, category, count desc
;

SELECT *
from @metrics_project.@metrics_dataset.me_top_cdm_observation_period rt
order by concept_field, category, count desc
;

SELECT *
from @metrics_project.@metrics_dataset.me_top_cdm_payer_plan_period rt
order by concept_field, category, count desc
;

SELECT *
from @metrics_project.@metrics_dataset.me_top_cdm_visit_occurrence rt
order by concept_field, category, count desc
;

SELECT *
from @metrics_project.@metrics_dataset.me_top_cdm_visit_detail rt
order by concept_field, category, count desc
;

SELECT *
from @metrics_project.@metrics_dataset.me_top_cdm_condition_occurrence rt
order by concept_field, category, count desc
;

SELECT *
from @metrics_project.@metrics_dataset.me_top_cdm_procedure_occurrence rt
order by concept_field, category, count desc
;

SELECT *
from @metrics_project.@metrics_dataset.me_top_cdm_observation rt
order by concept_field, category, count desc
;

SELECT *
from @metrics_project.@metrics_dataset.me_top_cdm_measurement rt
order by concept_field, category, count desc
;

SELECT *
from @metrics_project.@metrics_dataset.me_top_cdm_device_exposure rt
order by concept_field, category, count desc
;

SELECT *
from @metrics_project.@metrics_dataset.me_top_cdm_drug_exposure rt
order by concept_field, category, count desc
;

SELECT *
from @metrics_project.@metrics_dataset.me_top_cdm_cost rt
order by concept_field, category, count desc
;

SELECT *
from @metrics_project.@metrics_dataset.me_top_cdm_condition_era rt
order by concept_field, category, count desc
;

SELECT *
from @metrics_project.@metrics_dataset.me_top_cdm_drug_era rt
order by concept_field, category, count desc
;

SELECT *
from @metrics_project.@metrics_dataset.me_top_cdm_dose_era rt
order by concept_field, category, count desc
;

SELECT *
from @metrics_project.@metrics_dataset.me_top_cdm_specimen rt
order by concept_field, category, count desc
;

SELECT *
from @metrics_project.@metrics_dataset.me_top_cdm_note rt
order by concept_field, category, count desc
;

SELECT *
from @metrics_project.@metrics_dataset.me_top_cdm_note_nlp rt
order by concept_field, category, count desc
;

SELECT *
from @metrics_project.@metrics_dataset.me_top_cdm_fact_relationship rt
order by concept_field, category, count desc
;

SELECT *
from @metrics_project.@metrics_dataset.me_top_cdm_cohort_attribute rt
order by concept_field, category, count desc
;

SELECT *
from @metrics_project.@metrics_dataset.me_top_cdm_metadata rt
order by concept_field, category, count desc
;

