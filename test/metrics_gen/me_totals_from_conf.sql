DROP TABLE IF EXISTS @metrics_project.@metrics_dataset.me_total;
CREATE TABLE @metrics_project.@metrics_dataset.me_total
(
    table_name        STRING,
    count             INT64
);

INSERT INTO @metrics_project.@metrics_dataset.me_total
SELECT
    'cdm_care_site'     AS table_name,
    COUNT(*)             AS count
from @etl_project.@etl_dataset.cdm_care_site ev
;

INSERT INTO @metrics_project.@metrics_dataset.me_total
SELECT
    'cdm_provider'     AS table_name,
    COUNT(*)             AS count
from @etl_project.@etl_dataset.cdm_provider ev
;

INSERT INTO @metrics_project.@metrics_dataset.me_total
SELECT
    'cdm_person'     AS table_name,
    COUNT(*)             AS count
from @etl_project.@etl_dataset.cdm_person ev
;

INSERT INTO @metrics_project.@metrics_dataset.me_total
SELECT
    'cdm_death'     AS table_name,
    COUNT(*)             AS count
from @etl_project.@etl_dataset.cdm_death ev
;

INSERT INTO @metrics_project.@metrics_dataset.me_total
SELECT
    'cdm_observation_period'     AS table_name,
    COUNT(*)             AS count
from @etl_project.@etl_dataset.cdm_observation_period ev
;

INSERT INTO @metrics_project.@metrics_dataset.me_total
SELECT
    'cdm_payer_plan_period'     AS table_name,
    COUNT(*)             AS count
from @etl_project.@etl_dataset.cdm_payer_plan_period ev
;

INSERT INTO @metrics_project.@metrics_dataset.me_total
SELECT
    'cdm_visit_occurrence'     AS table_name,
    COUNT(*)             AS count
from @etl_project.@etl_dataset.cdm_visit_occurrence ev
;

INSERT INTO @metrics_project.@metrics_dataset.me_total
SELECT
    'cdm_visit_detail'     AS table_name,
    COUNT(*)             AS count
from @etl_project.@etl_dataset.cdm_visit_detail ev
;

INSERT INTO @metrics_project.@metrics_dataset.me_total
SELECT
    'cdm_condition_occurrence'     AS table_name,
    COUNT(*)             AS count
from @etl_project.@etl_dataset.cdm_condition_occurrence ev
;

INSERT INTO @metrics_project.@metrics_dataset.me_total
SELECT
    'cdm_procedure_occurrence'     AS table_name,
    COUNT(*)             AS count
from @etl_project.@etl_dataset.cdm_procedure_occurrence ev
;

INSERT INTO @metrics_project.@metrics_dataset.me_total
SELECT
    'cdm_observation'     AS table_name,
    COUNT(*)             AS count
from @etl_project.@etl_dataset.cdm_observation ev
;

INSERT INTO @metrics_project.@metrics_dataset.me_total
SELECT
    'cdm_measurement'     AS table_name,
    COUNT(*)             AS count
from @etl_project.@etl_dataset.cdm_measurement ev
;

INSERT INTO @metrics_project.@metrics_dataset.me_total
SELECT
    'cdm_device_exposure'     AS table_name,
    COUNT(*)             AS count
from @etl_project.@etl_dataset.cdm_device_exposure ev
;

INSERT INTO @metrics_project.@metrics_dataset.me_total
SELECT
    'cdm_drug_exposure'     AS table_name,
    COUNT(*)             AS count
from @etl_project.@etl_dataset.cdm_drug_exposure ev
;

INSERT INTO @metrics_project.@metrics_dataset.me_total
SELECT
    'cdm_cost'     AS table_name,
    COUNT(*)             AS count
from @etl_project.@etl_dataset.cdm_cost ev
;

INSERT INTO @metrics_project.@metrics_dataset.me_total
SELECT
    'cdm_condition_era'     AS table_name,
    COUNT(*)             AS count
from @etl_project.@etl_dataset.cdm_condition_era ev
;

INSERT INTO @metrics_project.@metrics_dataset.me_total
SELECT
    'cdm_drug_era'     AS table_name,
    COUNT(*)             AS count
from @etl_project.@etl_dataset.cdm_drug_era ev
;

INSERT INTO @metrics_project.@metrics_dataset.me_total
SELECT
    'cdm_dose_era'     AS table_name,
    COUNT(*)             AS count
from @etl_project.@etl_dataset.cdm_dose_era ev
;

INSERT INTO @metrics_project.@metrics_dataset.me_total
SELECT
    'cdm_specimen'     AS table_name,
    COUNT(*)             AS count
from @etl_project.@etl_dataset.cdm_specimen ev
;

INSERT INTO @metrics_project.@metrics_dataset.me_total
SELECT
    'cdm_note'     AS table_name,
    COUNT(*)             AS count
from @etl_project.@etl_dataset.cdm_note ev
;

INSERT INTO @metrics_project.@metrics_dataset.me_total
SELECT
    'cdm_note_nlp'     AS table_name,
    COUNT(*)             AS count
from @etl_project.@etl_dataset.cdm_note_nlp ev
;

INSERT INTO @metrics_project.@metrics_dataset.me_total
SELECT
    'cdm_fact_relationship'     AS table_name,
    COUNT(*)             AS count
from @etl_project.@etl_dataset.cdm_fact_relationship ev
;

INSERT INTO @metrics_project.@metrics_dataset.me_total
SELECT
    'cdm_cohort_attribute'     AS table_name,
    COUNT(*)             AS count
from @etl_project.@etl_dataset.cdm_cohort_attribute ev
;

INSERT INTO @metrics_project.@metrics_dataset.me_total
SELECT
    'cdm_metadata'     AS table_name,
    COUNT(*)             AS count
from @etl_project.@etl_dataset.cdm_metadata ev
;

