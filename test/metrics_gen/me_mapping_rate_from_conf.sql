DROP TABLE IF EXISTS @metrics_project.@metrics_dataset.me_mapping_rate;
CREATE TABLE @metrics_project.@metrics_dataset.me_mapping_rate
(
    table_name        STRING,
    concept_field     STRING,
    count             INT64,
    percent           FLOAT64,
    total             INT64
);

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_care_site'        AS table_name,
    'place_of_service_concept_id'     AS concept_field,
    COUNT(CASE WHEN place_of_service_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(place_of_service_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_care_site ev
WHERE place_of_service_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_provider'        AS table_name,
    'specialty_concept_id'     AS concept_field,
    COUNT(CASE WHEN specialty_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(specialty_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_provider ev
WHERE specialty_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_provider'        AS table_name,
    'gender_concept_id'     AS concept_field,
    COUNT(CASE WHEN gender_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(gender_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_provider ev
WHERE gender_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_person'        AS table_name,
    'gender_concept_id'     AS concept_field,
    COUNT(CASE WHEN gender_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(gender_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_person ev
WHERE gender_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_person'        AS table_name,
    'race_concept_id'     AS concept_field,
    COUNT(CASE WHEN race_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(race_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_person ev
WHERE race_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_person'        AS table_name,
    'ethnicity_concept_id'     AS concept_field,
    COUNT(CASE WHEN ethnicity_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(ethnicity_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_person ev
WHERE ethnicity_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_death'        AS table_name,
    'death_type_concept_id'     AS concept_field,
    COUNT(CASE WHEN death_type_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(death_type_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_death ev
WHERE death_type_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_death'        AS table_name,
    'cause_concept_id'     AS concept_field,
    COUNT(CASE WHEN cause_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(cause_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_death ev
WHERE cause_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_observation_period'        AS table_name,
    'period_type_concept_id'     AS concept_field,
    COUNT(CASE WHEN period_type_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(period_type_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_observation_period ev
WHERE period_type_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_payer_plan_period'        AS table_name,
    'payer_concept_id'     AS concept_field,
    COUNT(CASE WHEN payer_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(payer_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_payer_plan_period ev
WHERE payer_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_payer_plan_period'        AS table_name,
    'plan_concept_id'     AS concept_field,
    COUNT(CASE WHEN plan_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(plan_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_payer_plan_period ev
WHERE plan_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_payer_plan_period'        AS table_name,
    'sponsor_concept_id'     AS concept_field,
    COUNT(CASE WHEN sponsor_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(sponsor_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_payer_plan_period ev
WHERE sponsor_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_payer_plan_period'        AS table_name,
    'stop_reason_concept_id'     AS concept_field,
    COUNT(CASE WHEN stop_reason_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(stop_reason_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_payer_plan_period ev
WHERE stop_reason_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_visit_occurrence'        AS table_name,
    'visit_concept_id'     AS concept_field,
    COUNT(CASE WHEN visit_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(visit_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_visit_occurrence ev
WHERE visit_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_visit_occurrence'        AS table_name,
    'visit_type_concept_id'     AS concept_field,
    COUNT(CASE WHEN visit_type_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(visit_type_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_visit_occurrence ev
WHERE visit_type_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_visit_occurrence'        AS table_name,
    'visit_source_concept_id'     AS concept_field,
    COUNT(CASE WHEN visit_source_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(visit_source_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_visit_occurrence ev
WHERE visit_source_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_visit_occurrence'        AS table_name,
    'admitting_source_concept_id'     AS concept_field,
    COUNT(CASE WHEN admitting_source_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(admitting_source_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_visit_occurrence ev
WHERE admitting_source_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_visit_occurrence'        AS table_name,
    'discharge_to_concept_id'     AS concept_field,
    COUNT(CASE WHEN discharge_to_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(discharge_to_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_visit_occurrence ev
WHERE discharge_to_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_visit_detail'        AS table_name,
    'visit_detail_concept_id'     AS concept_field,
    COUNT(CASE WHEN visit_detail_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(visit_detail_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_visit_detail ev
WHERE visit_detail_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_visit_detail'        AS table_name,
    'visit_detail_type_concept_id'     AS concept_field,
    COUNT(CASE WHEN visit_detail_type_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(visit_detail_type_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_visit_detail ev
WHERE visit_detail_type_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_visit_detail'        AS table_name,
    'visit_detail_source_concept_id'     AS concept_field,
    COUNT(CASE WHEN visit_detail_source_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(visit_detail_source_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_visit_detail ev
WHERE visit_detail_source_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_visit_detail'        AS table_name,
    'admitting_source_concept_id'     AS concept_field,
    COUNT(CASE WHEN admitting_source_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(admitting_source_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_visit_detail ev
WHERE admitting_source_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_visit_detail'        AS table_name,
    'discharge_to_concept_id'     AS concept_field,
    COUNT(CASE WHEN discharge_to_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(discharge_to_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_visit_detail ev
WHERE discharge_to_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_condition_occurrence'        AS table_name,
    'condition_concept_id'     AS concept_field,
    COUNT(CASE WHEN condition_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(condition_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_condition_occurrence ev
WHERE condition_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_condition_occurrence'        AS table_name,
    'condition_type_concept_id'     AS concept_field,
    COUNT(CASE WHEN condition_type_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(condition_type_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_condition_occurrence ev
WHERE condition_type_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_condition_occurrence'        AS table_name,
    'condition_status_concept_id'     AS concept_field,
    COUNT(CASE WHEN condition_status_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(condition_status_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_condition_occurrence ev
WHERE condition_status_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_procedure_occurrence'        AS table_name,
    'procedure_concept_id'     AS concept_field,
    COUNT(CASE WHEN procedure_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(procedure_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_procedure_occurrence ev
WHERE procedure_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_procedure_occurrence'        AS table_name,
    'procedure_type_concept_id'     AS concept_field,
    COUNT(CASE WHEN procedure_type_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(procedure_type_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_procedure_occurrence ev
WHERE procedure_type_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_procedure_occurrence'        AS table_name,
    'modifier_concept_id'     AS concept_field,
    COUNT(CASE WHEN modifier_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(modifier_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_procedure_occurrence ev
WHERE modifier_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_observation'        AS table_name,
    'observation_concept_id'     AS concept_field,
    COUNT(CASE WHEN observation_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(observation_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_observation ev
WHERE observation_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_observation'        AS table_name,
    'observation_type_concept_id'     AS concept_field,
    COUNT(CASE WHEN observation_type_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(observation_type_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_observation ev
WHERE observation_type_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_observation'        AS table_name,
    'value_as_concept_id'     AS concept_field,
    COUNT(CASE WHEN value_as_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(value_as_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_observation ev
WHERE value_as_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_observation'        AS table_name,
    'qualifier_concept_id'     AS concept_field,
    COUNT(CASE WHEN qualifier_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(qualifier_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_observation ev
WHERE qualifier_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_observation'        AS table_name,
    'unit_concept_id'     AS concept_field,
    COUNT(CASE WHEN unit_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(unit_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_observation ev
WHERE unit_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_measurement'        AS table_name,
    'measurement_concept_id'     AS concept_field,
    COUNT(CASE WHEN measurement_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(measurement_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_measurement ev
WHERE measurement_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_measurement'        AS table_name,
    'measurement_type_concept_id'     AS concept_field,
    COUNT(CASE WHEN measurement_type_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(measurement_type_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_measurement ev
WHERE measurement_type_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_measurement'        AS table_name,
    'operator_concept_id'     AS concept_field,
    COUNT(CASE WHEN operator_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(operator_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_measurement ev
WHERE operator_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_measurement'        AS table_name,
    'value_as_concept_id'     AS concept_field,
    COUNT(CASE WHEN value_as_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(value_as_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_measurement ev
WHERE value_as_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_measurement'        AS table_name,
    'unit_concept_id'     AS concept_field,
    COUNT(CASE WHEN unit_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(unit_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_measurement ev
WHERE unit_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_device_exposure'        AS table_name,
    'device_concept_id'     AS concept_field,
    COUNT(CASE WHEN device_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(device_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_device_exposure ev
WHERE device_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_device_exposure'        AS table_name,
    'device_type_concept_id'     AS concept_field,
    COUNT(CASE WHEN device_type_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(device_type_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_device_exposure ev
WHERE device_type_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_drug_exposure'        AS table_name,
    'drug_concept_id'     AS concept_field,
    COUNT(CASE WHEN drug_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(drug_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_drug_exposure ev
WHERE drug_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_drug_exposure'        AS table_name,
    'drug_type_concept_id'     AS concept_field,
    COUNT(CASE WHEN drug_type_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(drug_type_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_drug_exposure ev
WHERE drug_type_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_drug_exposure'        AS table_name,
    'route_concept_id'     AS concept_field,
    COUNT(CASE WHEN route_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(route_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_drug_exposure ev
WHERE route_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_cost'        AS table_name,
    'cost_type_concept_id'     AS concept_field,
    COUNT(CASE WHEN cost_type_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(cost_type_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_cost ev
WHERE cost_type_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_cost'        AS table_name,
    'currency_concept_id'     AS concept_field,
    COUNT(CASE WHEN currency_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(currency_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_cost ev
WHERE currency_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_cost'        AS table_name,
    'revenue_code_concept_id'     AS concept_field,
    COUNT(CASE WHEN revenue_code_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(revenue_code_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_cost ev
WHERE revenue_code_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_cost'        AS table_name,
    'drg_concept_id'     AS concept_field,
    COUNT(CASE WHEN drg_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(drg_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_cost ev
WHERE drg_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_condition_era'        AS table_name,
    'condition_concept_id'     AS concept_field,
    COUNT(CASE WHEN condition_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(condition_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_condition_era ev
WHERE condition_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_drug_era'        AS table_name,
    'drug_concept_id'     AS concept_field,
    COUNT(CASE WHEN drug_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(drug_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_drug_era ev
WHERE drug_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_dose_era'        AS table_name,
    'drug_concept_id'     AS concept_field,
    COUNT(CASE WHEN drug_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(drug_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_dose_era ev
WHERE drug_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_dose_era'        AS table_name,
    'unit_concept_id'     AS concept_field,
    COUNT(CASE WHEN unit_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(unit_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_dose_era ev
WHERE unit_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_specimen'        AS table_name,
    'specimen_concept_id'     AS concept_field,
    COUNT(CASE WHEN specimen_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(specimen_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_specimen ev
WHERE specimen_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_specimen'        AS table_name,
    'specimen_type_concept_id'     AS concept_field,
    COUNT(CASE WHEN specimen_type_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(specimen_type_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_specimen ev
WHERE specimen_type_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_specimen'        AS table_name,
    'unit_concept_id'     AS concept_field,
    COUNT(CASE WHEN unit_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(unit_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_specimen ev
WHERE unit_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_specimen'        AS table_name,
    'anatomic_site_concept_id'     AS concept_field,
    COUNT(CASE WHEN anatomic_site_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(anatomic_site_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_specimen ev
WHERE anatomic_site_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_specimen'        AS table_name,
    'disease_status_concept_id'     AS concept_field,
    COUNT(CASE WHEN disease_status_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(disease_status_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_specimen ev
WHERE disease_status_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_note'        AS table_name,
    'note_type_concept_id'     AS concept_field,
    COUNT(CASE WHEN note_type_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(note_type_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_note ev
WHERE note_type_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_note'        AS table_name,
    'note_class_concept_id'     AS concept_field,
    COUNT(CASE WHEN note_class_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(note_class_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_note ev
WHERE note_class_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_note'        AS table_name,
    'encoding_concept_id'     AS concept_field,
    COUNT(CASE WHEN encoding_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(encoding_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_note ev
WHERE encoding_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_note'        AS table_name,
    'language_concept_id'     AS concept_field,
    COUNT(CASE WHEN language_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(language_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_note ev
WHERE language_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_note_nlp'        AS table_name,
    'section_concept_id'     AS concept_field,
    COUNT(CASE WHEN section_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(section_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_note_nlp ev
WHERE section_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_note_nlp'        AS table_name,
    'note_nlp_concept_id'     AS concept_field,
    COUNT(CASE WHEN note_nlp_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(note_nlp_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_note_nlp ev
WHERE note_nlp_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_fact_relationship'        AS table_name,
    'domain_concept_id_1'     AS concept_field,
    COUNT(CASE WHEN domain_concept_id_1 > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(domain_concept_id_1 > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_fact_relationship ev
WHERE domain_concept_id_1 IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_fact_relationship'        AS table_name,
    'domain_concept_id_2'     AS concept_field,
    COUNT(CASE WHEN domain_concept_id_2 > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(domain_concept_id_2 > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_fact_relationship ev
WHERE domain_concept_id_2 IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_fact_relationship'        AS table_name,
    'relationship_concept_id'     AS concept_field,
    COUNT(CASE WHEN relationship_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(relationship_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_fact_relationship ev
WHERE relationship_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_cohort_attribute'        AS table_name,
    'value_as_concept_id'     AS concept_field,
    COUNT(CASE WHEN value_as_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(value_as_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_cohort_attribute ev
WHERE value_as_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_metadata'        AS table_name,
    'metadata_concept_id'     AS concept_field,
    COUNT(CASE WHEN metadata_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(metadata_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_metadata ev
WHERE metadata_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_metadata'        AS table_name,
    'metadata_type_concept_id'     AS concept_field,
    COUNT(CASE WHEN metadata_type_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(metadata_type_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_metadata ev
WHERE metadata_type_concept_id IS NOT NULL
;

INSERT INTO @metrics_project.@metrics_dataset.me_mapping_rate
SELECT
    'cdm_metadata'        AS table_name,
    'value_as_concept_id'     AS concept_field,
    COUNT(CASE WHEN value_as_concept_id > 0 THEN 1 ELSE NULL END)      AS count,
    IF(COUNT(*) > 0,
        ROUND(CAST(COUNT(IF(value_as_concept_id > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),
        0)    AS percent,
    COUNT(*)                AS total
FROM @etl_project.@etl_dataset.cdm_metadata ev
WHERE value_as_concept_id IS NOT NULL
;

