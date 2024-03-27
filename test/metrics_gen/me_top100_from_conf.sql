DROP TABLE IF EXISTS @metrics_project.@metrics_dataset.me_top_cdm_care_site;
CREATE TABLE @metrics_project.@metrics_dataset.me_top_cdm_care_site
(
    concept_field     STRING,
    category          STRING,
    source_value      STRING,
    concept_id        INT64,
    concept_name      STRING,
    count             INT64,
    percent           FLOAT64
);

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_care_site
SELECT
    'place_of_service_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.place_of_service_source_value AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_care_site ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.place_of_service_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_care_site'
where ev.place_of_service_concept_id <> 0
group by ev.place_of_service_source_value, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_care_site
SELECT
    'place_of_service_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.place_of_service_source_value AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_care_site ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_care_site'
where ev.place_of_service_concept_id = 0
group by ev.place_of_service_source_value, tt.count
order by count desc
limit 100
;

DROP TABLE IF EXISTS @metrics_project.@metrics_dataset.me_top_cdm_provider;
CREATE TABLE @metrics_project.@metrics_dataset.me_top_cdm_provider
(
    concept_field     STRING,
    category          STRING,
    source_value      STRING,
    concept_id        INT64,
    concept_name      STRING,
    count             INT64,
    percent           FLOAT64
);

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_provider
SELECT
    'specialty_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.specialty_source_value AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_provider ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.specialty_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_provider'
where ev.specialty_concept_id <> 0
group by ev.specialty_source_value, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_provider
SELECT
    'specialty_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.specialty_source_value AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_provider ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_provider'
where ev.specialty_concept_id = 0
group by ev.specialty_source_value, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_provider
SELECT
    'gender_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.gender_source_value AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_provider ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.gender_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_provider'
where ev.gender_concept_id <> 0
group by ev.gender_source_value, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_provider
SELECT
    'gender_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.gender_source_value AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_provider ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_provider'
where ev.gender_concept_id = 0
group by ev.gender_source_value, tt.count
order by count desc
limit 100
;

DROP TABLE IF EXISTS @metrics_project.@metrics_dataset.me_top_cdm_person;
CREATE TABLE @metrics_project.@metrics_dataset.me_top_cdm_person
(
    concept_field     STRING,
    category          STRING,
    source_value      STRING,
    concept_id        INT64,
    concept_name      STRING,
    count             INT64,
    percent           FLOAT64
);

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_person
SELECT
    'gender_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.gender_source_value AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_person ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.gender_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_person'
where ev.gender_concept_id <> 0
group by ev.gender_source_value, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_person
SELECT
    'gender_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.gender_source_value AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_person ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_person'
where ev.gender_concept_id = 0
group by ev.gender_source_value, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_person
SELECT
    'race_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.race_source_value AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_person ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.race_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_person'
where ev.race_concept_id <> 0
group by ev.race_source_value, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_person
SELECT
    'race_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.race_source_value AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_person ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_person'
where ev.race_concept_id = 0
group by ev.race_source_value, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_person
SELECT
    'ethnicity_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.ethnicity_source_value AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_person ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.ethnicity_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_person'
where ev.ethnicity_concept_id <> 0
group by ev.ethnicity_source_value, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_person
SELECT
    'ethnicity_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.ethnicity_source_value AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_person ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_person'
where ev.ethnicity_concept_id = 0
group by ev.ethnicity_source_value, tt.count
order by count desc
limit 100
;

DROP TABLE IF EXISTS @metrics_project.@metrics_dataset.me_top_cdm_death;
CREATE TABLE @metrics_project.@metrics_dataset.me_top_cdm_death
(
    concept_field     STRING,
    category          STRING,
    source_value      STRING,
    concept_id        INT64,
    concept_name      STRING,
    count             INT64,
    percent           FLOAT64
);

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_death
SELECT
    'death_type_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.death_type_concept_id AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_death ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.death_type_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_death'
where ev.death_type_concept_id <> 0
group by ev.death_type_concept_id, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_death
SELECT
    'death_type_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.death_type_concept_id AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_death ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_death'
where ev.death_type_concept_id = 0
group by ev.death_type_concept_id, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_death
SELECT
    'cause_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.cause_source_value AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_death ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.cause_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_death'
where ev.cause_concept_id <> 0
group by ev.cause_source_value, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_death
SELECT
    'cause_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.cause_source_value AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_death ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_death'
where ev.cause_concept_id = 0
group by ev.cause_source_value, tt.count
order by count desc
limit 100
;

DROP TABLE IF EXISTS @metrics_project.@metrics_dataset.me_top_cdm_observation_period;
CREATE TABLE @metrics_project.@metrics_dataset.me_top_cdm_observation_period
(
    concept_field     STRING,
    category          STRING,
    source_value      STRING,
    concept_id        INT64,
    concept_name      STRING,
    count             INT64,
    percent           FLOAT64
);

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_observation_period
SELECT
    'period_type_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.period_type_concept_id AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_observation_period ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.period_type_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_observation_period'
where ev.period_type_concept_id <> 0
group by ev.period_type_concept_id, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_observation_period
SELECT
    'period_type_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.period_type_concept_id AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_observation_period ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_observation_period'
where ev.period_type_concept_id = 0
group by ev.period_type_concept_id, tt.count
order by count desc
limit 100
;

DROP TABLE IF EXISTS @metrics_project.@metrics_dataset.me_top_cdm_payer_plan_period;
CREATE TABLE @metrics_project.@metrics_dataset.me_top_cdm_payer_plan_period
(
    concept_field     STRING,
    category          STRING,
    source_value      STRING,
    concept_id        INT64,
    concept_name      STRING,
    count             INT64,
    percent           FLOAT64
);

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_payer_plan_period
SELECT
    'payer_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.payer_source_value AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_payer_plan_period ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.payer_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_payer_plan_period'
where ev.payer_concept_id <> 0
group by ev.payer_source_value, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_payer_plan_period
SELECT
    'payer_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.payer_source_value AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_payer_plan_period ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_payer_plan_period'
where ev.payer_concept_id = 0
group by ev.payer_source_value, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_payer_plan_period
SELECT
    'plan_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.plan_source_value AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_payer_plan_period ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.plan_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_payer_plan_period'
where ev.plan_concept_id <> 0
group by ev.plan_source_value, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_payer_plan_period
SELECT
    'plan_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.plan_source_value AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_payer_plan_period ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_payer_plan_period'
where ev.plan_concept_id = 0
group by ev.plan_source_value, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_payer_plan_period
SELECT
    'sponsor_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.sponsor_source_value AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_payer_plan_period ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.sponsor_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_payer_plan_period'
where ev.sponsor_concept_id <> 0
group by ev.sponsor_source_value, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_payer_plan_period
SELECT
    'sponsor_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.sponsor_source_value AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_payer_plan_period ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_payer_plan_period'
where ev.sponsor_concept_id = 0
group by ev.sponsor_source_value, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_payer_plan_period
SELECT
    'stop_reason_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.stop_reason_source_value AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_payer_plan_period ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.stop_reason_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_payer_plan_period'
where ev.stop_reason_concept_id <> 0
group by ev.stop_reason_source_value, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_payer_plan_period
SELECT
    'stop_reason_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.stop_reason_source_value AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_payer_plan_period ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_payer_plan_period'
where ev.stop_reason_concept_id = 0
group by ev.stop_reason_source_value, tt.count
order by count desc
limit 100
;

DROP TABLE IF EXISTS @metrics_project.@metrics_dataset.me_top_cdm_visit_occurrence;
CREATE TABLE @metrics_project.@metrics_dataset.me_top_cdm_visit_occurrence
(
    concept_field     STRING,
    category          STRING,
    source_value      STRING,
    concept_id        INT64,
    concept_name      STRING,
    count             INT64,
    percent           FLOAT64
);

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_visit_occurrence
SELECT
    'visit_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.visit_concept_id AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_visit_occurrence ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.visit_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_visit_occurrence'
where ev.visit_concept_id <> 0
group by ev.visit_concept_id, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_visit_occurrence
SELECT
    'visit_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.visit_concept_id AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_visit_occurrence ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_visit_occurrence'
where ev.visit_concept_id = 0
group by ev.visit_concept_id, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_visit_occurrence
SELECT
    'visit_type_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.visit_type_concept_id AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_visit_occurrence ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.visit_type_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_visit_occurrence'
where ev.visit_type_concept_id <> 0
group by ev.visit_type_concept_id, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_visit_occurrence
SELECT
    'visit_type_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.visit_type_concept_id AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_visit_occurrence ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_visit_occurrence'
where ev.visit_type_concept_id = 0
group by ev.visit_type_concept_id, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_visit_occurrence
SELECT
    'visit_source_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.visit_source_concept_id AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_visit_occurrence ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.visit_source_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_visit_occurrence'
where ev.visit_source_concept_id <> 0
group by ev.visit_source_concept_id, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_visit_occurrence
SELECT
    'visit_source_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.visit_source_concept_id AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_visit_occurrence ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_visit_occurrence'
where ev.visit_source_concept_id = 0
group by ev.visit_source_concept_id, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_visit_occurrence
SELECT
    'admitting_source_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.admitting_source_value AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_visit_occurrence ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.admitting_source_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_visit_occurrence'
where ev.admitting_source_concept_id <> 0
group by ev.admitting_source_value, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_visit_occurrence
SELECT
    'admitting_source_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.admitting_source_value AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_visit_occurrence ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_visit_occurrence'
where ev.admitting_source_concept_id = 0
group by ev.admitting_source_value, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_visit_occurrence
SELECT
    'discharge_to_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.discharge_to_source_value AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_visit_occurrence ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.discharge_to_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_visit_occurrence'
where ev.discharge_to_concept_id <> 0
group by ev.discharge_to_source_value, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_visit_occurrence
SELECT
    'discharge_to_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.discharge_to_source_value AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_visit_occurrence ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_visit_occurrence'
where ev.discharge_to_concept_id = 0
group by ev.discharge_to_source_value, tt.count
order by count desc
limit 100
;

DROP TABLE IF EXISTS @metrics_project.@metrics_dataset.me_top_cdm_visit_detail;
CREATE TABLE @metrics_project.@metrics_dataset.me_top_cdm_visit_detail
(
    concept_field     STRING,
    category          STRING,
    source_value      STRING,
    concept_id        INT64,
    concept_name      STRING,
    count             INT64,
    percent           FLOAT64
);

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_visit_detail
SELECT
    'visit_detail_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.visit_detail_concept_id AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_visit_detail ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.visit_detail_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_visit_detail'
where ev.visit_detail_concept_id <> 0
group by ev.visit_detail_concept_id, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_visit_detail
SELECT
    'visit_detail_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.visit_detail_concept_id AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_visit_detail ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_visit_detail'
where ev.visit_detail_concept_id = 0
group by ev.visit_detail_concept_id, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_visit_detail
SELECT
    'visit_detail_type_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.visit_detail_type_concept_id AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_visit_detail ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.visit_detail_type_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_visit_detail'
where ev.visit_detail_type_concept_id <> 0
group by ev.visit_detail_type_concept_id, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_visit_detail
SELECT
    'visit_detail_type_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.visit_detail_type_concept_id AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_visit_detail ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_visit_detail'
where ev.visit_detail_type_concept_id = 0
group by ev.visit_detail_type_concept_id, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_visit_detail
SELECT
    'visit_detail_source_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.visit_detail_source_concept_id AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_visit_detail ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.visit_detail_source_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_visit_detail'
where ev.visit_detail_source_concept_id <> 0
group by ev.visit_detail_source_concept_id, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_visit_detail
SELECT
    'visit_detail_source_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.visit_detail_source_concept_id AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_visit_detail ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_visit_detail'
where ev.visit_detail_source_concept_id = 0
group by ev.visit_detail_source_concept_id, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_visit_detail
SELECT
    'admitting_source_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.admitting_source_value AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_visit_detail ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.admitting_source_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_visit_detail'
where ev.admitting_source_concept_id <> 0
group by ev.admitting_source_value, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_visit_detail
SELECT
    'admitting_source_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.admitting_source_value AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_visit_detail ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_visit_detail'
where ev.admitting_source_concept_id = 0
group by ev.admitting_source_value, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_visit_detail
SELECT
    'discharge_to_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.discharge_to_source_value AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_visit_detail ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.discharge_to_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_visit_detail'
where ev.discharge_to_concept_id <> 0
group by ev.discharge_to_source_value, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_visit_detail
SELECT
    'discharge_to_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.discharge_to_source_value AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_visit_detail ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_visit_detail'
where ev.discharge_to_concept_id = 0
group by ev.discharge_to_source_value, tt.count
order by count desc
limit 100
;

DROP TABLE IF EXISTS @metrics_project.@metrics_dataset.me_top_cdm_condition_occurrence;
CREATE TABLE @metrics_project.@metrics_dataset.me_top_cdm_condition_occurrence
(
    concept_field     STRING,
    category          STRING,
    source_value      STRING,
    concept_id        INT64,
    concept_name      STRING,
    count             INT64,
    percent           FLOAT64
);

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_condition_occurrence
SELECT
    'condition_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.condition_source_value AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_condition_occurrence ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.condition_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_condition_occurrence'
where ev.condition_concept_id <> 0
group by ev.condition_source_value, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_condition_occurrence
SELECT
    'condition_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.condition_source_value AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_condition_occurrence ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_condition_occurrence'
where ev.condition_concept_id = 0
group by ev.condition_source_value, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_condition_occurrence
SELECT
    'condition_type_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.condition_type_concept_id AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_condition_occurrence ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.condition_type_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_condition_occurrence'
where ev.condition_type_concept_id <> 0
group by ev.condition_type_concept_id, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_condition_occurrence
SELECT
    'condition_type_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.condition_type_concept_id AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_condition_occurrence ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_condition_occurrence'
where ev.condition_type_concept_id = 0
group by ev.condition_type_concept_id, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_condition_occurrence
SELECT
    'condition_status_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.condition_status_source_value AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_condition_occurrence ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.condition_status_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_condition_occurrence'
where ev.condition_status_concept_id <> 0
group by ev.condition_status_source_value, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_condition_occurrence
SELECT
    'condition_status_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.condition_status_source_value AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_condition_occurrence ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_condition_occurrence'
where ev.condition_status_concept_id = 0
group by ev.condition_status_source_value, tt.count
order by count desc
limit 100
;

DROP TABLE IF EXISTS @metrics_project.@metrics_dataset.me_top_cdm_procedure_occurrence;
CREATE TABLE @metrics_project.@metrics_dataset.me_top_cdm_procedure_occurrence
(
    concept_field     STRING,
    category          STRING,
    source_value      STRING,
    concept_id        INT64,
    concept_name      STRING,
    count             INT64,
    percent           FLOAT64
);

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_procedure_occurrence
SELECT
    'procedure_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.procedure_source_value AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_procedure_occurrence ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.procedure_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_procedure_occurrence'
where ev.procedure_concept_id <> 0
group by ev.procedure_source_value, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_procedure_occurrence
SELECT
    'procedure_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.procedure_source_value AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_procedure_occurrence ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_procedure_occurrence'
where ev.procedure_concept_id = 0
group by ev.procedure_source_value, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_procedure_occurrence
SELECT
    'procedure_type_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.procedure_type_concept_id AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_procedure_occurrence ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.procedure_type_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_procedure_occurrence'
where ev.procedure_type_concept_id <> 0
group by ev.procedure_type_concept_id, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_procedure_occurrence
SELECT
    'procedure_type_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.procedure_type_concept_id AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_procedure_occurrence ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_procedure_occurrence'
where ev.procedure_type_concept_id = 0
group by ev.procedure_type_concept_id, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_procedure_occurrence
SELECT
    'modifier_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.modifier_source_value AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_procedure_occurrence ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.modifier_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_procedure_occurrence'
where ev.modifier_concept_id <> 0
group by ev.modifier_source_value, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_procedure_occurrence
SELECT
    'modifier_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.modifier_source_value AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_procedure_occurrence ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_procedure_occurrence'
where ev.modifier_concept_id = 0
group by ev.modifier_source_value, tt.count
order by count desc
limit 100
;

DROP TABLE IF EXISTS @metrics_project.@metrics_dataset.me_top_cdm_observation;
CREATE TABLE @metrics_project.@metrics_dataset.me_top_cdm_observation
(
    concept_field     STRING,
    category          STRING,
    source_value      STRING,
    concept_id        INT64,
    concept_name      STRING,
    count             INT64,
    percent           FLOAT64
);

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_observation
SELECT
    'observation_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.observation_source_value AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_observation ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.observation_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_observation'
where ev.observation_concept_id <> 0
group by ev.observation_source_value, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_observation
SELECT
    'observation_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.observation_source_value AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_observation ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_observation'
where ev.observation_concept_id = 0
group by ev.observation_source_value, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_observation
SELECT
    'observation_type_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.observation_type_concept_id AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_observation ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.observation_type_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_observation'
where ev.observation_type_concept_id <> 0
group by ev.observation_type_concept_id, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_observation
SELECT
    'observation_type_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.observation_type_concept_id AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_observation ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_observation'
where ev.observation_type_concept_id = 0
group by ev.observation_type_concept_id, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_observation
SELECT
    'value_as_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.value_as_string AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_observation ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.value_as_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_observation'
where ev.value_as_concept_id <> 0
group by ev.value_as_string, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_observation
SELECT
    'value_as_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.value_as_string AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_observation ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_observation'
where ev.value_as_concept_id = 0
group by ev.value_as_string, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_observation
SELECT
    'qualifier_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.qualifier_source_value AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_observation ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.qualifier_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_observation'
where ev.qualifier_concept_id <> 0
group by ev.qualifier_source_value, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_observation
SELECT
    'qualifier_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.qualifier_source_value AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_observation ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_observation'
where ev.qualifier_concept_id = 0
group by ev.qualifier_source_value, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_observation
SELECT
    'unit_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.unit_source_value AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_observation ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.unit_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_observation'
where ev.unit_concept_id <> 0
group by ev.unit_source_value, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_observation
SELECT
    'unit_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.unit_source_value AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_observation ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_observation'
where ev.unit_concept_id = 0
group by ev.unit_source_value, tt.count
order by count desc
limit 100
;

DROP TABLE IF EXISTS @metrics_project.@metrics_dataset.me_top_cdm_measurement;
CREATE TABLE @metrics_project.@metrics_dataset.me_top_cdm_measurement
(
    concept_field     STRING,
    category          STRING,
    source_value      STRING,
    concept_id        INT64,
    concept_name      STRING,
    count             INT64,
    percent           FLOAT64
);

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_measurement
SELECT
    'measurement_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.measurement_source_value AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_measurement ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.measurement_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_measurement'
where ev.measurement_concept_id <> 0
group by ev.measurement_source_value, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_measurement
SELECT
    'measurement_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.measurement_source_value AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_measurement ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_measurement'
where ev.measurement_concept_id = 0
group by ev.measurement_source_value, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_measurement
SELECT
    'measurement_type_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.measurement_type_concept_id AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_measurement ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.measurement_type_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_measurement'
where ev.measurement_type_concept_id <> 0
group by ev.measurement_type_concept_id, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_measurement
SELECT
    'measurement_type_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.measurement_type_concept_id AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_measurement ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_measurement'
where ev.measurement_type_concept_id = 0
group by ev.measurement_type_concept_id, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_measurement
SELECT
    'operator_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.operator_concept_id AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_measurement ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.operator_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_measurement'
where ev.operator_concept_id <> 0
group by ev.operator_concept_id, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_measurement
SELECT
    'operator_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.operator_concept_id AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_measurement ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_measurement'
where ev.operator_concept_id = 0
group by ev.operator_concept_id, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_measurement
SELECT
    'value_as_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.value_source_value AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_measurement ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.value_as_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_measurement'
where ev.value_as_concept_id <> 0
group by ev.value_source_value, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_measurement
SELECT
    'value_as_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.value_source_value AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_measurement ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_measurement'
where ev.value_as_concept_id = 0
group by ev.value_source_value, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_measurement
SELECT
    'unit_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.unit_source_value AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_measurement ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.unit_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_measurement'
where ev.unit_concept_id <> 0
group by ev.unit_source_value, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_measurement
SELECT
    'unit_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.unit_source_value AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_measurement ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_measurement'
where ev.unit_concept_id = 0
group by ev.unit_source_value, tt.count
order by count desc
limit 100
;

DROP TABLE IF EXISTS @metrics_project.@metrics_dataset.me_top_cdm_device_exposure;
CREATE TABLE @metrics_project.@metrics_dataset.me_top_cdm_device_exposure
(
    concept_field     STRING,
    category          STRING,
    source_value      STRING,
    concept_id        INT64,
    concept_name      STRING,
    count             INT64,
    percent           FLOAT64
);

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_device_exposure
SELECT
    'device_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.device_source_value AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_device_exposure ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.device_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_device_exposure'
where ev.device_concept_id <> 0
group by ev.device_source_value, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_device_exposure
SELECT
    'device_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.device_source_value AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_device_exposure ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_device_exposure'
where ev.device_concept_id = 0
group by ev.device_source_value, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_device_exposure
SELECT
    'device_type_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.device_type_concept_id AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_device_exposure ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.device_type_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_device_exposure'
where ev.device_type_concept_id <> 0
group by ev.device_type_concept_id, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_device_exposure
SELECT
    'device_type_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.device_type_concept_id AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_device_exposure ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_device_exposure'
where ev.device_type_concept_id = 0
group by ev.device_type_concept_id, tt.count
order by count desc
limit 100
;

DROP TABLE IF EXISTS @metrics_project.@metrics_dataset.me_top_cdm_drug_exposure;
CREATE TABLE @metrics_project.@metrics_dataset.me_top_cdm_drug_exposure
(
    concept_field     STRING,
    category          STRING,
    source_value      STRING,
    concept_id        INT64,
    concept_name      STRING,
    count             INT64,
    percent           FLOAT64
);

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_drug_exposure
SELECT
    'drug_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.drug_source_value AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_drug_exposure ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.drug_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_drug_exposure'
where ev.drug_concept_id <> 0
group by ev.drug_source_value, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_drug_exposure
SELECT
    'drug_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.drug_source_value AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_drug_exposure ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_drug_exposure'
where ev.drug_concept_id = 0
group by ev.drug_source_value, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_drug_exposure
SELECT
    'drug_type_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.drug_type_concept_id AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_drug_exposure ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.drug_type_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_drug_exposure'
where ev.drug_type_concept_id <> 0
group by ev.drug_type_concept_id, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_drug_exposure
SELECT
    'drug_type_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.drug_type_concept_id AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_drug_exposure ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_drug_exposure'
where ev.drug_type_concept_id = 0
group by ev.drug_type_concept_id, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_drug_exposure
SELECT
    'route_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.route_source_value AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_drug_exposure ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.route_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_drug_exposure'
where ev.route_concept_id <> 0
group by ev.route_source_value, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_drug_exposure
SELECT
    'route_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.route_source_value AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_drug_exposure ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_drug_exposure'
where ev.route_concept_id = 0
group by ev.route_source_value, tt.count
order by count desc
limit 100
;

DROP TABLE IF EXISTS @metrics_project.@metrics_dataset.me_top_cdm_cost;
CREATE TABLE @metrics_project.@metrics_dataset.me_top_cdm_cost
(
    concept_field     STRING,
    category          STRING,
    source_value      STRING,
    concept_id        INT64,
    concept_name      STRING,
    count             INT64,
    percent           FLOAT64
);

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_cost
SELECT
    'cost_type_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.cost_type_concept_id AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_cost ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.cost_type_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_cost'
where ev.cost_type_concept_id <> 0
group by ev.cost_type_concept_id, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_cost
SELECT
    'cost_type_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.cost_type_concept_id AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_cost ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_cost'
where ev.cost_type_concept_id = 0
group by ev.cost_type_concept_id, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_cost
SELECT
    'currency_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.currency_concept_id AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_cost ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.currency_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_cost'
where ev.currency_concept_id <> 0
group by ev.currency_concept_id, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_cost
SELECT
    'currency_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.currency_concept_id AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_cost ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_cost'
where ev.currency_concept_id = 0
group by ev.currency_concept_id, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_cost
SELECT
    'revenue_code_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.revenue_code_source_value AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_cost ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.revenue_code_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_cost'
where ev.revenue_code_concept_id <> 0
group by ev.revenue_code_source_value, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_cost
SELECT
    'revenue_code_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.revenue_code_source_value AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_cost ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_cost'
where ev.revenue_code_concept_id = 0
group by ev.revenue_code_source_value, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_cost
SELECT
    'drg_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.drg_source_value AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_cost ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.drg_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_cost'
where ev.drg_concept_id <> 0
group by ev.drg_source_value, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_cost
SELECT
    'drg_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.drg_source_value AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_cost ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_cost'
where ev.drg_concept_id = 0
group by ev.drg_source_value, tt.count
order by count desc
limit 100
;

DROP TABLE IF EXISTS @metrics_project.@metrics_dataset.me_top_cdm_condition_era;
CREATE TABLE @metrics_project.@metrics_dataset.me_top_cdm_condition_era
(
    concept_field     STRING,
    category          STRING,
    source_value      STRING,
    concept_id        INT64,
    concept_name      STRING,
    count             INT64,
    percent           FLOAT64
);

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_condition_era
SELECT
    'condition_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.condition_concept_id AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_condition_era ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.condition_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_condition_era'
where ev.condition_concept_id <> 0
group by ev.condition_concept_id, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_condition_era
SELECT
    'condition_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.condition_concept_id AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_condition_era ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_condition_era'
where ev.condition_concept_id = 0
group by ev.condition_concept_id, tt.count
order by count desc
limit 100
;

DROP TABLE IF EXISTS @metrics_project.@metrics_dataset.me_top_cdm_drug_era;
CREATE TABLE @metrics_project.@metrics_dataset.me_top_cdm_drug_era
(
    concept_field     STRING,
    category          STRING,
    source_value      STRING,
    concept_id        INT64,
    concept_name      STRING,
    count             INT64,
    percent           FLOAT64
);

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_drug_era
SELECT
    'drug_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.drug_concept_id AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_drug_era ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.drug_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_drug_era'
where ev.drug_concept_id <> 0
group by ev.drug_concept_id, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_drug_era
SELECT
    'drug_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.drug_concept_id AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_drug_era ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_drug_era'
where ev.drug_concept_id = 0
group by ev.drug_concept_id, tt.count
order by count desc
limit 100
;

DROP TABLE IF EXISTS @metrics_project.@metrics_dataset.me_top_cdm_dose_era;
CREATE TABLE @metrics_project.@metrics_dataset.me_top_cdm_dose_era
(
    concept_field     STRING,
    category          STRING,
    source_value      STRING,
    concept_id        INT64,
    concept_name      STRING,
    count             INT64,
    percent           FLOAT64
);

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_dose_era
SELECT
    'drug_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.drug_concept_id AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_dose_era ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.drug_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_dose_era'
where ev.drug_concept_id <> 0
group by ev.drug_concept_id, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_dose_era
SELECT
    'drug_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.drug_concept_id AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_dose_era ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_dose_era'
where ev.drug_concept_id = 0
group by ev.drug_concept_id, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_dose_era
SELECT
    'unit_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.unit_concept_id AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_dose_era ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.unit_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_dose_era'
where ev.unit_concept_id <> 0
group by ev.unit_concept_id, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_dose_era
SELECT
    'unit_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.unit_concept_id AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_dose_era ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_dose_era'
where ev.unit_concept_id = 0
group by ev.unit_concept_id, tt.count
order by count desc
limit 100
;

DROP TABLE IF EXISTS @metrics_project.@metrics_dataset.me_top_cdm_specimen;
CREATE TABLE @metrics_project.@metrics_dataset.me_top_cdm_specimen
(
    concept_field     STRING,
    category          STRING,
    source_value      STRING,
    concept_id        INT64,
    concept_name      STRING,
    count             INT64,
    percent           FLOAT64
);

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_specimen
SELECT
    'specimen_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.specimen_source_value AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_specimen ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.specimen_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_specimen'
where ev.specimen_concept_id <> 0
group by ev.specimen_source_value, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_specimen
SELECT
    'specimen_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.specimen_source_value AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_specimen ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_specimen'
where ev.specimen_concept_id = 0
group by ev.specimen_source_value, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_specimen
SELECT
    'specimen_type_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.specimen_type_concept_id AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_specimen ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.specimen_type_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_specimen'
where ev.specimen_type_concept_id <> 0
group by ev.specimen_type_concept_id, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_specimen
SELECT
    'specimen_type_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.specimen_type_concept_id AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_specimen ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_specimen'
where ev.specimen_type_concept_id = 0
group by ev.specimen_type_concept_id, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_specimen
SELECT
    'unit_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.unit_source_value AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_specimen ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.unit_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_specimen'
where ev.unit_concept_id <> 0
group by ev.unit_source_value, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_specimen
SELECT
    'unit_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.unit_source_value AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_specimen ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_specimen'
where ev.unit_concept_id = 0
group by ev.unit_source_value, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_specimen
SELECT
    'anatomic_site_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.anatomic_site_source_value AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_specimen ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.anatomic_site_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_specimen'
where ev.anatomic_site_concept_id <> 0
group by ev.anatomic_site_source_value, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_specimen
SELECT
    'anatomic_site_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.anatomic_site_source_value AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_specimen ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_specimen'
where ev.anatomic_site_concept_id = 0
group by ev.anatomic_site_source_value, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_specimen
SELECT
    'disease_status_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.disease_status_source_value AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_specimen ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.disease_status_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_specimen'
where ev.disease_status_concept_id <> 0
group by ev.disease_status_source_value, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_specimen
SELECT
    'disease_status_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.disease_status_source_value AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_specimen ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_specimen'
where ev.disease_status_concept_id = 0
group by ev.disease_status_source_value, tt.count
order by count desc
limit 100
;

DROP TABLE IF EXISTS @metrics_project.@metrics_dataset.me_top_cdm_note;
CREATE TABLE @metrics_project.@metrics_dataset.me_top_cdm_note
(
    concept_field     STRING,
    category          STRING,
    source_value      STRING,
    concept_id        INT64,
    concept_name      STRING,
    count             INT64,
    percent           FLOAT64
);

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_note
SELECT
    'note_type_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.note_type_concept_id AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_note ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.note_type_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_note'
where ev.note_type_concept_id <> 0
group by ev.note_type_concept_id, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_note
SELECT
    'note_type_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.note_type_concept_id AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_note ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_note'
where ev.note_type_concept_id = 0
group by ev.note_type_concept_id, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_note
SELECT
    'note_class_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.note_class_concept_id AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_note ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.note_class_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_note'
where ev.note_class_concept_id <> 0
group by ev.note_class_concept_id, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_note
SELECT
    'note_class_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.note_class_concept_id AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_note ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_note'
where ev.note_class_concept_id = 0
group by ev.note_class_concept_id, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_note
SELECT
    'encoding_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.encoding_concept_id AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_note ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.encoding_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_note'
where ev.encoding_concept_id <> 0
group by ev.encoding_concept_id, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_note
SELECT
    'encoding_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.encoding_concept_id AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_note ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_note'
where ev.encoding_concept_id = 0
group by ev.encoding_concept_id, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_note
SELECT
    'language_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.language_concept_id AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_note ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.language_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_note'
where ev.language_concept_id <> 0
group by ev.language_concept_id, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_note
SELECT
    'language_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.language_concept_id AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_note ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_note'
where ev.language_concept_id = 0
group by ev.language_concept_id, tt.count
order by count desc
limit 100
;

DROP TABLE IF EXISTS @metrics_project.@metrics_dataset.me_top_cdm_note_nlp;
CREATE TABLE @metrics_project.@metrics_dataset.me_top_cdm_note_nlp
(
    concept_field     STRING,
    category          STRING,
    source_value      STRING,
    concept_id        INT64,
    concept_name      STRING,
    count             INT64,
    percent           FLOAT64
);

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_note_nlp
SELECT
    'section_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.section_concept_id AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_note_nlp ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.section_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_note_nlp'
where ev.section_concept_id <> 0
group by ev.section_concept_id, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_note_nlp
SELECT
    'section_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.section_concept_id AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_note_nlp ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_note_nlp'
where ev.section_concept_id = 0
group by ev.section_concept_id, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_note_nlp
SELECT
    'note_nlp_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.note_nlp_concept_id AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_note_nlp ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.note_nlp_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_note_nlp'
where ev.note_nlp_concept_id <> 0
group by ev.note_nlp_concept_id, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_note_nlp
SELECT
    'note_nlp_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.note_nlp_concept_id AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_note_nlp ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_note_nlp'
where ev.note_nlp_concept_id = 0
group by ev.note_nlp_concept_id, tt.count
order by count desc
limit 100
;

DROP TABLE IF EXISTS @metrics_project.@metrics_dataset.me_top_cdm_fact_relationship;
CREATE TABLE @metrics_project.@metrics_dataset.me_top_cdm_fact_relationship
(
    concept_field     STRING,
    category          STRING,
    source_value      STRING,
    concept_id        INT64,
    concept_name      STRING,
    count             INT64,
    percent           FLOAT64
);

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_fact_relationship
SELECT
    'domain_concept_id_1'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.domain_concept_id_1 AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_fact_relationship ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.domain_concept_id_1 = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_fact_relationship'
where ev.domain_concept_id_1 <> 0
group by ev.domain_concept_id_1, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_fact_relationship
SELECT
    'domain_concept_id_1'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.domain_concept_id_1 AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_fact_relationship ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_fact_relationship'
where ev.domain_concept_id_1 = 0
group by ev.domain_concept_id_1, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_fact_relationship
SELECT
    'domain_concept_id_2'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.domain_concept_id_2 AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_fact_relationship ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.domain_concept_id_2 = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_fact_relationship'
where ev.domain_concept_id_2 <> 0
group by ev.domain_concept_id_2, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_fact_relationship
SELECT
    'domain_concept_id_2'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.domain_concept_id_2 AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_fact_relationship ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_fact_relationship'
where ev.domain_concept_id_2 = 0
group by ev.domain_concept_id_2, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_fact_relationship
SELECT
    'relationship_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.relationship_concept_id AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_fact_relationship ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.relationship_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_fact_relationship'
where ev.relationship_concept_id <> 0
group by ev.relationship_concept_id, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_fact_relationship
SELECT
    'relationship_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.relationship_concept_id AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_fact_relationship ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_fact_relationship'
where ev.relationship_concept_id = 0
group by ev.relationship_concept_id, tt.count
order by count desc
limit 100
;

DROP TABLE IF EXISTS @metrics_project.@metrics_dataset.me_top_cdm_cohort_attribute;
CREATE TABLE @metrics_project.@metrics_dataset.me_top_cdm_cohort_attribute
(
    concept_field     STRING,
    category          STRING,
    source_value      STRING,
    concept_id        INT64,
    concept_name      STRING,
    count             INT64,
    percent           FLOAT64
);

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_cohort_attribute
SELECT
    'value_as_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.value_as_concept_id AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_cohort_attribute ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.value_as_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_cohort_attribute'
where ev.value_as_concept_id <> 0
group by ev.value_as_concept_id, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_cohort_attribute
SELECT
    'value_as_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.value_as_concept_id AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_cohort_attribute ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_cohort_attribute'
where ev.value_as_concept_id = 0
group by ev.value_as_concept_id, tt.count
order by count desc
limit 100
;

DROP TABLE IF EXISTS @metrics_project.@metrics_dataset.me_top_cdm_metadata;
CREATE TABLE @metrics_project.@metrics_dataset.me_top_cdm_metadata
(
    concept_field     STRING,
    category          STRING,
    source_value      STRING,
    concept_id        INT64,
    concept_name      STRING,
    count             INT64,
    percent           FLOAT64
);

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_metadata
SELECT
    'metadata_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.metadata_concept_id AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_metadata ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.metadata_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_metadata'
where ev.metadata_concept_id <> 0
group by ev.metadata_concept_id, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_metadata
SELECT
    'metadata_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.metadata_concept_id AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_metadata ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_metadata'
where ev.metadata_concept_id = 0
group by ev.metadata_concept_id, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_metadata
SELECT
    'metadata_type_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.metadata_type_concept_id AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_metadata ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.metadata_type_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_metadata'
where ev.metadata_type_concept_id <> 0
group by ev.metadata_type_concept_id, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_metadata
SELECT
    'metadata_type_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.metadata_type_concept_id AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_metadata ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_metadata'
where ev.metadata_type_concept_id = 0
group by ev.metadata_type_concept_id, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_metadata
SELECT
    'value_as_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.value_as_concept_id AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_metadata ev
inner join @etl_project.@etl_dataset.voc_concept vc
    on ev.value_as_concept_id = vc.concept_id
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_metadata'
where ev.value_as_concept_id <> 0
group by ev.value_as_concept_id, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO @metrics_project.@metrics_dataset.me_top_cdm_metadata
SELECT
    'value_as_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.value_as_concept_id AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from @etl_project.@etl_dataset.cdm_metadata ev
inner join @metrics_project.@metrics_dataset.me_total tt
    on tt.table_name = 'cdm_metadata'
where ev.value_as_concept_id = 0
group by ev.value_as_concept_id, tt.count
order by count desc
limit 100
;

