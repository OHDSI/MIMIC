-- ----------------------------------------------------------------------------------------------
-- Alternative statistics for CDM tables
--
-- Number of  persons
-- Total
-- Race/Ethnicity Breakdown
-- Number of visits
-- Total
-- Stratified by visit_concept_id
-- Top 100 Mapped and Unmapped values on event tables
-- Total count on event tables
-- ----------------------------------------------------------------------------------------------

-- ----------------------------------------------------------------------------------------------
-- assuming that databases are:
-- cdm:             mimic_db
-- vocabularies:    mimic_vocabulary
-- metrics output:  mimic_metrics
--
-- create database mimic_metrics;
-- ----------------------------------------------------------------------------------------------

-- BQ queries to be converted to postrgers

-- ----------------------------------------------------------------------------------------------
-- totals
-- ----------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE mimic_metrics.me_total
(
    table_name        STRING,
    count             INT64
);

INSERT INTO mimic_metrics.me_total
SELECT
    'care_site'     AS table_name,
    COUNT(*)             AS count
from mimic_db.care_site ev
;

-- ----------------------------------------------------------------------------------------------
-- mapping rate
-- ----------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE mimic_metrics.me_mapping_rate
(
    table_name        STRING,
    concept_field     STRING,
    count             INT64,
    percent           FLOAT64,
    total             INT64
);


INSERT INTO mimic_metrics.me_mapping_rate
SELECT
    'care_site'        AS table_name,
    'place_of_service_concept_id'     AS concept_field,
    COUNT(*)                AS count,
    ROUND(COUNT(*) / tt.count * 100, 2)    AS percent,
    tt.count                AS total
FROM mimic_db.care_site ev
INNER JOIN mimic_metrics.me_total tt
    ON tt.table_name = 'care_site'
WHERE ev.place_of_service_concept_id <> 0
GROUP BY tt.count
;

-- ----------------------------------------------------------------------------------------------
-- top 100 
-- mapped and unmapped
-- ----------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE mimic_metrics.me_top_care_site
(
    concept_field     STRING,
    category          STRING,
    source_value      STRING,
    concept_id        INT64,
    concept_name      STRING,
    count             INT64,
    percent           FLOAT64
);

INSERT INTO mimic_metrics.me_top_care_site
SELECT
    'place_of_service_concept_id'     AS concept_field,
    'Mapped'              AS category,
    CAST(ev.place_of_service_source_value AS STRING)  AS source_value,
    vc.concept_id           AS concept_id,
    vc.concept_name         AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from mimic_db.care_site ev
inner join mimic_db.concept vc
    on ev.place_of_service_concept_id = vc.concept_id
inner join mimic_metrics.me_total tt
    on tt.table_name = 'care_site'
where ev.place_of_service_concept_id <> 0
group by ev.place_of_service_source_value, vc.concept_id, vc.concept_name, tt.count
order by count desc
limit 100
;

INSERT INTO mimic_metrics.me_top_care_site
SELECT
    'place_of_service_concept_id'      AS concept_field,
    'Unmapped'            AS category,
    CAST(ev.place_of_service_source_value AS STRING)  AS source_value,
    NULL                    AS concept_id,
    CAST(NULL AS STRING)    AS concept_name,
    COUNT(*)                AS count,
    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent
from mimic_db.care_site ev
inner join mimic_metrics.me_total tt
    on tt.table_name = 'care_site'
where ev.place_of_service_concept_id = 0
group by ev.place_of_service_source_value, tt.count
order by count desc
limit 100
;


