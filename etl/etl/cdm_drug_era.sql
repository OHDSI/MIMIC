-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Populate cdm_drug_era table
-- "standard" script
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.lk_join_voc_drug
AS SELECT DISTINCT
    ca.descendant_concept_id    AS descendant_concept_id,
    ca.ancestor_concept_id      AS ancestor_concept_id,
    c.concept_id                AS concept_id
FROM
    @etl_project.@etl_dataset.voc_concept_ancestor ca
JOIN
    @etl_project.@etl_dataset.voc_concept c
        ON  ca.ancestor_concept_id = c.concept_id
        AND c.vocabulary_id        IN ('RxNorm', 'RxNorm Extension')    -- selects RxNorm, RxNorm Extension vocabulary_id
        AND c.concept_class_id     = 'Ingredient'                       -- selects the Ingredients only.
                                                                        -- There are other concept_classes in RxNorm that
                                                                            -- we are not interested in.
;

-- -------------------------------------------------------------------
-- Defining spans of time when the Person
-- is assumed to be exposed to a particular
-- active ingredient.
-- The drug_concept_id field only contains
-- Concepts that have the concept_class 'Ingredient'
-- -------------------------------------------------------------------
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.tmp_pretarget_drug
AS SELECT
    d.drug_exposure_id          AS drug_exposure_id,
    d.person_id                 AS person_id,
    v.concept_id                AS ingredient_concept_id,
    d.drug_exposure_start_date  AS drug_exposure_start_date,
    d.days_supply               AS days_supply,
    d.drug_exposure_end_date    AS drug_exposure_end_date
FROM
    @etl_project.@etl_dataset.cdm_drug_exposure d
JOIN
    @etl_project.@etl_dataset.lk_join_voc_drug v
        ON v.descendant_concept_id = d.drug_concept_id
WHERE
    d.drug_concept_id != 0
;

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.tmp_subenddates_un_drug
    AS SELECT
        person_id                           AS person_id,
        ingredient_concept_id               AS ingredient_concept_id,
        drug_exposure_start_date            AS event_date,
        -1                                  AS event_type,
        ROW_NUMBER() OVER (
            PARTITION BY
                person_id,
                ingredient_concept_id
            ORDER BY
                drug_exposure_start_date)   AS start_ordinal
    FROM
        @etl_project.@etl_dataset.tmp_pretarget_drug
UNION ALL
    SELECT
        person_id                   AS person_id,
        ingredient_concept_id       AS ingredient_concept_id,
        drug_exposure_end_date      AS event_date,
        1                           AS event_type,
        NULL                        AS start_ordinal
    FROM
        @etl_project.@etl_dataset.tmp_pretarget_drug
;

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.tmp_subenddates_rows_drug
AS SELECT
    person_id                       AS person_id,
    ingredient_concept_id           AS ingredient_concept_id,
    event_date                      AS event_date,
    event_type                      AS event_type,
    MAX(start_ordinal) OVER (
        PARTITION BY
            person_id,
            ingredient_concept_id
        ORDER BY
            event_date,
            event_type
        ROWS UNBOUNDED PRECEDING)   AS start_ordinal,
            -- this pulls the current START down from the prior rows so that the NULLs
            -- from the END DATES will contain a value we can compare with
    ROW_NUMBER() OVER (
        PARTITION BY
            person_id,
            ingredient_concept_id
        ORDER BY
            event_date,
            event_type)             AS overall_ord
            -- this re-numbers the inner UNION so all rows are numbered ordered by the event date
FROM
    @etl_project.@etl_dataset.tmp_subenddates_un_drug
;

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.tmp_subenddates_drug
AS SELECT
    person_id               AS person_id,
    ingredient_concept_id   AS ingredient_concept_id,
    event_date              AS end_date
FROM
    @etl_project.@etl_dataset.tmp_subenddates_rows_drug e
WHERE
    (2 * e.start_ordinal) - e.overall_ord = 0
;

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.temp_ends_drug
AS SELECT
    dt.person_id                    AS person_id,
    dt.ingredient_concept_id        AS drug_concept_id,
    dt.drug_exposure_start_date     AS drug_exposure_start_date,
    MIN(e.end_date)                 AS drug_sub_exposure_end_date
FROM
    @etl_project.@etl_dataset.tmp_pretarget_drug dt
JOIN
    @etl_project.@etl_dataset.tmp_subenddates_drug e
        ON  dt.person_id             = e.person_id
        AND dt.ingredient_concept_id = e.ingredient_concept_id
        AND e.end_date               >= dt.drug_exposure_start_date
GROUP BY
    dt.drug_exposure_id,
    dt.person_id,
    dt.ingredient_concept_id,
    dt.drug_exposure_start_date
;

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.tmp_sub_drug
AS SELECT
    ROW_NUMBER() OVER (
        PARTITION BY
            person_id,
            drug_concept_id,
            drug_sub_exposure_end_date
        ORDER BY
            person_id,
            drug_concept_id)        AS row_number,
    person_id                       AS person_id,
    drug_concept_id                 AS drug_concept_id,
    MIN(drug_exposure_start_date)   AS drug_sub_exposure_start_date,
    drug_sub_exposure_end_date      AS drug_sub_exposure_end_date,
    COUNT(*)                        AS drug_exposure_count
FROM
    @etl_project.@etl_dataset.temp_ends_drug
GROUP BY
    person_id,
    drug_concept_id,
    drug_sub_exposure_end_date
ORDER BY
    person_id,
    drug_concept_id
;

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.tmp_finaltarget_drug
AS SELECT
    row_number                              AS row_number,
    person_id                               AS person_id,
    drug_concept_id                         AS ingredient_concept_id,
    drug_sub_exposure_start_date            AS drug_sub_exposure_start_date,
    drug_sub_exposure_end_date              AS drug_sub_exposure_end_date,
    drug_exposure_count                     AS drug_exposure_count,
    DATE_DIFF( drug_sub_exposure_end_date,
              drug_sub_exposure_start_date, DAY) AS days_exposed
FROM
    @etl_project.@etl_dataset.tmp_sub_drug
;

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.tmp_enddates_un_drug
    AS SELECT
        person_id                                       AS person_id,
        ingredient_concept_id                           AS ingredient_concept_id,
        drug_sub_exposure_start_date                    AS event_date,
        -1                                              AS event_type,
        ROW_NUMBER() OVER (
            PARTITION BY
                person_id,
                ingredient_concept_id
            ORDER BY
                drug_sub_exposure_start_date)           AS start_ordinal
    FROM
        @etl_project.@etl_dataset.tmp_finaltarget_drug
UNION ALL
-- pad the end dates by 30 to allow a grace period for overlapping ranges.
    SELECT
        person_id                                                       AS person_id,
        ingredient_concept_id                                           AS ingredient_concept_id,
        DATE_ADD (drug_sub_exposure_end_date, INTERVAL 30 DAY)          AS event_date,
        1                                                               AS event_type,
        NULL                                                            AS start_ordinal
    FROM
        @etl_project.@etl_dataset.tmp_finaltarget_drug
;

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.tmp_enddates_rows_drug
AS SELECT
    person_id                       AS person_id,
    ingredient_concept_id           AS ingredient_concept_id,
    event_date                      AS event_date,
    event_type                      AS event_type,
    MAX(start_ordinal) OVER (
        PARTITION BY
            person_id,
            ingredient_concept_id
        ORDER BY
            event_date,
            event_type
        ROWS UNBOUNDED PRECEDING)   AS start_ordinal,
      -- this pulls the current START down from the prior rows so that the NULLs
      -- from the END DATES will contain a value we can compare with
    ROW_NUMBER() OVER (
        PARTITION BY
            person_id,
            ingredient_concept_id
        ORDER BY
            event_date,
            event_type)             AS overall_ord
      -- this re-numbers the inner UNION so all rows are numbered ordered by the event date
FROM
    @etl_project.@etl_dataset.tmp_enddates_un_drug
;

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.tmp_enddates_drug
AS SELECT
    person_id                                       AS person_id,
    ingredient_concept_id                           AS ingredient_concept_id,
    DATE_SUB (event_date, INTERVAL 30 DAY)          AS end_date  -- unpad the end date
FROM
    @etl_project.@etl_dataset.tmp_enddates_rows_drug e
WHERE
    (2 * e.start_ordinal) - e.overall_ord = 0
;

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.tmp_drugera_ends_drug
AS SELECT
    ft.person_id                        AS person_id,
    ft.ingredient_concept_id            AS ingredient_concept_id,
    ft.drug_sub_exposure_start_date     AS drug_sub_exposure_start_date,
    MIN(e.end_date)                     AS drug_era_end_date,
    ft.drug_exposure_count              AS drug_exposure_count,
    ft.days_exposed                     AS days_exposed
FROM
    @etl_project.@etl_dataset.tmp_finaltarget_drug ft
JOIN
    @etl_project.@etl_dataset.tmp_enddates_drug e
        ON ft.person_id              = e.person_id
        AND e.end_date               >= ft.drug_sub_exposure_start_date
        AND ft.ingredient_concept_id = e.ingredient_concept_id
GROUP BY
    ft.person_id,
    ft.days_exposed,
    ft.drug_exposure_count,
    ft.ingredient_concept_id,
    ft.drug_sub_exposure_start_date
;

-- -------------------------------------------------------------------
-- Load Table: Drug_era
-- -------------------------------------------------------------------

--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_drug_era
(
    drug_era_id         INT64     not null ,
    person_id           INT64     not null ,
    drug_concept_id     INT64     not null ,
    drug_era_start_date DATE      not null ,
    drug_era_end_date   DATE      not null ,
    drug_exposure_count INT64              ,
    gap_days            INT64              ,
    -- 
    unit_id                       STRING,
    load_table_id                 STRING,
    load_row_id                   INT64
)
;

-- -------------------------------------------------------------------
-- @summary: 30 days window is allowed
-- -------------------------------------------------------------------
INSERT INTO @etl_project.@etl_dataset.cdm_drug_era
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())                                   AS drug_era_id,
    person_id                                                           AS person_id,
    ingredient_concept_id                                               AS drug_concept_id,
    MIN (drug_sub_exposure_start_date)                                  AS drug_era_start_date,
    drug_era_end_date                                                   AS drug_era_end_date,
    SUM(drug_exposure_count)                                            AS drug_exposure_count,
    DATE_DIFF( drug_era_end_date,
              MIN(drug_sub_exposure_start_date), DAY) - SUM(days_exposed)    AS gap_days,
-- --
    'drug_era.drug_exposure'                                            AS unit_id,
    CAST(NULL AS STRING)                                                AS load_table_id,
    CAST(NULL AS INT64)                                                 AS load_row_id
FROM
    @etl_project.@etl_dataset.tmp_drugera_ends_drug
GROUP BY
    person_id,
    drug_era_end_date,
    ingredient_concept_id
ORDER BY
    person_id,
    ingredient_concept_id
;

-- -------------------------------------------------------------------
-- Drop temporary table
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS @etl_project.@etl_dataset.tmp_drugera_ends_drug;
DROP TABLE IF EXISTS @etl_project.@etl_dataset.tmp_enddates_drug;
DROP TABLE IF EXISTS @etl_project.@etl_dataset.tmp_finaltarget_drug;
DROP TABLE IF EXISTS @etl_project.@etl_dataset.tmp_enddates_un_drug;
DROP TABLE IF EXISTS @etl_project.@etl_dataset.tmp_sub_drug;
DROP TABLE IF EXISTS @etl_project.@etl_dataset.temp_ends_drug;
DROP TABLE IF EXISTS @etl_project.@etl_dataset.tmp_pretarget_drug;
DROP TABLE IF EXISTS @etl_project.@etl_dataset.tmp_subenddates_un_drug;
DROP TABLE IF EXISTS @etl_project.@etl_dataset.tmp_subenddates_rows_drug;
DROP TABLE IF EXISTS @etl_project.@etl_dataset.tmp_subenddates_drug;
DROP TABLE IF EXISTS @etl_project.@etl_dataset.tmp_enddates_rows_drug;
-- -------------------------------------------------------------------
-- Loading finished
-- -------------------------------------------------------------------
