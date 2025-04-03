-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Populate cdm_dose_era table
-- "standard" script
-- -------------------------------------------------------------------

--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_dose_era
(
    dose_era_id           INT64     not null ,
    person_id             INT64     not null ,
    drug_concept_id       INT64     not null ,
    unit_concept_id       INT64     not null ,
    dose_value            FLOAT64   not null ,
    dose_era_start_date   DATE      not null ,
    dose_era_end_date     DATE      not null ,
    -- 
    unit_id                       STRING,
    load_table_id                 STRING,
    load_row_id                   INT64
)
;

-- -------------------------------------------------------------------
-- Create Temporary Table: tmp_drugIngredientExp
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- collect Drug Exposures
-- -------------------------------------------------------------------
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.tmp_drugIngredientExp AS
SELECT
    de.drug_exposure_id                     AS drug_exposure_id,
    de.person_id                            AS person_id,
    de.drug_exposure_start_date             AS drug_exposure_start_date,
    de.drug_exposure_end_date               AS drug_exposure_end_date,
    de.drug_concept_id                      AS drug_concept_id,
    ds.ingredient_concept_id                AS ingredient_concept_id,
    de.refills                              AS refills,
    CASE
        WHEN DE.days_supply = 0 THEN 1
        ELSE DE.days_supply
    END                                     AS days_supply,
    de.quantity                             AS quantity,
    ds.box_size                             AS box_size,
    ds.amount_value                         AS amount_value,
    ds.amount_unit_concept_id               AS amount_unit_concept_id,
    ds.numerator_value                      AS numerator_value,
    ds.numerator_unit_concept_id            AS numerator_unit_concept_id,
    ds.denominator_value                    AS denominator_value,
    ds.denominator_unit_concept_id          AS denominator_unit_concept_id,
    c.concept_class_id                      AS concept_class_id
FROM @etl_project.@etl_dataset.cdm_drug_exposure de
INNER JOIN @etl_project.@etl_dataset.voc_drug_strength ds
    ON de.drug_concept_id = ds.drug_concept_id
INNER JOIN @etl_project.@etl_dataset.voc_concept_ancestor ca
    ON  de.drug_concept_id = ca.descendant_concept_id
    AND ds.ingredient_concept_id = ca.ancestor_concept_id
LEFT JOIN @etl_project.@etl_dataset.voc_concept c
    ON  de.drug_concept_id = concept_id
    AND c.vocabulary_id IN ('RxNorm', 'RxNorm Extension')
;

-- -------------------------------------------------------------------
-- Create Temporary Table: tmp_drugWithDose
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- TODO
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.tmp_drugWithDose AS
SELECT
    drug_exposure_id                        AS drug_exposure_id,
    person_id                               AS person_id,
    drug_exposure_start_date                AS drug_exposure_start_date,
    drug_exposure_end_date                  AS drug_exposure_end_date,
    ingredient_concept_id                   AS drug_concept_id,
    refills                                 AS refills,
    days_supply                             AS days_supply,
    quantity                                AS quantity,
    -- CASE 1
    CASE
        WHEN amount_value IS NOT NULL
            AND denominator_unit_concept_id IS NULL
        THEN
            CASE
                WHEN quantity > 0
                    AND box_size IS NOT NULL
                    AND concept_class_id IN ('Branded Drug Box', 'Clinical Drug Box', 'Marketed Product',
                                            'Quant Branded Box', 'Quant Clinical Box')
                THEN amount_value * quantity * box_size / days_supply
                WHEN quantity > 0
                    AND concept_class_id NOT IN ('Branded Drug Box', 'Clinical Drug Box', 'Marketed Product',
                                                'Quant Branded Box', 'Quant Clinical Box')
                THEN amount_value * quantity / days_supply
                WHEN quantity = 0 AND box_size IS NOT NULL
                THEN amount_value * box_size / days_supply
                WHEN quantity = 0 AND box_size IS NULL
                THEN -1
            END
        -- CASE 2, 3
        WHEN numerator_value IS NOT NULL
            AND concept_class_id != 'Ingredient'
            AND denominator_unit_concept_id != 8505     --hour
        THEN
            CASE
                WHEN denominator_value IS NOT NULL
                THEN numerator_value / days_supply
                WHEN denominator_value IS NULL AND quantity != 0
                THEN numerator_value * quantity / days_supply
                WHEN denominator_value IS NULL AND quantity = 0
                THEN -1
            END
        -- CASE 4
        WHEN numerator_value IS NOT NULL
            AND concept_class_id = 'Ingredient'
            AND denominator_unit_concept_id != 8505
        THEN
            CASE
                WHEN quantity > 0
                THEN quantity / days_supply
                WHEN quantity = 0
                THEN -1
            END
        -- CASE 6
        WHEN numerator_value IS NOT NULL
            AND denominator_unit_concept_id = 8505
        THEN
            CASE
                WHEN denominator_value IS NOT NULL
                THEN numerator_value * 24 / denominator_value
                WHEN denominator_value IS NULL AND quantity != 0
                THEN numerator_value * 24 / quantity
                WHEN denominator_value IS NULL AND quantity = 0
                THEN -1
            END
    END                                     AS dose_value,
    -- CASE 1
    CASE
        WHEN amount_value IS NOT NULL
            AND denominator_unit_concept_id IS NULL
        THEN
            CASE
                WHEN quantity = 0 AND box_size IS NULL
                THEN -1
                ELSE amount_unit_concept_id
            END
        -- CASE 2, 3
        WHEN numerator_value IS NOT NULL
            AND concept_class_id != 'Ingredient'
            AND denominator_unit_concept_id != 8505     --hour
        THEN
            CASE
                WHEN denominator_value IS NULL AND quantity = 0
                THEN -1
                ELSE numerator_unit_concept_id
            END
        -- CASE 4
        WHEN numerator_value IS NOT NULL
            AND concept_class_id = 'Ingredient'
            AND denominator_unit_concept_id != 8505
        THEN
            CASE
                WHEN quantity > 0
                THEN 0
                WHEN quantity = 0
                THEN -1
            END
        -- CASE 6
        WHEN numerator_value IS NOT NULL
            AND denominator_unit_concept_id = 8505
        THEN
            CASE
            WHEN denominator_value IS NULL AND quantity = 0
            THEN -1
            ELSE numerator_unit_concept_id
        END
    END                                     AS unit_concept_id
FROM @etl_project.@etl_dataset.tmp_drugIngredientExp
;

-- -------------------------------------------------------------------
-- Create Temporary Table: tmp_cteDoseTarget
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- TODO
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.tmp_cteDoseTarget AS
SELECT
    dwd.drug_exposure_id                                                AS drug_exposure_id,
    dwd.person_id                                                       AS person_id,
    dwd.drug_concept_id                                                 AS drug_concept_id,
    dwd.unit_concept_id                                                 AS unit_concept_id,
    dwd.dose_value                                                      AS dose_value,
    dwd.drug_exposure_start_date                                        AS drug_exposure_start_date,
    dwd.days_supply                                                     AS days_supply,
    COALESCE(drug_exposure_end_date,
        -- If drug_exposure_end_date != NULL,
        -- return drug_exposure_end_date, otherwise go to next case
        NULLIF(DATE_ADD(drug_exposure_start_date, INTERVAL (1 * days_supply * (COALESCE(refills, 0) + 1)) DAY), drug_exposure_start_date),
        --If days_supply != NULL or 0, return drug_exposure_start_date + days_supply,
        -- otherwise go to next case
        DATE_ADD(drug_exposure_start_date, INTERVAL 1 DAY))       AS drug_exposure_end_date
        -- Add 1 day to the drug_exposure_start_date since
        -- there is no end_date or INTERVAL for the days_supply
FROM @etl_project.@etl_dataset.tmp_drugWithDose dwd
WHERE
    dose_value <> -1
;

-- -------------------------------------------------------------------
-- Create Temporary Table: tmp_cteDoseEndDates_rawdata
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- unfold to start/end events
-- NOTE - record count should double exactly
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.tmp_cteDoseEndDates_rawdata AS
SELECT
    person_id                                       AS person_id,
    drug_concept_id                                 AS drug_concept_id,
    unit_concept_id                                 AS unit_concept_id,
    dose_value                                      AS dose_value,
    drug_exposure_start_date                        AS event_date,
    -1                                              AS event_type,
    ROW_NUMBER() OVER (
        PARTITION BY person_id, drug_concept_id, unit_concept_id, CAST(dose_value AS INT64)
        ORDER BY drug_exposure_start_date)          AS start_ordinal
FROM @etl_project.@etl_dataset.tmp_cteDoseTarget
UNION ALL
-- pad the end dates by 30 to allow a grace period for overlapping ranges.
SELECT
    person_id                                                   AS person_id,
    drug_concept_id                                             AS drug_concept_id,
    unit_concept_id                                             AS unit_concept_id,
    dose_value                                                  AS dose_value,
    DATE_ADD(drug_exposure_end_date, INTERVAL 30 DAY)           AS event_date,
    1                                                           AS event_type,
    NULL                                                        AS start_ordinal
FROM @etl_project.@etl_dataset.tmp_cteDoseTarget
;

-- -------------------------------------------------------------------
-- Create Temporary Table: tmp_cteDoseEndDates_e
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- TODO
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.tmp_cteDoseEndDates_e AS
SELECT
    person_id                                                                   AS person_id,
    drug_concept_id                                                             AS drug_concept_id,
    unit_concept_id                                                             AS unit_concept_id,
    dose_value                                                                  AS dose_value,
    event_date                                                                  AS event_date,
    event_type                                                                  AS event_type,
    MAX(start_ordinal) OVER (
        PARTITION BY person_id, drug_concept_id, unit_concept_id, CAST(dose_value AS INT64) -- double-check if it is a valid cast
        ORDER BY event_date, event_type ROWS unbounded preceding)               AS start_ordinal,
    ROW_NUMBER() OVER (
        PARTITION BY person_id, drug_concept_id, unit_concept_id, CAST(dose_value AS INT64)
        ORDER BY event_date, event_type)                                        AS overall_ord
        -- order by above pulls the current START down from the prior
        -- rows so that the NULLs from the END DATES will contain a value we can compare with
FROM @etl_project.@etl_dataset.tmp_cteDoseEndDates_rawdata
;

-- -------------------------------------------------------------------
-- Create Temporary Table: tmp_cteDoseEndDates
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- TODO
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.tmp_cteDoseEndDates AS
SELECT
    person_id                                       AS person_id,
    drug_concept_id                                 AS drug_concept_id,
    unit_concept_id                                 AS unit_concept_id,
    dose_value                                      AS dose_value,
    DATE_SUB(event_date, INTERVAL 30 DAY)           AS end_date   -- unpad the end date
FROM @etl_project.@etl_dataset.tmp_cteDoseEndDates_e
WHERE
    (2 * start_ordinal) - overall_ord = 0
;

-- -------------------------------------------------------------------
-- Create Temporary Table: tmp_cteDoseFinalEnds
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- TODO
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.tmp_cteDoseFinalEnds AS
SELECT
    dt.person_id                    AS person_id,
    dt.drug_concept_id              AS drug_concept_id,
    dt.unit_concept_id              AS unit_concept_id,
    dt.dose_value                   AS dose_value,
    dt.drug_exposure_start_date     AS drug_exposure_start_date,
    MIN(e.end_date)                 AS drug_era_end_date
FROM @etl_project.@etl_dataset.tmp_cteDoseTarget dt
INNER JOIN @etl_project.@etl_dataset.tmp_cteDoseEndDates e
    ON  dt.person_id = e.person_id
    AND dt.drug_concept_id = e.drug_concept_id
    AND dt.unit_concept_id = e.unit_concept_id
    AND dt.drug_concept_id = e.drug_concept_id
    AND dt.dose_value = e.dose_value
    AND e.end_date >= dt.drug_exposure_start_date
GROUP BY
    dt.drug_exposure_id,
    dt.person_id,
    dt.drug_concept_id,
    dt.drug_exposure_start_date,
    dt.unit_concept_id,
    dt.dose_value
;

-- -------------------------------------------------------------------
-- Load Table: cdm_dose_era
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- final Dose Eras
-- -------------------------------------------------------------------

INSERT INTO @etl_project.@etl_dataset.cdm_dose_era
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())   AS dose_era_id,
    person_id                           AS person_id,
    drug_concept_id                     AS drug_concept_id,
    unit_concept_id                     AS unit_concept_id,
    dose_value                          AS dose_value,
    MIN(drug_exposure_start_date)       AS dose_era_start_date,
    drug_era_end_date                   AS dose_era_end_date,
    'dose_era.drug_exposure'            AS unit_id,
    CAST(NULL AS STRING)                AS load_table_id,
    CAST(NULL AS INT64)                 AS load_row_id
FROM @etl_project.@etl_dataset.tmp_cteDoseFinalEnds
GROUP BY
    person_id,
    drug_concept_id,
    unit_concept_id,
    dose_value,
    drug_era_end_date
ORDER BY
    person_id,
    drug_concept_id
;

-- -------------------------------------------------------------------
-- Drop Temporary Tables
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS @etl_project.@etl_dataset.tmp_drugIngredientExp;
DROP TABLE IF EXISTS @etl_project.@etl_dataset.tmp_drugWithDose;
DROP TABLE IF EXISTS @etl_project.@etl_dataset.tmp_cteDoseTarget;
DROP TABLE IF EXISTS @etl_project.@etl_dataset.tmp_cteDoseEndDates_rawdata;
DROP TABLE IF EXISTS @etl_project.@etl_dataset.tmp_cteDoseEndDates_e;
DROP TABLE IF EXISTS @etl_project.@etl_dataset.tmp_cteDoseEndDates;
DROP TABLE IF EXISTS @etl_project.@etl_dataset.tmp_cteDoseFinalEnds;
