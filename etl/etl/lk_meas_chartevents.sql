-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Populate lookup tables for cdm_measurement table
-- Rule 2
-- Labs from chartevents
-- 
-- Dependencies: run after 
--      st_core.sql,
--      st_hosp.sql,
--      st_icu.sql,
--      lk_meas_labevents (for unit mapping)
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- to add more measurements from chartevents (see table d_items and analysis for custom mapping)
-- 
-- custom mapping:
--      brand new vocabulary -> mimiciv_meas_chartevents_value -- mapped partially
--          src_chartevents.value = measurement.value_source_value -> value_as_concept_id
-- 
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Rule 2
-- chartevents
-- chartevents keeps all possible records about events during an ICU stay
-- it repeats labs, but in case of discrepancies labs is the final truth
-- both HR and RR obviously do not repeat labs
-- Saturation probably repeats labs. Compare to Lab with source_value = "Oxygen Saturation | 50817"
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_chartevents_clean AS
SELECT
    src.subject_id                  AS subject_id,
    src.hadm_id                     AS hadm_id,
    src.stay_id                     AS stay_id,
    -- src.itemid                      AS itemid,
    di.label                        AS source_code,
    src.charttime                   AS start_datetime,
    src.value                       AS value,
    src.valuenum                    AS valuenum,
    src.valueuom                    AS valueuom, -- unit of measurement
    --
    'chartevents'           AS unit_id,
    src.load_table_id       AS load_table_id,
    src.load_row_id         AS load_row_id,
    src.trace_id            AS trace_id
FROM
    `@etl_project`.@etl_dataset.src_chartevents src -- ce
INNER JOIN
    `@etl_project`.@etl_dataset.src_d_items di
        ON  src.itemid = di.itemid
WHERE
    di.label IN (
        'Heart Rate',
        'Respiratory Rate',
        'O2 saturation pulseoxymetry',
        'Heart Rhythm'
    )

;

-- -- -------------------------------------------------------------------
-- -- lk_meas_d_labitems_concept
-- -- brand new custom vocabulary -> mimiciv_me_chart_items
-- -- open points: Add 'Maps to value' when necessary
-- -- mapping rule: 
-- -- performance: process 1.4GB vs 3.9GB with distinct ce.itemid
-- -- to uncomment if we add more di.label
-- -- -------------------------------------------------------------------

-- CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_meas_d_items_concept AS
-- SELECT
--     di.itemid               AS itemid, -- PK
--     di.label                AS source_code,
--     vc.domain_id            AS source_domain_id,
--     vc.concept_id           AS source_concept_id,
--     vc2.domain_id           AS target_domain_id,
--     vc2.concept_id          AS target_concept_id,
--     di.category             AS category, -- for type_concept_id
--     di.param_type           AS param_type -- ?
-- FROM
--     `@etl_project`.@etl_dataset.src_d_items di
-- LEFT JOIN
--     `@etl_project`.@etl_dataset.voc_concept vc
--         ON  vc.concept_code = di.label
--         AND vc.vocabulary_id = 'mimiciv_me_chart_items' -- gcpt_labs_from_chartevents_to_concept
-- LEFT JOIN
--     `@etl_project`.@etl_dataset.voc_concept_relationship vcr
--         ON  vc.concept_id = vcr.concept_id_1
--         AND vcr.relationship_id = 'Maps to'
-- LEFT JOIN
--     `@etl_project`.@etl_dataset.voc_concept vc2
--         ON vc2.concept_id = vcr.concept_id_2
--         AND vc2.standard_concept = 'S'
--         AND vc2.invalid_reason IS NULL
-- WHERE
--     di.linksto = 'chartevents'
-- ;

-- -------------------------------------------------------------------
-- lk_chartevents_mapped
-- to map src_chartevents.value = measurement.value_source_value -> value_as_concept_id
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_chartevents_mapped AS
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())           AS measurement_id,
    src.subject_id                              AS subject_id,
    src.hadm_id                                 AS hadm_id,
    src.stay_id                                 AS stay_id,
    src.start_datetime                          AS start_datetime,
    src.source_code                             AS source_code,
    CASE
        WHEN src.source_code = 'Heart Rate'         THEN 4239408 -- Heart rate  Observable Entity   SNOMED
        WHEN src.source_code = 'Respiratory Rate'   THEN 4313591 -- Respiratory rate  Observable Entity   SNOMED
        WHEN src.source_code = 'O2 saturation pulseoxymetry' THEN 4020553 
                                                            -- Oxygen saturation measurement  Procedure   SNOMED
        WHEN src.source_code = 'Heart Rhythm'       THEN 4301868 -- Pulse rate    Observable Entity   SNOMED
        ELSE 0
    END                                         AS target_concept_id,
    0                                           AS source_concept_id,
    'Measurement'                               AS target_domain_id,
    IF(src.valuenum IS NULL, src.value, NULL)   AS value_source_value,
    src.valuenum                                AS value_as_number,
    src.valueuom                                AS unit_source_value, -- unit of measurement
    IF(src.valueuom IS NOT NULL, 
        COALESCE(uc.target_concept_id, 0), NULL)    AS unit_concept_id,
    --
    src.unit_id             AS unit_id,
    src.load_table_id       AS load_table_id,
    src.load_row_id         AS load_row_id,
    src.trace_id            AS trace_id
FROM
    `@etl_project`.@etl_dataset.lk_chartevents_clean src -- ce
LEFT JOIN 
    `@etl_project`.@etl_dataset.lk_meas_unit_concept uc
        ON uc.source_code = src.valueuom
;

