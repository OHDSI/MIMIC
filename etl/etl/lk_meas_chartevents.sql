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
--      st_icu.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Rule 2
-- labs from chartevents
-- chartevents keeps all possible records about events during an ICU stay
-- it repeats labs, but in case of discrepancies labs is the final truth
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.lk_chartevents_clean AS
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())       AS measurement_id,
    src.subject_id                  AS subject_id,
    src.hadm_id                     AS hadm_id,
    src.itemid                      AS itemid,
    src.storetime                   AS result_datetime, -- according to Alistair, storetime is the result time,
    src.charttime                   AS start_datetime,    -- according to Alistair, charttime is the specimen time,
    src.value                       AS value,
    src.valuenum                    AS valuenum,
    src.valueuom                    AS valueuom, -- unit of measurement
    --
    'chartevents'           AS unit_id,
    src.load_table_id       AS load_table_id,
    src.load_row_id         AS load_row_id,
    src.trace_id            AS trace_id
FROM
    `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.src_chartevents src -- ce
;

-- -------------------------------------------------------------------
-- lk_meas_d_labitems_concept
-- gcpt_labs_from_chartevents_to_concept -> mimiciv_me_chart
-- open points: Add 'Maps to value'
-- mapping rule: 
-- performance: process 1.4GB vs 3.9GB with distinct ce.itemid
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.lk_meas_d_items_concept AS
SELECT
    di.itemid               AS itemid, -- PK
    di.label                AS source_code,
    vc.domain_id            AS source_domain_id,
    vc.concept_id           AS source_concept_id,
    vc2.domain_id           AS target_domain_id,
    vc2.concept_id          AS target_concept_id,
    di.category             AS category, -- for type_concept_id
    di.param_type           AS param_type -- ?
FROM
    `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.src_d_items di
LEFT JOIN
    `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.voc_concept vc
        ON  vc.concept_code = di.label
        AND vc.vocabulary_id = 'mimiciv_me_chart_items' -- gcpt_labs_from_chartevents_to_concept
LEFT JOIN
    `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.voc_concept_relationship vcr
        ON  vc.concept_id = vcr.concept_id_1
        AND vcr.relationship_id = 'Maps to'
LEFT JOIN
    `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.voc_concept vc2
        ON vc2.concept_id = vcr.concept_id_2
        AND vc2.standard_concept = 'S'
        AND vc2.invalid_reason IS NULL
WHERE
    di.linksto = 'chartevents'
;




    d_items.itemid,
    d_items.category,
    d_items.label,
    d_items.mimic_id
src_admissions (mimic_id, hadm_id)
tmp_operator_concept
create "mimic_d_items_to_concept" similar to tmp_lab_loinc_concept
tmp_meas_unit_concept
    gcpt_labs_from_chartevents_to_concept.label,
    gcpt_labs_from_chartevents_to_concept.category,
    gcpt_labs_from_chartevents_to_concept.measurement_type_concept_id

"row_to_insert" AS (
SELECT
  chartevents_lab.measurement_id,
    src_patients.person_id,
    coalesce(omop_loinc.measurement_concept_id,
    gcpt_lab_label_to_concept.measurement_concept_id,
    0) as measurement_concept_id,
    chartevents_lab.measurement_datetime::date AS measurement_date,
    chartevents_lab.measurement_datetime AS measurement_datetime,
    CASE
     WHEN category ILIKE 'blood gases'  THEN  2000000010
     WHEN lower(category) IN ('chemistry','enzymes')  THEN  2000000011
     WHEN lower(category) IN ('hematology','heme/coag','csf','coags') THEN  2000000009
     WHEN lower(category) IN ('labs') THEN coalesce(gcpt_labs_from_chartevents_to_concept.measurement_type_concept_id,44818702)
     ELSE 44818702 -- there no trivial way to classify
  END AS measurement_type_concept_id -- Lab result,
    operator_concept_id AS operator_concept_id -- = operator,
    chartevents_lab.value_as_number AS value_as_number,
    null::bigint AS value_as_concept_id,
    gcpt_lab_unit_to_concept.unit_concept_id,
    null::double precision AS range_low,
    null::double precision AS range_high,
    null::bigint AS provider_id,
    src_admissions.visit_occurrence_id AS visit_occurrence_id,
    null::bigint As visit_detail_id,
    d_items.label AS measurement_source_value,
    d_items.mimic_id AS measurement_source_concept_id,
    gcpt_lab_unit_to_concept.unit_source_value,
    chartevents_lab.value_source_value,
    specimen_datetime
 
FROM
    chartevents_lab
LEFT JOIN src_patients USING (subject_id)
LEFT JOIN src_admissions USING (hadm_id)
LEFT JOIN d_items USING (itemid)
LEFT JOIN omop_loinc USING (label)
LEFT JOIN omop_operator USING (operator_name)
LEFT JOIN gcpt_lab_label_to_concept USING (label)
LEFT JOIN gcpt_labs_from_chartevents_to_concept USING (category,
    label)
LEFT JOIN gcpt_lab_unit_to_concept USING (unit_source_value)
),
