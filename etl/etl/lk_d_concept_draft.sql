-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Map internal codes to vocabularies
-- 
-- Dependencies: run after 
--      st_core.sql,
--      st_hosp.sql,
--      st_icu.sql
--
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
-- -------------------------------------------------------------------







-- -------------------------------------------------------------------
-- mapping di
--
-- drug 
--      gcpt_inputevents_drug_to_concept USING (itemid) - drug_concept_id, 2nd priority 
--          after rxnorm_map using(d_items.label)
--              created from gcpt_gdata_drug_exposure
--      gcpt_mv_input_label_to_concept USING (itemid) - drug_concept_id, 3rd priority
--      d_items.label = drug_source_value - drug rule 2, inputevents
--          gcpt_inputevents_drug_to_concept.itemid
-- meas
--      vocabulary_id = 'MIMIC d_items' - meas.chartevents
--          d_items.label = measurement_source_value
--          gcpt_lab_label_to_concept USING (label) - measurement_concept_id
--          gcpt_labs_from_chartevents_to_concept USING (category, label) - measurement_type_concept_id
--      category IN ( 'SPECIMEN', 'ORGANISM'), di.item_id = spec_itemid(?)
--      ab_item_id = di.itemid
--      di.category - much parsing for meas.chart.4
--      di.label = measurement_source_value, meas.outputevents
-- obs
--      obs.chart, concept_code = itemid
-- proc
--      itemid -> gcpt_procedure_to_concept -> mimiciv_proc_itemid
--          concept_code = itemid
--      d_items.label -> gcpt_datetimeevents_to_concept -> mimiciv_proc_datetimeevents
--           vc.concept_code = CAST(d_items.label AS STRING)
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_d_items_concept AS
SELECT
    di.itemid               AS itemid,      -- we can use itemid or source_code, but not necessary both
    di.label                AS source_code,
    di.linksto              AS linksto,
    di.category             AS category,
    di.lownormalvalue       AS lownormalvalue,
    di.highnormalvalue      AS highnormalvalue,
    vc.vocabulary_id        AS source_vocabulary_id,
    vc.domain_id            AS source_domain_id,
    vc.concept_id           AS source_concept_id,
    vc2.domain_id           AS target_domain_id,
    vc2.concept_id          AS target_concept_id
FROM
    `@etl_project`.@etl_dataset.src_d_items di
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept vc
        ON vc.concept_code = CAST(di.label AS STRING)
        AND vc.vocabulary_id like 'mimiciv_%'
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept_relationship vcr
        ON  vc.concept_id = vcr.concept_id_1
        AND vcr.relationship_id = 'Maps to'
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept vc2
        ON vc2.concept_id = vcr.concept_id_2
        AND vc2.standard_concept = 'S'
        AND vc2.invalid_reason IS NULL
;


-- -------------------------------------------------------------------
-- mapping d_labitems
-- 
-- d_labitems.. -> gcpt_...

-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_d_labitems_concept AS
SELECT
    di.itemid          AS itemid,
    di.label           AS source_code,
    di.linksto         AS linksto,
    vc.vocabulary_id        AS source_vocabulary_id,
    vc.domain_id            AS source_domain_id,
    vc.concept_id           AS source_concept_id,
    vc2.domain_id           AS target_domain_id,
    vc2.concept_id          AS target_concept_id
FROM
    `@etl_project`.@etl_dataset.src_d_labitems di
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept vc
        ON vc.concept_code = CAST(di.label AS STRING)
        AND vc.vocabulary_id like 'mimiciv_%'
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept_relationship vcr
        ON  vc.concept_id = vcr.concept_id_1
        AND vcr.relationship_id = 'Maps to'
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept vc2
        ON vc2.concept_id = vcr.concept_id_2
        AND vc2.standard_concept = 'S'
        AND vc2.invalid_reason IS NULL
;


-- -------------------------------------------------------------------
-- mapping d_micro
-- 
-- d_micro.. -> gcpt_...

-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_d_micro_concept AS
SELECT
    di.itemid          AS itemid,
    di.label           AS source_code,
    di.linksto         AS linksto,
    vc.vocabulary_id        AS source_vocabulary_id,
    vc.domain_id            AS source_domain_id,
    vc.concept_id           AS source_concept_id,
    vc2.domain_id           AS target_domain_id,
    vc2.concept_id          AS target_concept_id
FROM
    `@etl_project`.@etl_dataset.src_d_micro di
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept vc
        ON vc.concept_code = CAST(di.label AS STRING)
        AND vc.vocabulary_id like 'mimiciv_%'
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept_relationship vcr
        ON  vc.concept_id = vcr.concept_id_1
        AND vcr.relationship_id = 'Maps to'
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept vc2
        ON vc2.concept_id = vcr.concept_id_2
        AND vc2.standard_concept = 'S'
        AND vc2.invalid_reason IS NULL
;
