-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Populate cdm_drug_exposure table
-- 
-- Dependencies: run after 
--      st_core.sql,
--      st_hosp.sql,
--      cdm_person.sql,
--      cdm_visit_occurrence,
--      cdm_visit_detail(?)
--
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize create or replace
--
-- loaded custom mapping:
--      gcpt_route_to_concept                       -> mimiciv_drug_route
--      gcpt_prescriptions_ndcisnullzero_to_concept -> mimiciv_drug_ndc
-- open points: 
--      create src_inputevents
--      mimiciv_drug_ndc.concept_class_id = 'Prescription Drug' - is it right?
-- -------------------------------------------------------------------


-- -------------------------------------------------------------------
-- lk_prescriptions_clean 
-- Rule 1
-- -------------------------------------------------------------------
CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_prescriptions_clean AS
SELECT
    -- 'drug:['                || COALESCE(drug, drug_name_poe, drug_name_generic,'') || ']'||
    'drug:['                || COALESCE(drug,'') || ']'||
    'prod_strength:['       || COALESCE(prod_strength,'') || ']'||
    'drug_type:['           || COALESCE(drug_type,'') || ']'||
    -- 'formulary_drug_cd:['   || COALESCE(formulary_drug_cd,'') || ']' ||
     'dose_unit_rx:['       || COALESCE(dose_unit_rx,'') || ']' 
                                                                        AS concept_name,
    src.subject_id              AS subject_id,
    src.hadm_id                 AS hadm_id,
    src.dose_val_rx             AS dose_val_rx,
    src.starttime               AS start_datetime,
    src.stoptime                AS end_datetime,
    src.route                   AS route_source_code, --TODO: add route AS local concept,
    'mimiciv_drug_route'        AS route_source_vocabulary,
    src.form_unit_disp          AS dose_unit_source_code, --TODO: add unit AS local concept,
    CAST(src.ndc AS STRING)     AS ndc_source_code, -- ndc was used for automatic/manual mapping,
    'NDC'                       AS ndc_source_vocabulary,
    src.form_val_disp           AS form_val_disp,
    REGEXP_EXTRACT(src.form_val_disp, r'[-]?[\d]+[.]?[\d]*')  AS quantity,
    COALESCE(
        -- src.drug, src.drug_name_poe, src.drug_name_generic,'')
        src.drug, '')
        || ' ' || COALESCE(src.prod_strength, '')               AS gcpt_source_code,
    'mimiciv_drug_ndc'                                          AS gcpt_source_vocabulary, -- source_code = label
    -- 
    'rule.1.prescriptions'          AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id

FROM
    `@etl_project`.@etl_dataset.src_prescriptions src -- pr
;

-- -------------------------------------------------------------------
-- lk_pr_ndc_concept
-- Rule 1
-- mapping is 85% done from gsn coding
-- -------------------------------------------------------------------
CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_pr_ndc_concept AS
SELECT DISTINCT
    src.ndc_source_code     AS source_code,
    vc.domain_id            AS source_domain_id,
    vc.concept_id           AS source_concept_id,
    vc2.domain_id           AS target_domain_id,
    vc2.concept_id          AS target_concept_id
FROM
    `@etl_project`.@etl_dataset.lk_prescriptions_clean src -- pr
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept vc
        ON  vc.concept_code = src.ndc_source_code --this covers 85% of direct mapping but no standard
        AND vc.vocabulary_id = src.ndc_source_vocabulary -- NDC
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept_relationship vcr
        ON  vc.concept_id = vcr.concept_id_1 
        and vcr.relationship_id = 'Maps to'
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept vc2
        ON vc2.concept_id = vcr.concept_id_2
        AND vc2.standard_concept = 'S'
        AND vc2.invalid_reason IS NULL --covers 71% of rxnorm standards concepts
;

-- -------------------------------------------------------------------
-- lk_pr_gcpt_concept
-- Rule 1
-- -------------------------------------------------------------------
CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_pr_gcpt_concept AS
SELECT DISTINCT
    src.gcpt_source_code    AS source_code,
    vc.domain_id            AS source_domain_id,
    vc.concept_id           AS source_concept_id,
    vc2.domain_id           AS target_domain_id,
    vc2.concept_id          AS target_concept_id
FROM
    `@etl_project`.@etl_dataset.lk_prescriptions_clean src -- pr
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept vc
        ON  vc.concept_code = src.gcpt_source_code
        AND vc.vocabulary_id = src.gcpt_source_vocabulary
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept_relationship vcr
        ON  vc.concept_id = vc.concept_id_1 
        and vcr.relationship_id = 'Maps to'
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept vc2
        ON vc2.concept_id = vcr.concept_id_2
        AND vc2.standard_concept = 'S'
        AND vc2.invalid_reason IS NULL --covers 71% of rxnorm standards concepts
;

-- -------------------------------------------------------------------
-- lk_pr_route_concept
-- Rule 1
-- -------------------------------------------------------------------
CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_pr_route_concept AS
SELECT DISTINCT
    src.route_source_code   AS source_code,
    vc.domain_id            AS source_domain_id,
    vc.concept_id           AS source_concept_id,
    vc2.domain_id           AS target_domain_id,
    vc2.concept_id          AS target_concept_id
FROM
    `@etl_project`.@etl_dataset.lk_prescriptions_clean src -- pr
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept vc
        ON  vc.concept_code = src.route_source_code
        AND vc.vocabulary_id = src.route_source_vocabulary
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept_relationship vcr
        ON  vc.concept_id = vc.concept_id_1 
        and vcr.relationship_id = 'Maps to'
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept vc2
        ON vc2.concept_id = vcr.concept_id_2
        AND vc2.standard_concept = 'S'
        AND vc2.invalid_reason IS NULL --covers 71% of rxnorm standards concepts
;

-- -- -------------------------------------------------------------------
-- -- tmp_omop_local_drug
-- -- "custom mapping" from prescription table
-- -- -------------------------------------------------------------------

-- CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.tmp_omop_local_drug AS
-- SELECT 
--     -- concept_name AS drug_source_value, 
--     -- concept_id AS drug_source_concept_id 
-- select
--     'drug:['|| coalesce(drug, drug_name_poe, drug_name_generic,'') ||']'||  
--     'prod_strength:['||coalesce(prod_strength,'')||']'|| 
--     'drug_type:['||coalesce(drug_type,'')||']'|| 
--     'formulary_drug_cd:['||coalesce(formulary_drug_cd,'') || ']' || 
--     'dose_unit_rx:[' || coalesce(dose_unit_rx,'') || ']'  
--                                                     AS concept_name --this will be joined to the drug_exposure table
--     , 'Drug_exposure'::text as domain_id
--     , 'MIMIC prescriptions' as vocabulary_id
--     , '' as concept_class_id

--     , 'gsn:['||coalesce(gsn,'')||']'|| 
--       'ndc:['||coalesce(ndc,'')||']'                AS concept_code
-- , drug_name_poe
-- , drug_name_generic
-- , drug
-- from prescriptions

-- FROM
--     `@etl_project`.@etl_dataset.voc_concept 
-- WHERE 
--     domain_id = 'prescriptions' AND vocabulary_id = 'MIMIC prescriptions'
-- ;

-- -------------------------------------------------------------------
-- lk_drug_mapped
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_drug_mapped AS
SELECT
    src.hadm_id                                     AS hadm_id,
    src.subject_id                                  AS subject_id,
    COALESCE(vc_ndc.target_concept_id, vc_gcpt.target_concept_id, 0)    AS target_concept_id,
    src.start_datetime                                                  AS start_datetime,
    src.end_datetime                                                    AS end_datetime,
    38000177                                        AS type_concept_id,
    src.quantity                                    AS quantity,
    COALESCE(vc_route.target_concept_id, 0)                             AS route_concept_id,
    COALESCE(vc_ndc.source_code, vc_gcpt.source_code, src.concept_name) AS source_code,
    COALESCE(vc_ndc.source_concept_id, vc_gcpt.source_concept_id, 0)    AS source_concept_id,
    src.route_source_code                                               AS route_source_code,
    src.dose_unit_source_code                       AS dose_unit_source_code,
    src.form_val_disp                               AS quantity_source_value,
    -- 
    src.unit_id                     AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    `@etl_project`.@etl_dataset.lk_prescriptions_clean src
LEFT JOIN
    `@etl_project`.@etl_dataset.lk_pr_ndc_concept vc_ndc
        ON  src.ndc_source_code = vc_ndc.source_code
        AND vc_ndc.target_concept_id IS NOT NULL -- temporary
LEFT JOIN
    `@etl_project`.@etl_dataset.lk_pr_gcpt_concept vc_gcpt
        ON  src.gcpt_source_code = vc_gcpt.source_code
        AND vc_gcpt.target_concept_id IS NOT NULL -- temporary
LEFT JOIN
    `@etl_project`.@etl_dataset.lk_pr_route_concept vc_route
        ON src.route_source_code = vc_route.source_code
        AND vc_route.target_concept_id IS NOT NULL -- temporary
;

-- -------------------------------------------------------------------
-- cdm_drug_exposure
-- -------------------------------------------------------------------

--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE mimiciv_cdm_tuf_10_ant_2020_09_11.cdm_drug_exposure
(
    drug_exposure_id              INT64       not null ,
    person_id                     INT64       not null ,
    drug_concept_id               INT64       not null ,
    drug_exposure_start_date      DATE        not null ,
    drug_exposure_start_datetime  DATETIME             ,
    drug_exposure_end_date        DATE        not null ,
    drug_exposure_end_datetime    DATETIME             ,
    verbatim_end_date             DATE                 ,
    drug_type_concept_id          INT64       not null ,
    stop_reason                   STRING               ,
    refills                       INT64                ,
    quantity                      FLOAT64              ,
    days_supply                   INT64                ,
    sig                           STRING               ,
    route_concept_id              INT64                ,
    lot_number                    STRING               ,
    provider_id                   INT64                ,
    visit_occurrence_id           INT64                ,
    visit_detail_id               INT64                ,
    drug_source_value             STRING               ,
    drug_source_concept_id        INT64                ,
    route_source_value            STRING               ,
    dose_unit_source_value        STRING               ,
    -- 
    unit_id                       STRING,
    load_table_id                 STRING,
    load_row_id                   INT64,
    trace_id                      STRING
)
;


INSERT INTO `@etl_project`.@etl_dataset.cdm_drug_exposure
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())           AS drug_exposure_id,
    per.person_id                               AS person_id,
    src.target_concept_id                       AS drug_concept_id,
    CAST(src.start_datetime AS DATE)            AS drug_exposure_start_date,
    src.start_datetime                          AS drug_exposure_start_datetime,
    CAST(src.end_datetime AS DATE)              AS drug_exposure_end_date,
    src.end_datetime                            AS drug_exposure_end_datetime,
    CAST(NULL AS DATE)                          AS verbatim_end_date,
    src.type_concept_id                         AS drug_type_concept_id,
    CAST(NULL AS STRING)                        AS stop_reason,
    CAST(NULL AS INT64)                         AS refills,
    src.quantity                                AS quantity,
    CAST(NULL AS INT64)                         AS days_supply,
    CAST(NULL AS STRING)                        AS sig,
    src.route_concept_id                        AS route_concept_id,
    CAST(NULL AS STRING)                        AS lot_number,
    CAST(NULL AS INT64)                         AS provider_id,
    vis.visit_occurrence_id                     AS visit_occurrence_id,
    CAST(NULL AS INT64)                         AS visit_detail_id,
    src.source_code                             AS drug_source_value,
    src.source_concept_id                       AS drug_source_concept_id,
    src.route_source_code                       AS route_source_value,
    src.dose_unit_source_code                   AS dose_unit_source_value,
    -- 
    CONCAT('drug.', src.unit_id)    AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    `@etl_project`.@etl_dataset.lk_drug_mapped src
LEFT JOIN 
    `@etl_project`.@etl_dataset.cdm_person per
        ON CAST(src.subject_id AS STRING) = per.person_source_value
LEFT JOIN 
    `@etl_project`.@etl_dataset.cdm_visit_occurrence vis
        ON CAST(src.hadm_id AS STRING) = vis.visit_source_value
WHERE
    src.target_domain_id = 'Drug'
;

-- MEASUREMENT / inputevent
-- ajouter champs unit_concept_id
-- type =  38000180 -- Inpatient administration
-- route = 4112421 -- intravenous ()

-- inputevent_mv
-- route_concept_source = ordercategorydescription (ordercategoryname)
-- -> CREER les deux concepts
-- cgid provider
-- privilegie rate
-- stop reason: statusdescription
-- quality_concept_id : when 1 then cancel else ok. --> infered from data.
-- when orderid then fact_relationship with 44818791 -- Has temporal context [SNOMED]
-- weight into observation/measurement

-- -- -------------------------------------------------------------------
-- -- lk_imv_clean
-- -- Rule 2, inputevents
-- -- -------------------------------------------------------------------

-- CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_imv_clean AS
-- SELECT
--     subject_id,
--     hadm_id,
--     itemid,
--     starttime AS drug_exposure_start_datetime,
--     endtime AS drug_exposure_end_datetime,
--     CASE WHEN rate IS NOT NULL THEN rate WHEN amount IS NOT NULL THEN amount ELSE NULL END AS quantity,
--     CASE WHEN rate IS NOT NULL THEN rateuom WHEN amount IS NOT NULL THEN amountuom ELSE NULL END AS dose_unit_source_value,
--     38000180 AS drug_type_concept_id -- Inpatient administration
-- --, 4112421 AS route_concept_id -- intraveous,
--     orderid = linkorderid AS is_leader -- other input are linked to it/them,
--     first_value(mimic_id) over(partition by orderid order by starttime ASC) = mimic_id AS is_orderid_leader -- other input are linked to it/them,
--     linkorderid,
--     orderid,
--     ordercategorydescription ||
--          ' (' ||
--          ordercategoryname ||
--          ')' AS route_source_value,
--     statusdescription AS stop_reason,
--     ordercategoryname,
--     cancelreason
-- FROM
--     `@etl_project`.@etl_dataset.src_inputevents
-- WHERE cancelreason = 0
-- ;

-- --"rxnorm_map AS SELECT distinct
--     --     ON  (drug_source_value) concept_id AS drug_concept_id, drug_source_value
-- -- FROM
--     -- `@etl_project`.@etl_dataset.mimic.gcpt_gdata_drug_exposure
-- -- LEFT JOIN
-- --     `@etl_project`.@etl_dataset.cdm_concept
--     --     ON  drug_concept_id::text = concept_code AND domain_id = 'Drug' WHERE drug_concept_id IS NOT NULL;

-- CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.tmp_rxnorm_map AS-- exploit the mapping based on ndc
-- select distinct 
--     drug_concept_id,
--     concept_name AS drug_source_value
-- FROM
--     `@etl_project`.@etl_dataset.cdm_drug_exposure
-- LEFT JOIN
--     `@etl_project`.@etl_dataset.cdm_concept
--         ON  drug_concept_id = concept_id where drug_concept_id != 0
-- ;


-- -- -------------------------------------------------------------------
-- -- Rule 2, inputevents
-- --
-- -- custom mapping
-- -- gcpt_inputevents_drug_to_concept
-- -- gcpt_mv_input_label_to_concept
-- -- gcpt_map_route_to_concept
-- -- -------------------------------------------------------------------

-- CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.tmp_gcpt_inputevents_drug_to_concept AS
-- SELECT
--     itemid, concept_id AS drug_concept_id
-- FROM
--     `@etl_project`.@etl_dataset.gcpt_inputevents_drug_to_concept
-- ;

-- CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.tmp_gcpt_mv_input_label_to_concept AS
-- SELECT DISTINCT
--         ON  (item_id) item_id AS itemid, concept_id AS drug_concept_id 
-- FROM
--     `@etl_project`.@etl_dataset.gcpt_mv_input_label_to_concept
-- ;

-- CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.tmp_gcpt_map_route_to_concept AS
-- SELECT
--     concept_id AS route_concept_id, ordercategoryname -- AS originalroute
-- FROM
--     `@etl_project`.@etl_dataset.gcpt_map_route_to_concept;

-- -- -------------------------------------------------------------------
-- -- Rule 2, inputevents
-- -- d_items, custom mapping
-- -- -------------------------------------------------------------------

-- CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.tmp_d_items AS
-- SELECT
--     itemid, label AS drug_source_value, mimic_id AS drug_source_concept_id
-- FROM
--     `@etl_project`.@etl_dataset.d_items;


-- -- It is for dose_unit_source_value (from "rule 3" for inputevents_scv) 
-- CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.tmp_gcpt_continuous_unit_carevue.csv AS
-- select dose_unit_source_value, dose_unit_source_value_new
-- from gcpt_continuous_unit_carevue;


-- -- -------------------------------------------------------------------
-- -- lk_imv_mapped
-- -- Rule 2, inputevents
-- -- -------------------------------------------------------------------

-- CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.tmp_row_to_insert AS
-- SELECT
-- drug_exposure_id,
--     person_id,
--     COALESCE(
--         rxnorm_map.drug_concept_id, 
--         gcpt_inputevents_drug_to_concept.drug_concept_id, 
--         gcpt_mv_input_label_to_concept.drug_concept_id, 
--         0)                                                  AS drug_concept_id,
--     drug_exposure_start_datetime,
--     drug_exposure_end_datetime,
--     null::date AS verbatim_end_date,
--     drug_type_concept_id,
--     stop_reason,
--     null::integer AS refills,
--     quantity,
--     null::integer AS days_supply,
--     null::text AS sig,
--     COALESCE(route_concept_id, 0) AS route_concept_id,
--     null::integer AS lot_number,
--     provider_id,
--     visit_occurrence_id,
--     null::integer AS visit_detail_id,
--     drug_source_value,
--     d_items.drug_source_concept_id,
--     route_source_value,
--     dose_unit_source_value
-- FROM
--     `@etl_project`.@etl_dataset.imv
-- LEFT JOIN gcpt_inputevents_drug_to_concept USING (itemid)
-- LEFT JOIN gcpt_mv_input_label_to_concept USING (itemid)
-- LEFT JOIN gcpt_map_route_to_concept USING (ordercategoryname)
-- LEFT JOIN d_items USING (itemid)
-- LEFT JOIN rxnorm_map USING (drug_source_value)
-- ;

-- INSERT INTO `@etl_project`.@etl_dataset.cdm_drug_exposure
-- SELECT
--     FARM_FINGERPRINT(GENERATE_UUID())           AS drug_exposure_id,
--     person_id,
--     drug_concept_id,
--     drug_exposure_start_date,
--     drug_exposure_start_datetime,
--     drug_exposure_end_date,
--     drug_exposure_end_datetime,
--     verbatim_end_date,
--     drug_type_concept_id,
--     stop_reason,
--     refills,
--     quantity,
--     days_supply,
--     sig,
--     route_concept_id,
--     lot_number,
--     provider_id,
--     row_to_insert.visit_occurrence_id,
--     visit_detail_assign.visit_detail_id,
--     drug_source_value,
--     drug_source_concept_id,
--     route_source_value,
--     dose_unit_source_value,
--     quantity::text AS quantity_source_value
-- FROM
--     `@etl_project`.@etl_dataset.lk_imv_mapped src -- lk_imv
-- LEFT JOIN 
--     `@etl_project`.@etl_dataset.cdm_person per
--         ON CAST(src.subject_id AS STRING) = per.person_source_value
-- LEFT JOIN 
--     `@etl_project`.@etl_dataset.cdm_visit_occurrence vis
--         ON CAST(src.hadm_id AS STRING) = vis.visit_source_value
-- -- LEFT JOIN 
-- --     `@etl_project`.@etl_dataset.cdm_visit_detail_assign
-- --         ON src.visit_occurrence_id = visit_detail_assign.visit_occurrence_id
-- --         AND
-- --         (--only one visit_detail
-- --             (is_first IS TRUE AND is_last IS TRUE)
-- --             OR -- first
-- --             (is_first IS TRUE AND is_last IS FALSE 
-- --                 AND row_to_insert.drug_exposure_start_datetime <= visit_detail_assign.visit_end_datetime)
-- --             OR -- last
-- --             (is_last IS TRUE AND is_first IS FALSE 
-- --                 AND row_to_insert.drug_exposure_start_datetime > visit_detail_assign.visit_start_datetime)
-- --             OR -- middle
-- --             (is_last IS FALSE AND is_first IS FALSE 
-- --                 AND row_to_insert.drug_exposure_start_datetime > visit_detail_assign.visit_start_datetime 
-- --                 AND row_to_insert.drug_exposure_start_datetime <= visit_detail_assign.visit_end_datetime)
-- --         )
-- WHERE
--     src.target_domain_id = 'Drug'
-- ;

