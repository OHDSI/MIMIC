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
--      at the moment rule 1 is only implemented
--      TODO: one distinct list of codes, then one join to vocabulary tables (1.42GB per vocab join)
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
    COALESCE(src.stoptime, 
        DATETIME_ADD(src.starttime, INTERVAL 1 DAY)) AS end_datetime,
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
    'prescriptions'                 AS unit_id,
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
        ON  vc.concept_id = vcr.concept_id_1 
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
        ON  vc.concept_id = vcr.concept_id_1 
        and vcr.relationship_id = 'Maps to'
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept vc2
        ON vc2.concept_id = vcr.concept_id_2
        AND vc2.standard_concept = 'S'
        AND vc2.invalid_reason IS NULL --covers 71% of rxnorm standards concepts
;

-- -------------------------------------------------------------------
-- lk_drug_mapped
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_drug_mapped AS
SELECT
    src.hadm_id                                     AS hadm_id,
    src.subject_id                                  AS subject_id,
    COALESCE(vc_ndc.target_concept_id, vc_gcpt.target_concept_id, 0)    AS target_concept_id,
    COALESCE(vc_ndc.target_domain_id, vc_gcpt.target_domain_id, 'Drug') AS target_domain_id,
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
    CONCAT('drug.', src.unit_id)    AS unit_id,
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
CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.cdm_drug_exposure
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
    CAST(src.quantity AS FLOAT64)               AS quantity,
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
