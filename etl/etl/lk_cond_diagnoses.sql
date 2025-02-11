-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Populate cdm_condition_occurrence table
-- 
-- Dependencies: run after 
--      st_core.sql,
--      st_hosp.sql
--
-- Remove dots from icd_code
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize create or replace
--
-- Why not mimic_id as in III? 
--      Because source rows can be multiplied during the ETL.
-- seq_num in custom mapping (seq_num_to_concept): 
--      Replace deprecated detailed type concepts with standard: 32821    OMOP4976894 EHR billing record
--      TODO in later interations: investigate a possibility to prioritize diagnoses according to seq_num
-- condition_status_source_value / concept_id
--      investigate if there is data for conditons status
-- -------------------------------------------------------------------

-- 4,520 rows on demo

-- -------------------------------------------------------------------
-- lk_diagnoses_icd_clean
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.lk_diagnoses_icd_clean AS
SELECT
    src.subject_id                              AS subject_id,
    src.hadm_id                                 AS hadm_id,
    CASE 
        WHEN src.seq_num > 20 THEN 20
        ELSE src.seq_num
    END                                         AS seq_num, -- to fit "Inpatient detail %" concepts provided by OMOP
    COALESCE(adm.edregtime, adm.admittime)      AS start_datetime, -- always exists
    dischtime                                   AS end_datetime,
    src.icd_code                                AS source_code,
    CASE 
        WHEN src.icd_version = 9 THEN 'ICD9CM'
        WHEN src.icd_version = 10 THEN 'ICD10CM'
        ELSE NULL
    END                                         AS source_vocabulary_id,
    --
    'diagnoses_icd'         AS unit_id,
    src.load_table_id       AS load_table_id,
    src.load_row_id         AS load_row_id,
    src.trace_id            AS trace_id
FROM
    @etl_project.@etl_dataset.src_diagnoses_icd src
INNER JOIN
    @etl_project.@etl_dataset.src_admissions adm
        ON  src.hadm_id = adm.hadm_id
;

-- -------------------------------------------------------------------
-- lk_djagnoses_icd_mapped
-- Since Condition_occurrence is quite simple, skip creating a separate codes table,
-- but create mapped table, which goes to Condition and Drug as well
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.lk_diagnoses_icd_mapped AS
SELECT
    src.subject_id                      AS subject_id,
    src.hadm_id                         AS hadm_id,
    src.seq_num                         AS seq_num,
    src.start_datetime                  AS start_datetime,
    src.end_datetime                    AS end_datetime,
    32821                               AS type_concept_id, -- OMOP4976894 EHR billing record
    --
    src.source_code                     AS source_code,
    src.source_vocabulary_id            AS source_vocabulary_id,
    vc.concept_id                       AS source_concept_id,
    vc.domain_id                        AS source_domain_id,
    vc2.concept_id                      AS target_concept_id,
    vc2.domain_id                       AS target_domain_id,
    --
    CONCAT('cond.', src.unit_id) AS unit_id,
    src.load_table_id       AS load_table_id,
    src.load_row_id         AS load_row_id,
    src.trace_id            AS trace_id  
FROM
    @etl_project.@etl_dataset.lk_diagnoses_icd_clean src
LEFT JOIN
    @etl_project.@etl_dataset.voc_concept vc
        ON REPLACE(vc.concept_code, '.', '') = REPLACE(TRIM(src.source_code), '.', '')
        AND vc.vocabulary_id = src.source_vocabulary_id
LEFT JOIN
    @etl_project.@etl_dataset.voc_concept_relationship vcr
        ON  vc.concept_id = vcr.concept_id_1
        AND vcr.relationship_id = 'Maps to'
LEFT JOIN
    @etl_project.@etl_dataset.voc_concept vc2
        ON vc2.concept_id = vcr.concept_id_2
        AND vc2.standard_concept = 'S'
        AND vc2.invalid_reason IS NULL
;

-- -------------------------------------------------------------------
-- gcpt_admissions_diagnosis_to_concept 
--      do not use since there is no diagnosis in core.admissions
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- cleanup
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS @etl_project.@etl_dataset.tmp_seq_num_to_concept;
