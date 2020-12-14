-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Populate cdm_procedure_occurrence table
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
-- remove dots from ICD codes
-- loaded custom mapping:
--      gcpt_procedure_to_concept -> mimiciv_proc_itemid
--      gcpt_datetimeevents_to_concept -> mimiciv_proc_datetimeevents
-- to review "custom mapping" from d_icd_procedures: domain_id = 'd_icd_procedures' AND vocabulary_id = 'MIMIC Local Codes'
-- to review relationship_id IN ('CPT4 - SNOMED eq','Maps to')
-- datetimeevents: to summarize count of duplicated rows or to use charttime instead of value?
-- Rule 1
--      implement procedure_type_concept_id(seq_num, 'Carrier claim detail - ([\d]+)th position')
-- Rule 1
--      add more custom mapping: gcpt_cpt4_to_concept --> mimiciv_proc_xxx (?)
-- -------------------------------------------------------------------


-- -------------------------------------------------------------------
-- lk_hcpcsevents_clean
-- Rule 1
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_hcpcsevents_clean AS
 SELECT
    src.subject_id      AS subject_id,
    src.hadm_id         AS hadm_id,
    adm.dischtime       AS procedure_datetime,
    src.seq_num         AS seq_num, --- procedure_type as in condtion_occurrence
    src.hcpcs_cd                            AS hcpcs_cd,
    src.short_description                   AS short_description,
    --
    src.load_table_id                   AS load_table_id,
    src.load_row_id                     AS load_row_id,
    src.trace_id                        AS trace_id
FROM
    `@etl_project`.@etl_dataset.src_hcpcsevents src
INNER JOIN
    `@etl_project`.@etl_dataset.src_admissions adm
        ON src.hadm_id = adm.hadm_id
;

-- -------------------------------------------------------------------
-- lk_proc_event_clean from procedureevents
-- Rule 2
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_proc_event_clean AS
SELECT
    src.hadm_id,
    src.subject_id,
    src.itemid,
    src.starttime AS procedure_datetime,
    d_items.label AS procedure_source_value, -- label = d_icd_procedures.icd9_code
    src.value AS quantity, -- THEN it stores the duration... this is a warkaround and may be inproved
    --
    src.load_table_id                   AS load_table_id,
    src.load_row_id                     AS load_row_id,
    src.trace_id                        AS trace_id
FROM
    `@etl_project`.@etl_dataset.src_procedureevents src
LEFT JOIN
    `@etl_project`.@etl_dataset.src_d_items d_items
        ON src.itemid = d_items.itemid
        AND d_items.linksto = 'procedureevents' -- to map everything, then split by domain_id?
where cancelreason = 0 -- not cancelled
;

-- -------------------------------------------------------------------
-- lk_procedures_icd_clean
-- Rule 3
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_procedures_icd_clean AS
SELECT
    src.subject_id                              AS subject_id,
    src.hadm_id                                 AS hadm_id,
    src.icd_code                                AS icd_code,
    src.icd_version                             AS icd_version,
    CONCAT(REPLACE(src.icd_code, '.', ''), '|', 
        CAST(src.icd_version AS STRING))        AS source_code, -- to join lk_icd_proc_concept
    adm.dischtime                               AS procedure_datetime,
    --
    src.load_table_id                   AS load_table_id,
    src.load_row_id                     AS load_row_id,
    src.trace_id                        AS trace_id
FROM
    `@etl_project`.@etl_dataset.src_procedures_icd src
INNER JOIN
    `@etl_project`.@etl_dataset.src_admissions adm
        ON src.hadm_id = adm.hadm_id
;

-- -------------------------------------------------------------------
-- datetimeevents
-- Rule 4
-- custom mapping gcpt_datetimeevents_to_concept -> mimiciv_proc_datetimeevents
-- -------------------------------------------------------------------
CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_datetimeevents_clean AS
SELECT
    src.subject_id           AS subject_id,
    src.hadm_id              AS hadm_id,
    src.itemid               AS itemid,
    src.value                AS start_datetime,
    di.label                AS source_code,
    --
    src.load_table_id                   AS load_table_id,
    src.load_row_id                     AS load_row_id,
    src.trace_id                        AS trace_id    
FROM
    `@etl_project`.@etl_dataset.src_datetimeevents src -- de
LEFT JOIN
    `@etl_project`.@etl_dataset.src_d_items di
        ON  src.itemid = di.itemid
;


-- -------------------------------------------------------------------
-- mapping
-- HCPCS Rule 1
-- add gcpt_cpt4_to_concept --> mimiciv_proc_xxx (?)
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_hcpcs_concept AS
SELECT
    CONCAT(REPLACE(vc.concept_code, '.', ''), '|', 
        REGEXP_EXTRACT(vc.vocabulary_id, r'[\d]+'))     AS source_code,
    vc.domain_id            AS source_domain_id,
    vc.concept_id           AS source_concept_id,
    vc2.domain_id           AS target_domain_id,
    vc2.concept_id          AS target_concept_id
FROM
    `@etl_project`.@etl_dataset.voc_concept vc
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept_relationship vcr
        ON  vc.concept_id = vcr.concept_id_1
        AND vcr.relationship_id = 'Maps to'
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept vc2
        ON vc2.concept_id = vcr.concept_id_2
        AND vc2.standard_concept = 'S'
        AND vc2.invalid_reason IS NULL
WHERE
    vc.vocabulary_id IN ('HCPCS', 'CPT4')
;

-- -------------------------------------------------------------------
-- mapping
-- Rule 2 
-- itemid -> gcpt_procedure_to_concept -> mimiciv_proc_itemid
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_itemid_concept AS
SELECT
    d_items.itemid          AS itemid,
    CAST(d_items.itemid AS STRING)    AS source_code,
    vc.domain_id            AS source_domain_id,
    vc.concept_id           AS source_concept_id,
    vc2.domain_id           AS target_domain_id,
    vc2.concept_id          AS target_concept_id
FROM
    `@etl_project`.@etl_dataset.src_d_items d_items
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept vc
        ON vc.concept_code = CAST(d_items.itemid AS STRING)
        AND vc.vocabulary_id = 'mimiciv_proc_itemid'
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept_relationship vcr
        ON  vc.concept_id = vcr.concept_id_1
        AND vcr.relationship_id = 'Maps to'
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept vc2
        ON vc2.concept_id = vcr.concept_id_2
        AND vc2.standard_concept = 'S'
        AND vc2.invalid_reason IS NULL
WHERE
    d_items.linksto = 'procedureevents' -- map all, then split by domain_id?
;

-- -------------------------------------------------------------------
-- mapping
-- ICD - Rule 3
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_icd_proc_concept AS
SELECT
    CONCAT(REPLACE(vc.concept_code, '.', ''), '|', 
        REGEXP_EXTRACT(vc.vocabulary_id, r'[\d]+'))     AS source_code,
    vc.domain_id            AS source_domain_id,
    vc.concept_id           AS source_concept_id,
    vc2.domain_id           AS target_domain_id,
    vc2.concept_id          AS target_concept_id
FROM
    `@etl_project`.@etl_dataset.voc_concept vc
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept_relationship vcr
        ON  vc.concept_id = vcr.concept_id_1
        AND vcr.relationship_id = 'Maps to'
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept vc2
        ON vc2.concept_id = vcr.concept_id_2
        AND vc2.standard_concept = 'S'
        AND vc2.invalid_reason IS NULL
WHERE
    vc.vocabulary_id IN ('ICD9Proc', 'ICD10PCS')
;

-- -------------------------------------------------------------------
-- mapping
-- Rule 4 
-- d_items.label -> gcpt_datetimeevents_to_concept -> mimiciv_proc_datetimeevents
-- can be put together with lk_itemid_concept
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_datetimeevents_concept AS
SELECT
    d_items.itemid          AS itemid,      -- we can use itemid or source_code, but not necessary both
    d_items.label           AS source_code,
    vc.domain_id            AS source_domain_id,
    vc.concept_id           AS source_concept_id,
    vc2.domain_id           AS target_domain_id,
    vc2.concept_id          AS target_concept_id
FROM
    `@etl_project`.@etl_dataset.src_d_items d_items
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept vc
        ON vc.concept_code = CAST(d_items.label AS STRING)
        AND vc.vocabulary_id = 'mimiciv_proc_datetimeevents'
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept_relationship vcr
        ON  vc.concept_id = vcr.concept_id_1
        AND vcr.relationship_id = 'Maps to'
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept vc2
        ON vc2.concept_id = vcr.concept_id_2
        AND vc2.standard_concept = 'S'
        AND vc2.invalid_reason IS NULL
WHERE
    d_items.linksto = 'datetimeevents' -- map all, then split by domain_id?
;

-- -------------------------------------------------------------------
-- lk_procedure_mapped
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_procedure_mapped AS

-- Rule 1, HCPCS
SELECT
    src.hadm_id                             AS hadm_id, -- to visit
    src.subject_id                          AS subject_id, -- to person
    COALESCE(lc.target_concept_id, 0)       AS target_concept_id,
    src.procedure_datetime                  AS start_datetime,
    257                                     AS type_concept_id, -- Hospitalization Cost Record
    src.hcpcs_cd                            AS source_code,
    COALESCE(lc.source_concept_id, 0)       AS source_concept_id,
    CAST(1 AS FLOAT64)                      AS quantity,
    CAST(NULL AS INT64)                     AS visit_detail_id,
    lc.target_domain_id                     AS target_domain_id,
    --
    'proc.hcpcsevents'              AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    `@etl_project`.@etl_dataset.lk_hcpcsevents_clean src
LEFT JOIN
    `@etl_project`.@etl_dataset.lk_hcpcs_concept lc
        ON src.hcpcs_cd = lc.source_code
;


INSERT INTO `@etl_project`.@etl_dataset.lk_procedure_mapped

-- rule 2, "proc_event" and custom mapping
SELECT
    src.hadm_id                             AS hadm_id, -- to visit
    src.subject_id                          AS subject_id, -- to person
    COALESCE(lc.target_concept_id, 0)       AS target_concept_id,
    src.procedure_datetime                  AS start_datetime,
    38000275                                AS type_concept_id, -- EHR order list entry,
    CAST(src.itemid AS STRING)              AS source_code,
    COALESCE(lc.source_concept_id, 0)       AS source_concept_id,
    src.quantity                            AS quantity,
    -- visit_detail_assign.visit_detail_id     AS visit_detail_id,
    CAST(NULL AS INT64)                     AS visit_detail_id,
    lc.target_domain_id                     AS target_domain_id,
    --
    'proc.procedureevents'          AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    `@etl_project`.@etl_dataset.lk_proc_event_clean src
LEFT JOIN
    `@etl_project`.@etl_dataset.lk_itemid_concept lc
        ON src.itemid = lc.itemid
-- LEFT JOIN
--     `@etl_project`.@etl_dataset.tmp_:OMOP_SCHEMA.visit_detail_assign
--         ON admissions.visit_occurrence_id = visit_detail_assign.visit_occurrence_id
-- AND
--     (--only one visit_detail
--         (is_first IS TRUE AND is_last IS TRUE)
--         OR -- first
--         (is_first IS TRUE AND is_last IS FALSE 
--             AND proc_event.procedure_datetime <= visit_detail_assign.visit_end_datetime)
--         OR -- last
--         (is_last IS TRUE AND is_first IS FALSE 
--             AND proc_event.procedure_datetime > visit_detail_assign.visit_start_datetime)
--         OR -- middle
--         (is_last IS FALSE AND is_first IS FALSE 
--             AND proc_event.procedure_datetime > visit_detail_assign.visit_start_datetime 
--             AND proc_event.procedure_datetime <= visit_detail_assign.visit_end_datetime)
--     )
;


INSERT INTO `@etl_project`.@etl_dataset.lk_procedure_mapped

-- rule 3, "admissions" and ICD
SELECT
    src.hadm_id                             AS hadm_id, -- to visit
    src.subject_id                          AS subject_id, -- to person
    COALESCE(lc.target_concept_id, 0)       AS target_concept_id,
    src.procedure_datetime                  AS start_datetime,
    38003622                                AS type_concept_id,
    src.source_code                         AS source_code,
    COALESCE(lc.source_concept_id, 0)       AS source_concept_id,
    1                                       AS quantity,
    CAST(NULL AS INT64)                     AS visit_detail_id,
    lc.target_domain_id                     AS target_domain_id,
    --
    'proc.procedures_icd'           AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    `@etl_project`.@etl_dataset.lk_procedures_icd_clean src
LEFT JOIN
    `@etl_project`.@etl_dataset.lk_icd_proc_concept lc
        ON src.source_code = lc.source_code
;


INSERT INTO `@etl_project`.@etl_dataset.lk_procedure_mapped

-- rule 4, "datetimeevents" and custom mapping
SELECT
    src.hadm_id                             AS hadm_id, -- to visit
    src.subject_id                          AS subject_id, -- to person
    COALESCE(lc.target_concept_id, 0)       AS target_concept_id,
    src.start_datetime                      AS start_datetime,
    38000275                                AS type_concept_id, -- EHR order list entry
    src.source_code                         AS source_code, -- add itemid?
    COALESCE(lc.source_concept_id, 0)       AS source_concept_id,
    1                                       AS quantity,
    -- visit_detail_assign.visit_detail_id     AS visit_detail_id,
    CAST(NULL AS INT64)                     AS visit_detail_id,
    lc.target_domain_id                     AS target_domain_id,
    --
    'proc.datetimeevents'           AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    `@etl_project`.@etl_dataset.lk_datetimeevents_clean src
LEFT JOIN
    `@etl_project`.@etl_dataset.lk_datetimeevents_concept lc
        ON src.itemid = lc.itemid
-- LEFT JOIN
--     `@etl_project`.@etl_dataset.tmp_visit_detail_assign vda
--         ON vis.visit_occurrence_id = vda.visit_occurrence_id
--         AND
--         (--only one visit_detail
--             (is_first IS TRUE AND is_last IS TRUE)
--             OR -- first
--             (is_first IS TRUE AND is_last IS FALSE AND src.observation_datetime <= vda.visit_end_datetime)
--             OR -- last
--             (is_last IS TRUE AND is_first IS FALSE AND src.observation_datetime > vda.visit_start_datetime)
--             OR -- middle
--             (is_last IS FALSE AND is_first IS FALSE AND src.observation_datetime > vda.visit_start_datetime 
--                 AND src.observation_datetime <= vda.visit_end_datetime)
--         )
;

-- -------------------------------------------------------------------
-- cdm_procedure_occurrence
-- -------------------------------------------------------------------

--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.cdm_procedure_occurrence
(
    procedure_occurrence_id     INT64     not null ,
    person_id                   INT64     not null ,
    procedure_concept_id        INT64     not null ,
    procedure_date              DATE      not null ,
    procedure_datetime          DATETIME           ,
    procedure_type_concept_id   INT64     not null ,
    modifier_concept_id         INT64              ,
    quantity                    INT64              ,
    provider_id                 INT64              ,
    visit_occurrence_id         INT64              ,
    visit_detail_id             INT64              ,
    procedure_source_value      STRING             ,
    procedure_source_concept_id INT64              ,
    modifier_source_value      STRING              ,
    -- 
    unit_id                       STRING,
    load_table_id                 STRING,
    load_row_id                   INT64,
    trace_id                      STRING
)
;

INSERT INTO `@etl_project`.@etl_dataset.cdm_procedure_occurrence
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())           AS procedure_occurrence_id,
    per.person_id                               AS person_id,
    src.target_concept_id                       AS procedure_concept_id,
    CAST(src.start_datetime AS DATE)            AS procedure_date,
    src.start_datetime                          AS procedure_datetime,
    src.type_concept_id                         AS procedure_type_concept_id,
    0                                           AS modifier_concept_id,
    CAST(src.quantity AS INT64)                 AS quantity,
    CAST(NULL AS INT64)                         AS provider_id,
    vis.visit_occurrence_id                     AS visit_occurrence_id,
    src.visit_detail_id                         AS visit_detail_id,
    src.source_code                             AS procedure_source_value,
    src.source_concept_id                       AS procedure_source_concept_id,
    CAST(NULL AS STRING)                        AS modifier_source_value,
    -- 
    CONCAT('procedure.', src.unit_id)           AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    `@etl_project`.@etl_dataset.lk_procedure_mapped src
INNER JOIN
    `@etl_project`.@etl_dataset.cdm_person per
        ON CAST(src.subject_id AS STRING) = per.person_source_value
INNER JOIN
    `@etl_project`.@etl_dataset.cdm_visit_occurrence vis
        ON  vis.visit_source_value = 
            CONCAT(CAST(src.subject_id AS STRING), '|', COALESCE(CAST(src.hadm_id AS STRING), 'None'))
WHERE
    src.target_domain_id = 'Procedure'
;

