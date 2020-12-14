-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Populate cdm_specimen table
-- 
-- Dependencies: run after 
--      cdm_person.sql,
--      cdm_visit_occurrence,
--      cdm_visit_detail(?)
--      lk_meas_labevents (specimen from labevents, unit_concept)
--      
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize create or replace
--
-- -------------------------------------------------------------------

-- use     `@etl_project`.@etl_dataset.lk_meas_unit_concept uc


-- -------------------------------------------------------------------
-- Rule 1 specimen from microbiology
-- -------------------------------------------------------------------


-- -------------------------------------------------------------------
-- Rule 2 specimen from chartevents (fake?)
-- -------------------------------------------------------------------


-- -------------------------------------------------------------------
-- Rule 3 specimen from labevents (fake?)
-- -------------------------------------------------------------------


-- -------------------------------------------------------------------
-- lk_specimen_lab_concept
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_specimen_lab_concept AS
SELECT
    vc.concept_code     AS source_code, -- gcpt.label
    vc.domain_id        AS source_domain_id,
    vc.concept_id       AS source_concept_id,
    vc2.domain_id       AS target_domain_id,
    vc2.concept_id      AS target_concept_id
    dlab.fluid              AS fluid,
    dlab.category           AS category -- 'Blood Gas', 'Chemistry', 'Hematology'
FROM
    `@etl_project`.@etl_dataset.src_d_labitems dlab
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept vc
        ON  dlab.fluid = vc.concept_code
        -- gcpt_labs_specimen_to_concept -> mimiciv_spe_lab_specimen
        AND vc.vocabulary_id = 'mimiciv_spe_lab_specimen'
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

LEFT JOIN
    `@etl_project`.@etl_dataset.tmp_lab_specimen_concept spc
        ON labc.fluid = spc.source_code

-- -------------------------------------------------------------------
-- lk_specimen_lab
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_specimen_lab AS
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())       AS specimen_id,
    src.subject_id                          AS subject_id,
    COALESCE(spc.target_concept_id, 0)      AS specimen_concept_id,
    581378                                  AS specimen_type_concept_id, -- EHR Detail
    src.charttime                           AS specimen_datetime,
    src.measurement_id                      AS measurement_id -- usefull for fact_relationship
FROM  
    `@etl_project`.@etl_dataset.lk_meas_labevents_clean src
INNER JOIN 
    `@etl_project`.@etl_dataset.lk_meas_d_labitems_concept labc
        ON labc.itemid = src.itemid
INNER JOIN
    `@etl_project`.@etl_dataset.lk_specimen_lab_concept spc
        ON labc.fluid = spc.source_code
;


-- -------------------------------------------------------------------
-- lk_fr_specimen_measurement
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_fr_specimen_measurement AS
SELECT
    36 AS domain_concept_id_1, -- Specimen
    specimen_id as fact_id_1,
    21 AS domain_concept_id_2, -- Measurement
    measurement_id as fact_id_2,
    44818854 as relationship_concept_id -- Specimen of (SNOMED)
FROM
    `@etl_project`.@etl_dataset.lk_specimen_lab
UNION ALL
SELECT
    21 AS domain_concept_id_1, -- Measurement
    measurement_id as fact_id_1,
    36 AS domain_concept_id_2, -- Specimen
    specimen_id as fact_id_2,
    44818756 as relationship_concept_id -- Has specimen (SNOMED)   
FROM
    `@etl_project`.@etl_dataset.lk_specimen_lab
;



























-- -------------------------------------------------------------------
-- lk_specimen_mapped
-- -------------------------------------------------------------------


LEFT JOIN
    `@etl_project`.@etl_dataset.lk_specimen_lab_concept spc
        ON labc.fluid = spc.source_code


-- -------------------------------------------------------------------
-- cdm_specimen
-- -------------------------------------------------------------------

--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.cdm_specimen
(
    specimen_id                 INT64     not null ,
    person_id                   INT64     not null ,
    specimen_concept_id         INT64     not null ,
    specimen_type_concept_id    INT64     not null ,
    specimen_date               DATE      not null ,
    specimen_datetime           DATETIME           ,
    quantity                    FLOAT64            ,
    unit_concept_id             INT64              ,
    anatomic_site_concept_id    INT64              ,
    disease_status_concept_id   INT64              ,
    specimen_source_id          STRING             ,
    specimen_source_value       STRING             ,
    unit_source_value           STRING             ,
    anatomic_site_source_value  STRING             ,
    disease_status_source_value STRING             ,
    -- 
    unit_id                       STRING,
    load_table_id                 STRING,
    load_row_id                   INT64,
    trace_id                      STRING
)
;


INSERT INTO `@etl_project`.@etl_dataset.cdm_specimen
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())           AS specimen_id,
    per.person_id                               AS person_id,
    src.target_concept_id                       AS specimen_concept_id, -- class Specimen, vocab SNOMED, domain Specimen
    src.type_concept_id                         AS specimen_type_concept_id,
    CAST(src.start_datetime AS DATE)            AS specimen_date,
    src.start_datetime                          AS specimen_datetime,
    CAST(src.quantity AS INT64)                 AS quantity,
    src.unit_concept_id                         AS unit_concept_id,
    src.anatomic_site_concept_id                AS anatomic_site_concept_id
    src.disease_status_concept_id               AS disease_status_concept_id,
    src.specimen_source_id                      AS specimen_source_id,
    src.specimen_source_value                   AS specimen_source_value,
    src.unit_source_value                       AS unit_source_value,
    src.anatomic_site_source_value              AS anatomic_site_source_value,
    src.disease_status_source_value             AS disease_status_source_value,
    -- 
    CONCAT('specimen.', src.unit_id)    AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    `@etl_project`.@etl_dataset.lk_specimen_mapped src
INNER JOIN
    `@etl_project`.@etl_dataset.cdm_person per
        ON CAST(src.subject_id AS STRING) = per.person_source_value
WHERE
    src.target_domain_id = 'Specimen'
;
