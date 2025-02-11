-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Populate cdm_drug_exposure table
-- 
-- Dependencies: run after 
--      lk_drug_prescriptions.sql
--      cdm_person.sql,
--      cdm_visit_occurrence.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize create or replace
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- cdm_drug_exposure
-- -------------------------------------------------------------------

--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_drug_exposure
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

INSERT INTO @etl_project.@etl_dataset.cdm_drug_exposure
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
    @etl_project.@etl_dataset.lk_drug_mapped src
INNER JOIN
    @etl_project.@etl_dataset.cdm_person per
        ON CAST(src.subject_id AS STRING) = per.person_source_value
INNER JOIN
    @etl_project.@etl_dataset.cdm_visit_occurrence vis
        ON  vis.visit_source_value = 
            CONCAT(CAST(src.subject_id AS STRING), '|', CAST(src.hadm_id AS STRING))
WHERE
    src.target_domain_id = 'Drug'
;
