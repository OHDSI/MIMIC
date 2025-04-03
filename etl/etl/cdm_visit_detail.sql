-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Populate cdm_visit_detail table
-- 
-- Dependencies: run after 
--      st_core.sql,
--      st_hosp.sql,
--      st_waveform.sql,
--      lk_vis_adm_transfers.sql,
--      cdm_person.sql,
--      cdm_visit_occurrence.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize create or replace
-- negative unique id from FARM_FINGERPRINT(GENERATE_UUID())
--
-- src.callout - is there any derived table in MIMIC IV?
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- cdm_visit_detail
-- -------------------------------------------------------------------

--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_visit_detail
(
    visit_detail_id                    INT64     not null ,
    person_id                          INT64     not null ,
    visit_detail_concept_id            INT64     not null ,
    visit_detail_start_date            DATE      not null ,
    visit_detail_start_datetime        DATETIME           ,
    visit_detail_end_date              DATE      not null ,
    visit_detail_end_datetime          DATETIME           ,
    visit_detail_type_concept_id       INT64     not null , -- detail! -- this typo still exists in v.5.3.1(???)
    provider_id                        INT64              ,
    care_site_id                       INT64              ,
    admitting_source_concept_id        INT64              ,
    discharge_to_concept_id            INT64              ,
    preceding_visit_detail_id          INT64              ,
    visit_detail_source_value          STRING             ,
    visit_detail_source_concept_id     INT64              , -- detail! -- this typo still exists in v.5.3.1(???)
    admitting_source_value             STRING             ,
    discharge_to_source_value          STRING             ,
    visit_detail_parent_id             INT64              ,
    visit_occurrence_id                INT64     not null ,
    -- 
    unit_id                       STRING,
    load_table_id                 STRING,
    load_row_id                   INT64,
    trace_id                      STRING  
)
;

-- -------------------------------------------------------------------
-- Rule 1. transfers
-- Rule 2. services
-- -------------------------------------------------------------------




INSERT INTO @etl_project.@etl_dataset.cdm_visit_detail
SELECT
    src.visit_detail_id                     AS visit_detail_id,
    per.person_id                           AS person_id,
    COALESCE(vdc.target_concept_id, 0)      AS visit_detail_concept_id,
                                            -- see source value in care_site.care_site_source_value
    CAST(src.start_datetime AS DATE)        AS visit_start_date,
    src.start_datetime                      AS visit_start_datetime,
    CAST(src.end_datetime AS DATE)          AS visit_end_date,
    src.end_datetime                        AS visit_end_datetime,
    32817                                   AS visit_detail_type_concept_id,   -- EHR   Type Concept    Standard                          
    CAST(NULL AS INT64)                     AS provider_id,
    cs.care_site_id                         AS care_site_id,

    IF(
        src.admission_location IS NOT NULL,
        COALESCE(la.target_concept_id, 0),
        NULL)                               AS admitting_source_concept_id,
    IF(
        src.discharge_location IS NOT NULL,
        COALESCE(ld.target_concept_id, 0),
        NULL)                               AS discharge_to_concept_id,

    src.preceding_visit_detail_id           AS preceding_visit_detail_id,
    src.source_value                        AS visit_detail_source_value,
    COALESCE(vdc.source_concept_id, 0)      AS visit_detail_source_concept_id,
    src.admission_location                  AS admitting_source_value,
    src.discharge_location                  AS discharge_to_source_value,
    CAST(NULL AS INT64)                     AS visit_detail_parent_id,
    vis.visit_occurrence_id                 AS visit_occurrence_id,
    -- 
    CONCAT('visit_detail.', src.unit_id)    AS unit_id,
    src.load_table_id                 AS load_table_id,
    src.load_row_id                   AS load_row_id,
    src.trace_id                      AS trace_id
FROM
    @etl_project.@etl_dataset.lk_visit_detail_prev_next src
INNER JOIN
    @etl_project.@etl_dataset.cdm_person per 
        ON CAST(src.subject_id AS STRING) = per.person_source_value
INNER JOIN
    @etl_project.@etl_dataset.cdm_visit_occurrence vis 
        ON  vis.visit_source_value = 
            CONCAT(CAST(src.subject_id AS STRING), '|', 
                COALESCE(CAST(src.hadm_id AS STRING), CAST(src.date_id AS STRING)))
LEFT JOIN
    @etl_project.@etl_dataset.cdm_care_site cs
        ON cs.care_site_source_value = src.current_location
LEFT JOIN
    @etl_project.@etl_dataset.lk_visit_concept vdc
        ON vdc.source_code = src.current_location
LEFT JOIN
    @etl_project.@etl_dataset.lk_visit_concept la 
        ON la.source_code = src.admission_location
LEFT JOIN
    @etl_project.@etl_dataset.lk_visit_concept ld
        ON ld.source_code = src.discharge_location
;
