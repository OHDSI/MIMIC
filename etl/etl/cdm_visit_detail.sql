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
--      cdm_person.sql,
--      cdm_visit_occurrence,
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize create or replace
-- `@etl_project`.@etl_dataset.cdm_person;
--
-- negative unique id from FARM_FINGERPRINT(GENERATE_UUID())
--
-- gcpt_* tables - custom mapping
-- Add actual custom mapping
--
-- src.callout - is there any derived table in MIMIC IV?
--
-- CAST(NULL AS STRING)                AS visit_source_value, -- mandatory!
--
-- tmp_serv.visit_detail_id = src_service.load_row_id -> replace with proper visit_detail_id
--
-- FK violation for preceding_visit_detail_id.
-- -------------------------------------------------------------------

-- visit detail

-- -------------------------------------------------------------------
-- tmp_transfers_raw
--
-- Rule 1, from transfers without discharges
-- Rule 2, from admissions emergency only
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.tmp_transfers_raw AS
-- including  emergency
SELECT
    tr.subject_id                                   AS subject_id,
    tr.hadm_id                                      AS hadm_id,
    tr.careunit                                     AS careunit,
    tr.intime                                       AS intime,
    COALESCE(tr.outtime, adm.dischtime)             AS outtime,
    -- FARM_FINGERPRINT(GENERATE_UUID())               AS mimic_id, -- replace the id
    adm.admission_location                          AS admission_location,
    adm.discharge_location                          AS discharge_location,
    -- 
    'visit_detail.transfers'        AS unit_id,
    tr.load_table_id                AS load_table_id,
    tr.load_row_id                  AS load_row_id,
    tr.trace_id                     AS trace_id
FROM 
    `@etl_project`.@etl_dataset.src_transfers tr
LEFT JOIN 
    `@etl_project`.@etl_dataset.src_admissions adm
        ON tr.hadm_id = adm.hadm_id
WHERE 
    tr.eventtype != 'discharge' -- these are not useful
UNION ALL
SELECT
    adm.subject_id                                  AS subject_id,
    adm.hadm_id                                     AS hadm_id,
    'EMERGENCY'                                     AS careunit,
    adm.edregtime                                   AS intime,
    MIN(tr.intime)                                  AS dischtime, 
-- the end of the emergency is considered the begin of the the admission 
-- the admittime is sometime after the first transfer,
    -- FARM_FINGERPRINT(GENERATE_UUID()) AS mimic_id,
    adm.admission_location                          AS admission_location,
    adm.discharge_location                          AS discharge_location,
    -- 
    'visit_detail.admissions'       AS unit_id,
    adm.load_table_id                AS load_table_id,
    adm.load_row_id                  AS load_row_id,
    adm.trace_id                     AS trace_id
FROM 
    `@etl_project`.@etl_dataset.src_admissions adm
LEFT JOIN
    `@etl_project`.@etl_dataset.src_transfers tr
        ON adm.hadm_id = tr.hadm_id
WHERE 
    adm.edregtime IS NOT NULL -- only those having a emergency timestamped
GROUP BY
    adm.subject_id,
    adm.hadm_id,
    adm.edregtime,
    adm.admission_location,
    adm.discharge_location,
    adm.load_table_id,
    adm.load_row_id,
    adm.trace_id
;

-- -------------------------------------------------------------------
-- tmp_transfers
-- generate "mimic_id" = visit_detail_id here
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.tmp_transfers AS
-- including  emergency
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())               AS visit_detail_id,
    tr.subject_id                                   AS subject_id,
    tr.hadm_id                                      AS hadm_id,
    tr.careunit                                     AS careunit,
    tr.intime                                       AS intime,
    tr.outtime                                      AS outtime,
    tr.admission_location                           AS admission_location,
    tr.discharge_location                           AS discharge_location,
    -- 
    tr.unit_id                      AS unit_id,
    tr.load_table_id                AS load_table_id,
    tr.load_row_id                  AS load_row_id,
    tr.trace_id                     AS trace_id
FROM 
    `@etl_project`.@etl_dataset.tmp_transfers_raw tr
;

-- -------------------------------------------------------------------
-- tmp_gcpt_care_site
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.tmp_gcpt_care_site AS
SELECT 
    cs.care_site_name                   AS care_site_name,
    cs.care_site_id                     AS care_site_id,
    0       AS visit_detail_concept_id
    -- vc_cs.visit_detail_concept_id       AS visit_detail_concept_id
FROM 
    `@etl_project`.@etl_dataset.cdm_care_site cs
-- LEFT JOIN 
--     `@etl_project`.@etl_dataset.lk_gcpt_care_site vc_cs 
--         ON vc_cs.care_site_name = cs.care_site_source_value
;

-- -------------------------------------------------------------------
-- tmp_gcpt_admission_location_to_concept
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE 
    `@etl_project`.@etl_dataset.tmp_gcpt_admission_location_to_concept AS
SELECT
    0 AS admitting_concept_id,
    'admission_location' AS admission_location
--     vc_al.concept_id                AS admitting_concept_id, 
--     vc_al.admission_location        AS admission_location
-- FROM 
--     `@etl_project`.@etl_dataset.lk_gcpt_admission_location_to_concept vc_al
-- gcpt_admission_location_to_concept -> mimiciv_vis_admission_location
;

-- -------------------------------------------------------------------
-- tmp_gcpt_discharge_location_to_concept
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE 
    `@etl_project`.@etl_dataset.tmp_gcpt_discharge_location_to_concept AS
SELECT 
    0                AS discharge_to_concept_id, 
    'discharge_location'        AS discharge_location
--     vc_dl.concept_id                AS discharge_to_concept_id, 
--     vc_dl.discharge_location        AS discharge_location
-- FROM 
--     `@etl_project`.@etl_dataset.lk_gcpt_discharge_location_to_concept vc_al
-- gcpt_discharge_location_to_concept -> mimiciv_vis_discharge_location
;

-- -------------------------------------------------------------------
-- tmp_visit_detail_ward
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.tmp_visit_detail_ward AS
SELECT 
    tr.subject_id                       AS subject_id,
    vis.visit_occurrence_id             AS visit_occurrence_id,
    tr.hadm_id                          AS hadm_id,
    COALESCE(tr.careunit, 'UNKNOWN')    AS careunit,
    tr.intime                           AS intime,
    tr.outtime                          AS outtime,
    tr.visit_detail_id = first_value(tr.visit_detail_id) 
        OVER(PARTITION BY vis.visit_occurrence_id ORDER BY tr.intime ASC )  AS is_first,
    tr.visit_detail_id = last_value(tr.visit_detail_id)
        OVER(PARTITION BY vis.visit_occurrence_id ORDER BY tr.intime ASC 
            RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)              AS is_last,
    LAG(tr.visit_detail_id) 
        OVER (PARTITION BY tr.hadm_id ORDER BY tr.intime ASC)               AS preceding_visit_detail_id,
    tr.admission_location                          AS admission_location,
    tr.discharge_location                          AS discharge_location,
    --
    tr.unit_id                       AS unit_id,
    tr.load_table_id                 AS load_table_id,
    tr.load_row_id                   AS load_row_id,
    tr.trace_id                      AS trace_id
FROM 
    `@etl_project`.@etl_dataset.tmp_transfers tr
INNER JOIN 
    `@etl_project`.@etl_dataset.cdm_visit_occurrence vis 
        ON CAST(tr.hadm_id AS STRING) = vis.visit_source_value
;

-- -------------------------------------------------------------------
-- cdm_visit_detail
-- -------------------------------------------------------------------

--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.cdm_visit_detail
(
    visit_detail_id                    INT64     not null ,
    person_id                          INT64     not null ,
    visit_detail_concept_id            INT64     not null ,
    visit_detail_start_date            DATE      not null ,
    visit_detail_start_datetime        DATETIME           ,
    visit_detail_end_date              DATE      not null ,
    visit_detail_end_datetime          DATETIME           ,
    visit_deatil_type_concept_id       INT64     not null , -- deatil!
    provider_id                        INT64              ,
    care_site_id                       INT64              ,
    admitting_source_concept_id        INT64              ,
    discharge_to_concept_id            INT64              ,
    preceding_visit_detail_id          INT64              ,
    visit_detail_source_value          STRING             ,
    visit_deatil_source_concept_id     INT64              ,
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

INSERT INTO `@etl_project`.@etl_dataset.cdm_visit_detail
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())   AS visit_detail_id,
    per.person_id                       AS person_id,
    coalesce(vc_cs.visit_detail_concept_id, 2000000013) AS visit_detail_concept_id, --unknown
    CAST(tr.intime AS DATE)             AS visit_start_date,
    tr.intime                           AS visit_start_datetime,
    CAST(tr.outtime AS DATE)            AS visit_end_date,
    tr.outtime                          AS visit_end_datetime,
    2000000006                          AS visit_deatil_type_concept_id,  -- [MIMIC Generated] ward and physical
    CAST(NULL AS INT64)                 AS provider_id,
    vc_cs.care_site_id                  AS care_site_id,
    CASE 
        WHEN is_first IS FALSE THEN 4030023
        ELSE vc_al.admitting_concept_id
    END                                 AS admitting_source_concept_id,
    CASE 
        WHEN is_last IS FALSE THEN 4030023
        ELSE vc_dl.discharge_to_concept_id
    END                                 AS discharge_to_concept_id,
    tr.preceding_visit_detail_id        AS preceding_visit_detail_id,
    tr.trace_id                    AS visit_detail_source_value, -- mandatory! -- to define the value
    CAST(NULL AS INT64)                 AS visit_detail_source_concept_id,
    CASE 
        WHEN is_first IS FALSE THEN 'transfer'
        ELSE tr.admission_location
    END                                 AS admitting_source_value,
    CASE 
        WHEN is_last IS FALSE THEN 'transfer'
        ELSE tr.discharge_location
    END                                 AS discharge_to_source_value,
    CAST(NULL AS INT64)                 AS visit_detail_parent_id,
    tr.visit_occurrence_id,
    -- 
    tr.unit_id                       AS unit_id,
    tr.load_table_id                 AS load_table_id,
    tr.load_row_id                   AS load_row_id,
    tr.trace_id                      AS trace_id
FROM 
    `@etl_project`.@etl_dataset.tmp_visit_detail_ward tr
LEFT JOIN 
    `@etl_project`.@etl_dataset.cdm_person per 
        ON CAST(tr.subject_id AS STRING) = per.person_source_value
LEFT JOIN 
    `@etl_project`.@etl_dataset.tmp_gcpt_admission_location_to_concept vc_al
        ON tr.admission_location = vc_al.admission_location
LEFT JOIN 
    `@etl_project`.@etl_dataset.tmp_gcpt_discharge_location_to_concept vc_dl
        ON tr.discharge_location = vc_dl.discharge_location
LEFT JOIN 
    `@etl_project`.@etl_dataset.tmp_gcpt_care_site vc_cs
        ON vc_cs.care_site_name = tr.careunit
;


-- -------------------------------------------------------------------
-- src.callout -> callout_delay 
-- cdm_cohort_attribute:
--     1 AS  attribute_definition_id -- callout delay from callout_delay
--     2  AS  attribute_definition_id  -- visit delay from visit_detail_ward
-- -------------------------------------------------------------------

-- callout_delay AS
-- SELECT
--     visit_detail_id                                 AS subject_id,
--     visit_start_datetime                            AS cohort_start_date,
--     visit_end_datetime                              AS cohort_end_date,
--     EXTRACT(
--         epoch FROM outcometime - createtime
--     ) / 3600 / 24                                   AS discharge_delay,
--     (outcometime - createtime) / 2 + createtime     AS mean_time
-- FROM callout
-- LEFT JOIN
--     visit_detail_ward v
--         ON v.hadm_id = callout.hadm_id
--         AND callout.careunit = v.careunit
--         AND ((outcometime - createtime) / 2 + createtime) 
--             BETWEEN v.visit_start_datetime AND v.visit_end_datetime
-- WHERE 
--     callout_outcome NOT ILIKE 'cancel%' 
--     AND visit_detail_id IS NOT NULL
-- ;

-- -- insert_callout_delay AS (
-- INSERT INTO cdm_cohort_attribute
-- SELECT
--     0 AS cohort_definition_id,
--     cohort_start_date,
--     cohort_end_date,
--     subject_id,
--     1 AS  attribute_definition_id, -- callout delay
--     discharge_delay AS value_as_number,
--     0 value_as_concept_id
-- FROM 
--     callout_delay
-- ;

-- -- insert_visit_detail_delay AS (
-- INSERT INTO cdm_cohort_attribute
-- SELECT
--     0 AS cohort_definition_id,
--     visit_start_datetime AS  cohort_start_date,
--     visit_end_datetime AS cohort_end_date,
--     visit_detail_id AS subject_id,
--     2  AS  attribute_definition_id,  -- visit delay
--     EXTRACT(
--         epoch FROM visit_end_datetime - visit_start_datetime
--     ) / 3600 / 24               AS  value_as_number,
--     0                           AS value_as_concept_id
-- FROM 
--     visit_detail_ward
-- ;


-- -------------------------------------------------------------------
-- Rule 3, services information (src: hosp.services)
-- -------------------------------------------------------------------

-- SERVICES information

-- cdm_person 
-- tmp_gcpt_care_site
-- src_admission (tmp_admission - because we fetch visit_occurrence_id(?))

-- -------------------------------------------------------------------
-- tmp_serv_tmp
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.tmp_serv_tmp AS
SELECT
    serv.load_row_id AS visit_detail_id,
    serv.subject_id,
    serv.hadm_id,
    serv.transfertime,
    serv.prev_service,
    serv.curr_service,
    vis.visit_occurrence_id, -- mimic_id
    LEAD(serv.load_row_id) OVER (PARTITION BY serv.hadm_id ORDER BY serv.transfertime) AS next,
    LAG(serv.load_row_id) OVER (PARTITION BY serv.hadm_id ORDER BY serv.transfertime) AS prev,
    adm.admittime,
    adm.dischtime,
    --
    serv.load_table_id                 AS load_table_id,
    serv.load_row_id                   AS load_row_id,
    serv.trace_id                      AS trace_id
FROM 
    `@etl_project`.@etl_dataset.src_services serv
INNER JOIN 
    `@etl_project`.@etl_dataset.cdm_visit_occurrence vis 
        ON CAST(serv.hadm_id AS STRING) = vis.visit_source_value
LEFT JOIN 
    `@etl_project`.@etl_dataset.src_admissions adm
        ON serv.hadm_id = adm.hadm_id
;

-- -------------------------------------------------------------------
-- tmp_serv
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.tmp_serv AS
SELECT
    serv_tmp.visit_occurrence_id,
    serv_tmp.load_row_id               AS visit_detail_id,
    serv_tmp.subject_id,
    serv_tmp.hadm_id,
    serv_tmp.curr_service,
    serv_adm_prev.load_row_id          AS preceding_visit_detail_id,
    serv_tmp.transfertime           AS start_datetime,
    CASE 
        WHEN serv_tmp.prev IS NULL     AND serv_tmp.next IS NOT NULL THEN serv_adm_next.transfertime
        WHEN serv_tmp.prev IS NULL     AND serv_tmp.next IS NULL     THEN serv_tmp.dischtime
        WHEN serv_tmp.prev IS NOT NULL AND serv_tmp.next IS NULL     THEN serv_tmp.dischtime
        WHEN serv_tmp.prev IS NOT NULL AND serv_tmp.next IS NOT NULL THEN serv_adm_next.transfertime
    END                             AS end_datetime,
    --
    'visit_detail.services'          AS unit_id,
    serv_tmp.load_table_id          AS load_table_id,
    serv_tmp.load_row_id            AS load_row_id,
    serv_tmp.trace_id               AS trace_id
FROM 
    `@etl_project`.@etl_dataset.tmp_serv_tmp serv_tmp
LEFT JOIN 
    `@etl_project`.@etl_dataset.tmp_serv_tmp serv_adm_prev 
        ON (serv_tmp.prev = serv_adm_prev.visit_detail_id)
LEFT JOIN 
    `@etl_project`.@etl_dataset.tmp_serv_tmp serv_adm_next 
        ON (serv_tmp.next = serv_adm_next.visit_detail_id)
;


-- -------------------------------------------------------------------
-- cdm_visit_detail
-- insert rows for Rule 3
-- -------------------------------------------------------------------

INSERT INTO `@etl_project`.@etl_dataset.cdm_visit_detail -- SERVICE INFORMATIONS
SELECT
    tr.visit_detail_id                  AS visit_detail_id,
    per.person_id                       AS person_id,
    coalesce(vc_cs.visit_detail_concept_id, 0)  AS visit_detail_concept_id,
    CAST(tr.start_datetime AS DATE)     AS visit_start_date,
    tr.start_datetime                   AS visit_start_datetime,
    CAST(tr.end_datetime AS DATE)       AS visit_end_date,
    tr.end_datetime                     AS visit_end_datetime,
    45770670                            AS visit_deatil_type_concept_id,
    CAST(NULL AS INT64)                 AS provider_id,
    vc_cs.care_site_id                  AS care_site_id,
    0                                   AS admitting_source_concept_id,
    0                                   AS discharge_to_concept_id,
    tr.preceding_visit_detail_id        AS preceding_visit_detail_id,
    tr.trace_id                         AS visit_detail_source_value, -- mandatory!
    CAST(NULL AS INT64)                 AS visit_detail_source_concept_id, -- null???
    CAST(NULL AS STRING)                AS admitting_source_value,
    CAST(NULL AS STRING)                AS discharge_to_source_value,
    CAST(NULL AS INT64)                 AS visit_detail_parent_id,
    tr.visit_occurrence_id              AS visit_occurrence_id,
    -- 
    tr.unit_id                       AS unit_id,
    tr.load_table_id                 AS load_table_id,
    tr.load_row_id                   AS load_row_id,
    tr.trace_id                      AS trace_id
FROM 
    `@etl_project`.@etl_dataset.tmp_serv tr
LEFT JOIN 
    `@etl_project`.@etl_dataset.tmp_gcpt_care_site vc_cs
        ON vc_cs.care_site_name = tr.curr_service
LEFT JOIN 
    `@etl_project`.@etl_dataset.cdm_person per 
        ON CAST(tr.subject_id AS STRING) = per.person_source_value
;

-- -------------------------------------------------------------------
-- :OMOP_SCHEMA.visit_detail_assign
--      this table is not a standard OMOP table
-- -------------------------------------------------------------------

-- -- first draft of icustay assignation table
-- -- the way of assigning is quite simple right now
-- -- but simple error is better than complicate error
-- -- meaning, those links are artificial watever we do
-- DROP TABLE IF EXISTS :OMOP_SCHEMA.visit_detail_assign;
-- CREATE TABLE :OMOP_SCHEMA.visit_detail_assign AS
-- SELECT
-- visit_detail_id,
--     visit_occurrence_id,
--     visit_start_datetime,
--     visit_end_datetime,
--     visit_detail_id = first_value(visit_detail_id) OVER(PARTITION BY visit_occurrence_id 
--         ORDER BY visit_start_datetime ASC )                                                     AS  is_first,
--     visit_detail_id = last_value(visit_detail_id) OVER(PARTITION BY visit_occurrence_id 
--         ORDER BY visit_start_datetime ASC range between current row and unbounded following)    AS is_last,
--     visit_detail_concept_id = 581382 AS is_icu,
--     visit_detail_concept_id = 581381 AS is_emergency
-- FROM  :OMOP_SCHEMA.visit_detail
-- WHERE visit_type_concept_id = 2000000006 -- only ward kind
-- ;


-- -------------------------------------------------------------------
-- visit_detail for waveforms
-- -------------------------------------------------------------------

INSERT INTO `@etl_project`.@etl_dataset.cdm_visit_detail
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())   AS visit_detail_id,
    per.person_id                       AS person_id,
    0                                   AS visit_detail_concept_id, -- to find the applicable
    CAST(wh.start_datetime AS DATE)     AS visit_start_date,
    wh.start_datetime                   AS visit_start_datetime,
    CAST(wh.end_datetime AS DATE)       AS visit_end_date,
    wh.end_datetime                     AS visit_end_datetime,
    2000000006                          AS visit_deatil_type_concept_id,  -- [MIMIC Generated] ward and physical
    CAST(NULL AS INT64)                 AS provider_id,
    0                                   AS care_site_id, -- see if there are info in wh
    0                                   AS admitting_source_concept_id,
    0                                   AS discharge_to_concept_id,
    0                                   AS preceding_visit_detail_id, -- to find out
    wh.reference_id                     AS visit_detail_source_value,
    CAST(NULL AS INT64)                 AS visit_detail_source_concept_id,
    CAST(NULL AS STRING)                AS admitting_source_value,
    CAST(NULL AS STRING)                AS discharge_to_source_value,
    CAST(NULL AS INT64)                 AS visit_detail_parent_id,
    vis.visit_occurrence_id,
    -- 
    CONCAT('visit_detail.waveform_header')   AS unit_id,
    wh.load_table_id                 AS load_table_id,
    wh.load_row_id                   AS load_row_id,
    wh.trace_id                      AS trace_id
FROM
    `@etl_project`.@etl_dataset.src_waveform_header wh
LEFT JOIN
    `@etl_project`.@etl_dataset.cdm_person per
        ON CAST(wh.subject_id AS STRING) = per.person_source_value
LEFT JOIN
    `@etl_project`.@etl_dataset.cdm_visit_occurrence vis 
        ON CAST(wh.hadm_id AS STRING) = vis.visit_source_value
;

