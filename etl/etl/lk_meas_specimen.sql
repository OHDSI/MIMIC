-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Populate lookup tables for cdm_specimen and cdm_measurement tables
-- 
-- Dependencies: run after 
--      st_hosp
--      
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- microbiology custom mapping:
--      gcpt_microbiology_specimen_to_concept -> mimiciv_micro_specimen -- loaded
--          d_micro.label = mbe.spec_type_desc -> source_code
--      (gcpt) brand new vocab -> mimiciv_micro_microtest -- loaded
--          d_micro.label = mbe.test_name -> source_code
--      gcpt_org_name_to_concept -> mimiciv_micro_organism -- loaded
--          d_micro.label = mbe.org_name -> source_code
--      gcpt_atb_to_concept -> mimiciv_micro_antibiotic -- loaded
--          d_micro.label = mbe.ab_name -> source_code
--          https://athena.ohdsi.org/search-terms/terms?domain=Measurement&conceptClass=Lab+Test&page=1&pageSize=15&query=susceptibility 
--      (gcpt) brand new vocab -> mimiciv_micro_resistance -- loaded
--        src_microbiologyevents.interpretation -> source_code
--
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- lk_micro_cross_ref
-- group microevent_id = trace_id for each type of records
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.lk_micro_cross_ref AS
SELECT
    trace_id                                    AS trace_id_ab, -- for antibiotics
    FIRST_VALUE(src.trace_id) OVER (
        PARTITION BY
            src.subject_id,
            src.hadm_id,
            COALESCE(src.charttime, src.chartdate),
            src.spec_itemid,
            src.test_itemid,
            src.org_itemid
        ORDER BY src.trace_id
    )                                           AS trace_id_org, -- for test-organism pairs
    FIRST_VALUE(src.trace_id) OVER (
        PARTITION BY
            src.subject_id,
            src.hadm_id,
            COALESCE(src.charttime, src.chartdate),
            src.spec_itemid
        ORDER BY src.trace_id
    )                                           AS trace_id_spec, -- for specimen
    subject_id                                  AS subject_id,    -- to pick additional hadm_id from admissions
    hadm_id                                     AS hadm_id,
    COALESCE(src.charttime, src.chartdate)      AS start_datetime -- just to do coalesce once
FROM
    @etl_project.@etl_dataset.src_microbiologyevents src -- mbe
;

-- -------------------------------------------------------------------
-- lk_micro_hadm_id
-- pick additional hadm_id by event start_datetime
-- row_num is added to select the earliest if more than one hadm_ids are found
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.lk_micro_hadm_id AS
SELECT
    src.trace_id_ab                     AS event_trace_id,
    adm.hadm_id                         AS hadm_id,
    ROW_NUMBER() OVER (
        PARTITION BY src.trace_id_ab
        ORDER BY adm.start_datetime
    )                                   AS row_num
FROM  
    @etl_project.@etl_dataset.lk_micro_cross_ref src
INNER JOIN 
    @etl_project.@etl_dataset.lk_admissions_clean adm
        ON adm.subject_id = src.subject_id
        AND src.start_datetime BETWEEN adm.start_datetime AND adm.end_datetime
WHERE
    src.hadm_id IS NULL
;

-- -------------------------------------------------------------------
-- Part 2 of microbiology: test taken and organisms grown in the material of the test
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.lk_meas_organism_clean AS
SELECT DISTINCT
    src.subject_id                              AS subject_id,
    src.hadm_id                                 AS hadm_id,
    cr.start_datetime                           AS start_datetime,
    src.spec_itemid                             AS spec_itemid, -- d_micro.itemid, type of specimen taken
    src.test_itemid                             AS test_itemid, -- d_micro.itemid, test taken from the specimen
    src.org_itemid                              AS org_itemid, -- d_micro.itemid, organism which has grown
    cr.trace_id_spec                            AS trace_id_spec, -- to link org and spec in fact_relationship
    -- 
    'micro.organism'                AS unit_id,
    src.load_table_id               AS load_table_id,
    0                               AS load_row_id,
    cr.trace_id_org                 AS trace_id         -- trace_id for test-organism
FROM
    @etl_project.@etl_dataset.src_microbiologyevents src -- mbe
INNER JOIN
    @etl_project.@etl_dataset.lk_micro_cross_ref cr
        ON src.trace_id = cr.trace_id_org
;

-- -------------------------------------------------------------------
-- Part 1 of microbiology: specimen
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.lk_specimen_clean AS
SELECT DISTINCT
    src.subject_id                              AS subject_id,
    src.hadm_id                                 AS hadm_id,
    src.start_datetime                          AS start_datetime,
    src.spec_itemid                             AS spec_itemid, -- d_micro.itemid, type of specimen taken
    -- 
    'micro.specimen'                AS unit_id,
    src.load_table_id               AS load_table_id,
    0                               AS load_row_id,
    cr.trace_id_spec                AS trace_id         -- trace_id for specimen
FROM
    @etl_project.@etl_dataset.lk_meas_organism_clean src -- mbe
INNER JOIN
    @etl_project.@etl_dataset.lk_micro_cross_ref cr
        ON src.trace_id = cr.trace_id_spec
;

-- -------------------------------------------------------------------
-- Part 3 of microbiology: antibiotics tested on organisms
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.lk_meas_ab_clean AS
SELECT
    src.subject_id                              AS subject_id,
    src.hadm_id                                 AS hadm_id,
    cr.start_datetime                           AS start_datetime,
    src.ab_itemid                               AS ab_itemid, -- antibiotic tested
    src.dilution_comparison                     AS dilution_comparison, -- operator sign
    src.dilution_value                          AS dilution_value, -- numeric dilution value
    src.interpretation                          AS interpretation, -- degree of resistance
    cr.trace_id_org                             AS trace_id_org, -- to link org to ab in fact_relationship
    -- 
    'micro.antibiotics'             AS unit_id,
    src.load_table_id               AS load_table_id,
    0                               AS load_row_id,
    src.trace_id                    AS trace_id         -- trace_id for antibiotics, no groupping is needed
FROM
    @etl_project.@etl_dataset.src_microbiologyevents src
INNER JOIN
    @etl_project.@etl_dataset.lk_micro_cross_ref cr
        ON src.trace_id = cr.trace_id_ab
WHERE
    src.ab_itemid IS NOT NULL
;

-- -------------------------------------------------------------------
-- lk_d_micro_clean
-- add resistance source codes to all microbiology source codes
-- source_label for organism: test name plus specimen name
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.lk_d_micro_clean AS
SELECT
    dm.itemid                                       AS itemid,
    CAST(dm.itemid AS STRING)                       AS source_code,
    dm.label                                        AS source_label, -- for organism_mapped: test name plus specimen name
    CONCAT('mimiciv_micro_', LOWER(dm.category))    AS source_vocabulary_id,
FROM
    @etl_project.@etl_dataset.src_d_micro dm
UNION ALL
SELECT DISTINCT
    CAST(NULL AS INT64)                             AS itemid,
    src.interpretation                              AS source_code,
    src.interpretation                              AS source_label,
    'mimiciv_micro_resistance'                      AS source_vocabulary_id
FROM
    @etl_project.@etl_dataset.lk_meas_ab_clean src
WHERE
    src.interpretation IS NOT NULL
;

-- -------------------------------------------------------------------
-- lk_d_micro_concept
--
--      gcpt_microbiology_specimen_to_concept -> mimiciv_micro_specimen
--          d_micro.label = mbe.spec_type_desc -> source_code
--      (gcpt) brand new vocab -> mimiciv_micro_microtest
--          d_micro.label = mbe.test_name -> source_code
--      gcpt_org_name_to_concept -> mimiciv_micro_organism
--          d_micro.label = mbe.org_name -> source_code
--      gcpt_atb_to_concept -> mimiciv_micro_antibiotic -- "susceptibility" Lab Test concepts
--          d_micro.label = mbe.ab_name -> source_code
--          https://athena.ohdsi.org/search-terms/terms?domain=Measurement&conceptClass=Lab+Test&page=1&pageSize=15&query=susceptibility 
--      (gcpt) brand new vocab -> mimiciv_micro_resistance -- loaded
--        src_microbiologyevents.interpretation -> source_code
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.lk_d_micro_concept AS
SELECT
    dm.itemid                   AS itemid,
    dm.source_code              AS source_code, -- itemid
    dm.source_label             AS source_label, -- symbolic information in case more mapping is required
    dm.source_vocabulary_id     AS source_vocabulary_id,
    -- source_concept
    vc.domain_id                AS source_domain_id,
    vc.concept_id               AS source_concept_id,
    vc.concept_name             AS source_concept_name,
    -- target concept
    vc2.vocabulary_id           AS target_vocabulary_id,
    vc2.domain_id               AS target_domain_id,
    vc2.concept_id              AS target_concept_id,
    vc2.concept_name            AS target_concept_name,
    vc2.standard_concept        AS target_standard_concept
FROM
    @etl_project.@etl_dataset.lk_d_micro_clean dm
LEFT JOIN
    @etl_project.@etl_dataset.voc_concept vc
        ON  dm.source_code = vc.concept_code
        -- gcpt_microbiology_specimen_to_concept -> mimiciv_micro_specimen
        -- (gcpt) brand new vocab -> mimiciv_micro_test
        -- gcpt_org_name_to_concept -> mimiciv_micro_organism
        -- (gcpt) brand new vocab -> mimiciv_micro_resistance
        AND vc.vocabulary_id = dm.source_vocabulary_id
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
-- lk_specimen_mapped
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.lk_specimen_mapped AS
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())           AS specimen_id,
    src.subject_id                              AS subject_id,
    COALESCE(src.hadm_id, hadm.hadm_id)         AS hadm_id,
    CAST(src.start_datetime AS DATE)            AS date_id,
    32856                                       AS type_concept_id, -- Lab
    src.start_datetime                          AS start_datetime,
    src.spec_itemid                             AS spec_itemid,
    mc.source_code                              AS source_code,
    mc.source_vocabulary_id                     AS source_vocabulary_id,
    mc.source_concept_id                        AS source_concept_id,
    COALESCE(mc.target_domain_id, 'Specimen')   AS target_domain_id,
    mc.target_concept_id                        AS target_concept_id,
    -- 
    src.unit_id                     AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    @etl_project.@etl_dataset.lk_specimen_clean src
INNER JOIN
    @etl_project.@etl_dataset.lk_d_micro_concept mc
        ON src.spec_itemid = mc.itemid
LEFT JOIN
    @etl_project.@etl_dataset.lk_micro_hadm_id hadm
        ON hadm.event_trace_id = src.trace_id
        AND hadm.row_num = 1
;

-- -------------------------------------------------------------------
-- lk_meas_organism_mapped
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.lk_meas_organism_mapped AS
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())           AS measurement_id,
    src.subject_id                              AS subject_id,
    COALESCE(src.hadm_id, hadm.hadm_id)         AS hadm_id,
    CAST(src.start_datetime AS DATE)            AS date_id,
    32856                                       AS type_concept_id, -- Lab
    src.start_datetime                          AS start_datetime,
    src.test_itemid                             AS test_itemid,
    src.spec_itemid                             AS spec_itemid,
    src.org_itemid                              AS org_itemid,
    CONCAT(tc.source_code, '|', sc.source_code)     AS source_code, -- test itemid plus specimen itemid
    tc.source_vocabulary_id                     AS source_vocabulary_id,
    tc.source_concept_id                        AS source_concept_id,
    COALESCE(tc.target_domain_id, 'Measurement')    AS target_domain_id,
    tc.target_concept_id                        AS target_concept_id,
    oc.source_code                              AS value_source_value,
    oc.target_concept_id                        AS value_as_concept_id,
    -- fields to link to specimen and test-organism
    src.trace_id_spec                           AS trace_id_spec,
    --
    src.unit_id                     AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    @etl_project.@etl_dataset.lk_meas_organism_clean src
INNER JOIN
    @etl_project.@etl_dataset.lk_d_micro_concept tc
        ON src.test_itemid = tc.itemid
INNER JOIN
    @etl_project.@etl_dataset.lk_d_micro_concept sc
        ON src.spec_itemid = sc.itemid
LEFT JOIN
    @etl_project.@etl_dataset.lk_d_micro_concept oc
        ON src.org_itemid = oc.itemid
LEFT JOIN
    @etl_project.@etl_dataset.lk_micro_hadm_id hadm
        ON hadm.event_trace_id = src.trace_id
        AND hadm.row_num = 1
;

-- -------------------------------------------------------------------
-- lk_meas_ab_mapped
--
--      (gcpt) brand new vocab -> mimiciv_micro_resistance
--          mbe.interpretation -> source_code -> 4 rows for resistance degrees.
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.lk_meas_ab_mapped AS
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())           AS measurement_id,
    src.subject_id                              AS subject_id,
    COALESCE(src.hadm_id, hadm.hadm_id)         AS hadm_id,
    CAST(src.start_datetime AS DATE)            AS date_id,
    32856                                       AS type_concept_id, -- Lab
    src.start_datetime                          AS start_datetime,
    src.ab_itemid                               AS ab_itemid,
    ac.source_code                              AS source_code,
    COALESCE(ac.target_concept_id, 0)           AS target_concept_id,
    COALESCE(ac.source_concept_id, 0)           AS source_concept_id,
    rc.target_concept_id                        AS value_as_concept_id,
    src.interpretation                          AS value_source_value,
    src.dilution_value                          AS value_as_number,
    src.dilution_comparison                     AS operator_source_value,
    opc.target_concept_id                       AS operator_concept_id,
    COALESCE(ac.target_domain_id, 'Measurement')    AS target_domain_id,
    -- fields to link test-organism and antibiotics
    src.trace_id_org                            AS trace_id_org,
    -- 
    src.unit_id                     AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    @etl_project.@etl_dataset.lk_meas_ab_clean src
INNER JOIN
    @etl_project.@etl_dataset.lk_d_micro_concept ac
        ON src.ab_itemid = ac.itemid
LEFT JOIN
    @etl_project.@etl_dataset.lk_d_micro_concept rc
        ON src.interpretation = rc.source_code
        AND rc.source_vocabulary_id = 'mimiciv_micro_resistance' -- new vocab
LEFT JOIN
    @etl_project.@etl_dataset.lk_meas_operator_concept opc -- see lk_meas_labevents.sql
        ON src.dilution_comparison = opc.source_code
LEFT JOIN
    @etl_project.@etl_dataset.lk_micro_hadm_id hadm
        ON hadm.event_trace_id = src.trace_id
        AND hadm.row_num = 1
;
