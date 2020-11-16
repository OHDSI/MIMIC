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
--      gcpt_microbiology_specimen_to_concept -> mimiciv_micro_specimen -- to load
--          d_micro.label = mbe.spec_type_desc -> source_code
--      gcpt_org_name_to_concept -> mimiciv_micro_organism -- to load
--          d_micro.label = mbe.org_name -> source_code
--      (gcpt) brand new vocab -> mimiciv_micro_microtest -- to create
--          d_micro.label = mbe.test_name -> source_code
--      gcpt_atb_to_concept -> mimiciv_micro_antibiotic -- to re-map to "susceptibility" Lab Test concepts
--          d_micro.label = mbe.ab_name -> source_code
--          https://athena.ohdsi.org/search-terms/terms?domain=Measurement&conceptClass=Lab+Test&page=1&pageSize=15&query=susceptibility 
--      (gcpt) brand new vocab -> mimiciv_micro_resistance -- to create
--          mbe.interpretation -> source_code -> 4 rows for resistance degrees.
--
-- lk_meas_organism_mapped and lk_meas_ab_mapped:
--      provide measurement_id to use in fact_relationship
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Part 2 of microbiology: test taken and organisms grown in the material of the test
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_meas_organism_clean AS
SELECT
    src.subject_id                              AS subject_id,
    src.hadm_id                                 AS hadm_id,
    src.spec_itemid                             AS spec_itemid, -- d_micro.itemid, link back to the specimen taken
    src.test_itemid                             AS test_itemid, -- d_micro.itemid, test taken from the specimen
    COALESCE(src.charttime, src.chartdate)      AS start_datetime,
    src.org_itemid                              AS org_itemid, -- d_micro.itemid, organism which has grown
    -- 
    'micro.organism'                AS unit_id,
    src.load_table_id               AS load_table_id,
    0                               AS load_row_id,
    MIN(src.trace_id)               AS trace_id
FROM
    `@etl_project`.@etl_dataset.src_microbiologyevents src -- mbe
GROUP BY
    src.subject_id,
    src.hadm_id,
    src.spec_itemid,
    src.test_itemid,
    COALESCE(src.charttime, src.chartdate),
    src.org_itemid,
    src.load_table_id
;

-- -------------------------------------------------------------------
-- Part 1 of microbiology: specimen
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_specimen_clean AS
SELECT
    src.subject_id                              AS subject_id,
    src.start_datetime                          AS start_datetime,
    src.spec_itemid                             AS spec_itemid, -- d_micro.itemid, type of specimen taken
    -- 
    'micro.specimen'                AS unit_id,
    src.load_table_id               AS load_table_id,
    0                               AS load_row_id,
    MIN(src.trace_id)               AS trace_id
FROM
    `@etl_project`.@etl_dataset.lk_meas_organism_clean src -- mbe
GROUP BY
    src.subject_id,
    src.start_datetime,
    src.spec_itemid,
    src.load_table_id
;

-- -------------------------------------------------------------------
-- Part 3 of microbiology: antibiotics tested on organisms
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_meas_ab_clean AS
SELECT
    src.subject_id                              AS subject_id,
    src.hadm_id                                 AS hadm_id,
    src.spec_itemid                             AS spec_itemid, -- d_micro.itemid, link back to the specimen taken
    src.test_itemid                             AS test_itemid, -- d_micro.itemid, test taken from the specimen
    COALESCE(src.charttime, src.chartdate)      AS start_datetime,
    src.org_itemid                              AS org_itemid, -- d_micro.itemid, organism which has grown
    src.ab_itemid                               AS ab_itemid, -- antibiotic tested
    src.dilution_comparison                     AS dilution_comparison, -- operator sign
    src.dilution_value                          AS dilution_value, -- numeric dilution value
    src.interpretation                          AS interpretation, -- degree of resistance
    -- 
    'micro.antibiotics'             AS unit_id,
    src.load_table_id               AS load_table_id,
    0                               AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    `@etl_project`.@etl_dataset.src_microbiologyevents src
WHERE
    src.ab_itemid IS NOT NULL
;

-- -------------------------------------------------------------------
-- lk_d_micro_concept
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_d_micro_concept AS
SELECT
    dm.itemid           AS itemid,
    dm.label            AS source_code, -- gcpt.label
    CONCAT('mimiciv_micro_', LOWER(dm.category))    AS source_vocabulary_id,
    vc.domain_id        AS source_domain_id,
    vc.concept_id       AS source_concept_id,
    vc2.domain_id       AS target_domain_id,
    vc2.concept_id      AS target_concept_id
FROM
    `@etl_project`.@etl_dataset.src_d_micro dm
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept vc
        ON  dm.label = vc.concept_code
        -- gcpt_microbiology_specimen_to_concept -> mimiciv_micro_specimen
        -- gcpt_org_name_to_concept -> mimiciv_micro_organism
        -- (gcpt) brand new vocab -> mimiciv_micro_test
        AND vc.vocabulary_id = CONCAT('mimiciv_micro_', LOWER(dm.category))
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

-- INSERT INTO `@etl_project`.@etl_dataset.lk_d_micro_concept
-- (resistance mapping)


-- -------------------------------------------------------------------
-- Rule 2 specimen from labevents (fake?)
-- -------------------------------------------------------------------


-- -------------------------------------------------------------------
-- Rule 3 specimen from chartevents (fake?)
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- lk_specimen_mapped
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_specimen_mapped AS
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())           AS specimen_id,
    src.subject_id                              AS subject_id,
    COALESCE(mc.target_concept_id, 0)           AS target_concept_id,
    src.start_datetime                          AS start_datetime,
    mc.source_code                              AS source_code,
    COALESCE(mc.target_domain_id, 'Specimen')   AS target_domain_id,
    -- 
    src.unit_id                     AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    `@etl_project`.@etl_dataset.lk_specimen_clean src
INNER JOIN
    `@etl_project`.@etl_dataset.lk_d_micro_concept mc
        ON src.spec_itemid = mc.itemid
;

-- -------------------------------------------------------------------
-- lk_meas_organism_mapped
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_meas_organism_mapped AS
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())           AS measurement_id,
    src.subject_id                              AS subject_id,
    src.hadm_id                                 AS hadm_id,
    0                                           AS type_concept_id,
    src.start_datetime                          AS start_datetime,
    CONCAT(tc.source_code, ' | ', sc.source_code)   AS source_code, -- test name plus specimen name
    COALESCE(tc.target_concept_id, 0)           AS target_concept_id,
    COALESCE(tc.source_concept_id, 0)           AS source_concept_id,
    oc.source_code                              AS value_source_value,
    oc.target_concept_id                        AS value_as_concept_id,
    COALESCE(tc.target_domain_id, 'Measurement')    AS target_domain_id,
    -- fields to link to specimen and ab meas
    src.spec_itemid                             AS spec_itemid,
    src.test_itemid                             AS test_itemid,
    src.org_itemid                              AS org_itemid,
    -- 
    src.unit_id                     AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    `@etl_project`.@etl_dataset.lk_meas_organism_clean src
INNER JOIN
    `@etl_project`.@etl_dataset.lk_d_micro_concept tc
        ON src.test_itemid = tc.itemid
INNER JOIN
    `@etl_project`.@etl_dataset.lk_d_micro_concept sc
        ON src.spec_itemid = sc.itemid
LEFT JOIN
    `@etl_project`.@etl_dataset.lk_d_micro_concept oc
        ON src.org_itemid = oc.itemid
;

-- -------------------------------------------------------------------
-- lk_meas_ab_mapped
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_meas_ab_mapped AS
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())           AS measurement_id,
    src.subject_id                              AS subject_id,
    src.hadm_id                                 AS hadm_id,
    0                                           AS type_concept_id,
    src.start_datetime                          AS start_datetime,
    ac.source_code                              AS source_code,
    COALESCE(ac.target_concept_id, 0)           AS target_concept_id,
    COALESCE(ac.source_concept_id, 0)           AS source_concept_id,
    rc.target_concept_id                        AS value_as_concept_id,
    src.interpretation                          AS value_source_value,
    src.dilution_value                          AS value_as_number,
    src.dilution_comparison                     AS operator_source_value,
    opc.target_concept_id                       AS operator_concept_id,
    COALESCE(ac.target_domain_id, 'Measurement')    AS target_domain_id,
    -- fields to link to specimen and ab meas
    src.spec_itemid                             AS spec_itemid,
    src.test_itemid                             AS test_itemid,
    src.org_itemid                              AS org_itemid,
    -- 
    src.unit_id                     AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    `@etl_project`.@etl_dataset.lk_meas_ab_clean src
INNER JOIN
    `@etl_project`.@etl_dataset.lk_d_micro_concept ac
        ON src.ab_itemid = ac.itemid
LEFT JOIN
    `@etl_project`.@etl_dataset.lk_d_micro_concept rc
        ON src.interpretation = rc.source_code
        AND rc.source_vocabulary_id = 'mimiciv_micro_resistance' -- gcpt_?
LEFT JOIN
    `@etl_project`.@etl_dataset.lk_meas_operator_concept opc -- see lk_meas_labevents.sql
        ON src.dilution_comparison = opc.source_code
;
