-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Populate cdm_observation table
-- 
-- Dependencies: run after 
--      lk_meas_specimen
-- -------------------------------------------------------------------


CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_fact_relationship
(
    domain_concept_id_1     INT64     not null ,
    fact_id_1               INT64     not null ,
    domain_concept_id_2     INT64     not null ,
    fact_id_2               INT64     not null ,
    relationship_concept_id INT64     not null ,
    -- 
    unit_id                       STRING  
)
;

-- -------------------------------------------------------------------
-- specimen to test-organism
-- -------------------------------------------------------------------

INSERT INTO @etl_project.@etl_dataset.cdm_fact_relationship
SELECT
    36                      AS domain_concept_id_1, -- Specimen
    spec.specimen_id        AS fact_id_1,
    21                      AS domain_concept_id_2, -- Measurement
    org.measurement_id      AS fact_id_2,
    32669                   AS relationship_concept_id, -- Specimen to Measurement   Standard
    'fact.spec.test'        AS unit_id
FROM
    @etl_project.@etl_dataset.lk_specimen_mapped spec
INNER JOIN
    @etl_project.@etl_dataset.lk_meas_organism_mapped org
        ON org.trace_id_spec = spec.trace_id
;

INSERT INTO @etl_project.@etl_dataset.cdm_fact_relationship
SELECT
    21                      AS domain_concept_id_1, -- Measurement
    org.measurement_id      AS fact_id_1,
    36                      AS domain_concept_id_2, -- Specimen
    spec.specimen_id        AS fact_id_2,
    32668                   AS relationship_concept_id, -- Measurement to Specimen   Standard
    'fact.test.spec'        AS unit_id
FROM
    @etl_project.@etl_dataset.lk_specimen_mapped spec
INNER JOIN
    @etl_project.@etl_dataset.lk_meas_organism_mapped org
        ON org.trace_id_spec = spec.trace_id
;

-- -------------------------------------------------------------------
-- test-organism to antibiotic
-- -------------------------------------------------------------------

INSERT INTO @etl_project.@etl_dataset.cdm_fact_relationship
SELECT
    21                      AS domain_concept_id_1, -- Measurement
    org.measurement_id      AS fact_id_1,
    21                      AS domain_concept_id_2, -- Measurement
    ab.measurement_id       AS fact_id_2,
    581436                  AS relationship_concept_id, -- Parent to Child Measurement   Standard
    'fact.test.ab'          AS unit_id
FROM
    @etl_project.@etl_dataset.lk_meas_organism_mapped org
INNER JOIN
    @etl_project.@etl_dataset.lk_meas_ab_mapped ab
        ON ab.trace_id_org = org.trace_id
;

INSERT INTO @etl_project.@etl_dataset.cdm_fact_relationship
SELECT
    21                      AS domain_concept_id_1, -- Measurement
    ab.measurement_id       AS fact_id_1,
    21                      AS domain_concept_id_2, -- Measurement
    org.measurement_id      AS fact_id_2,
    581437                  AS relationship_concept_id, -- Child to Parent Measurement   Standard
    'fact.ab.test'          AS unit_id
FROM
    @etl_project.@etl_dataset.lk_meas_organism_mapped org
INNER JOIN
    @etl_project.@etl_dataset.lk_meas_ab_mapped ab
        ON ab.trace_id_org = org.trace_id
;
