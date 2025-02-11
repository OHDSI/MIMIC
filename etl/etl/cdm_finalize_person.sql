-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Remove patients from cdm_person which have no records in cdm_observation_period 
-- (DQD requirement)
-- 
-- Dependencies: run after 
--      cdm_person
--      cdm_observation_period
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- cdm_person
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.tmp_person AS
SELECT per.*
FROM 
    @etl_project.@etl_dataset.cdm_person per
INNER JOIN
    @etl_project.@etl_dataset.cdm_observation_period op
        ON  per.person_id = op.person_id
;

TRUNCATE TABLE @etl_project.@etl_dataset.cdm_person;

INSERT INTO @etl_project.@etl_dataset.cdm_person
SELECT per.*
FROM
    @etl_project.@etl_dataset.tmp_person per
;

DROP TABLE IF EXISTS @etl_project.@etl_dataset.tmp_person;
