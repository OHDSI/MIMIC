-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Remove patients from person which have no records in observation_period
-- (DQD requirement)
-- 
-- Dependencies: run after 
--      person
--      observation_period
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- person
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.tmp_person AS
SELECT per.*
FROM 
    @etl_project.@etl_dataset.person per
INNER JOIN
    @etl_project.@etl_dataset.observation_period op
        ON  per.person_id = op.person_id
;

TRUNCATE TABLE @etl_project.@etl_dataset.person;

INSERT INTO @etl_project.@etl_dataset.person
SELECT per.*
FROM
    @etl_project.@etl_dataset.tmp_person per
;

DROP TABLE IF EXISTS @etl_project.@etl_dataset.tmp_person;
