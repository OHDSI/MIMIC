-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Populate cdm_provider table
-- 
-- Dependencies: run after st_core.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- MIMIC IV does not contain table "caregivers" which was used in MIMIC III OMOP
-- Find replacement or leave cdm_provider empty
-- -------------------------------------------------------------------

-- MIMIC III code for reference:

-- WITH caregivers AS 
-- (
--  SELECT 
--      mimic_id as provider_id, 
--      label as provider_source_value, 
--      description as specialty_source_value 
--  FROM caregivers
-- )
-- INSERT INTO :OMOP_SCHEMA.PROVIDER
-- (
--   provider_id
--  , provider_source_value
--  , specialty_source_value
-- )
-- SELECT caregivers.provider_id, caregivers.provider_source_value, caregivers.specialty_source_value
-- FROM caregivers;
