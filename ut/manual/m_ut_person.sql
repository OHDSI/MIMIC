-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Check UT violation for cdm_person table
-- -------------------------------------------------------------------

SELECT DISTINCT
    cdm.gender_source_value, cdm.gender_concept_id, vc.concept_code, vc.standard_concept, vc.invalid_reason
FROM
    `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.cdm_person cdm
LEFT JOIN
    `odysseus-mimic-dev`.mimiciv_cdm_tuf_10_ant_2020_09_11.voc_concept vc
        ON cdm.gender_concept_id = vc.concept_id
        AND vc.standard_concept = 'S'
WHERE
    cdm.gender_concept_id <> 0
;
