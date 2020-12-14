-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Metrics / statistics for target tables
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- cdm_measurement
-- -------------------------------------------------------------------

-- todo: mapping rate against the whole table (count in qa/metric_total_row_count.sql?)

-- mapped: 87335 / unmapped: 18211
SELECT
    'measurement' AS table_id,
    unit_id AS unit_id,
    COUNT(CASE WHEN measurement_concept_id = 0 THEN NULL ELSE measurement_concept_id END) AS rows_mapped,
    COUNT(CASE WHEN measurement_concept_id = 0 THEN 0 ELSE NULL END) AS rows_unmapped,
    COUNT(*) AS rows_total,
    COUNT(CASE WHEN measurement_concept_id = 0 THEN NULL ELSE measurement_concept_id END) / COUNT(*) AS mapping_rate 
FROM `odysseus-mimic-dev.mimiciv_cdm_tuf_10_ant_2020_10_21.lk_measurement_rule_1`
GROUP BY unit_id
;
