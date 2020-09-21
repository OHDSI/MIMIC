-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -----------------------------------------------------------------------------------
-- 2020-09-19
-- TUF-11, Load vocabularies to `odysseus-mimic-dev.vocabulary_2020_09_11`
-- -----------------------------------------------------------------------------------

+----------+--------------------------------------------------------------------------+-----------+
| check_id |                                check_name                                | row_count |
+----------+--------------------------------------------------------------------------+-----------+
|        7 | wrong valid_start_date, valid_end_date or invalid_reason for the concept |      4271 |
+----------+--------------------------------------------------------------------------+-----------+


SELECT c.vocabulary_id, count(*) as cnt
FROM `@bq_target_project.@bq_target_dataset`.z_check_voc_5 ch
INNER JOIN `@bq_target_project.@bq_target_dataset`.concept c
    ON  ch.concept_id_1 = c.concept_id -- concept_id_1 
GROUP BY c.vocabulary_id
ORDER BY c.vocabulary_id
;

SELECT c.vocabulary_id, count(*) as cnt
FROM `@bq_target_project.@bq_target_dataset`.z_check_voc_7 ch
INNER JOIN `@bq_target_project.@bq_target_dataset`.concept c
    ON  ch.concept_id = c.concept_id -- concept_id (todo: unification)
GROUP BY c.vocabulary_id
ORDER BY c.vocabulary_id
;

SELECT c.vocabulary_id, count(*) as cnt
FROM `@bq_target_project.@bq_target_dataset`.z_check_voc_12 ch
INNER JOIN `@bq_target_project.@bq_target_dataset`.concept c
    ON  ch.concept_id_1 = c.concept_id
GROUP BY c.vocabulary_id
ORDER BY c.vocabulary_id
;

