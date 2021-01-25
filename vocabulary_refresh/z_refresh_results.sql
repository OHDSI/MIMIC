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


-- -----------------------------------------------------------------------------------
-- 2020-10-19
-- TUF-55, Load custom mapping
-- -----------------------------------------------------------------------------------

+----------+--------------------------------------------------------------------------+-----------+
| check_id |                                check_name                                | row_count |
+----------+--------------------------------------------------------------------------+-----------+
|        5 | wrong relationships: Maps to TO D OR U or replacement relationships TO D |        26 |
|        7 | wrong valid_start_date, valid_end_date or invalid_reason for the concept |      4271 |
|       12 | wrong relationships: Maps to/Maps to value not to S                      |        32 |
|       15 | nonexistent classes                                                      |         1 |
|       16 | nonexistent domains                                                      |         1 |
+----------+--------------------------------------------------------------------------+-----------+

--       5 | wrong relationships: mimiciv_drug_ndc needs to be re-mapped              |        26 |
--       7 | ICD10PSC, invalid_reason is null, and valid_end_date = 2019-12-31        |      4271 |
--      12 | wrong relationships: mimiciv_drug_ndc needs to be re-mapped              |        32 |
--      15 | nonexistent classes - mimiciv_marital_status - fixed
--      16 | nonexistent domains - mimiciv_marital_status - fixed

-- -----------------------------------------------------------------------------------
-- 2020-11-18
-- TUF-55, Load custom mapping for microbiology and more
-- -----------------------------------------------------------------------------------

+----------+--------------------------------------------------------------------------+-----------+
| check_id |                                check_name                                | row_count |
+----------+--------------------------------------------------------------------------+-----------+
|        5 | wrong relationships: Maps to TO D OR U or replacement relationships TO D |        26 |
|        7 | wrong valid_start_date, valid_end_date or invalid_reason for the concept |      4271 |
|       12 | wrong relationships: Maps to/Maps to value not to S                      |        32 |
+----------+--------------------------------------------------------------------------+-----------+

-- -----------------------------------------------------------------------------------
-- 2020-11-18
-- TUF-55, Load custom mapping for microbiology and more
-- -----------------------------------------------------------------------------------

+----------+--------------------------------------------------------------------------+-----------+
| check_id |                                check_name                                | row_count |
+----------+--------------------------------------------------------------------------+-----------+
|        5 | wrong relationships: Maps to TO D OR U or replacement relationships TO D |        27 |
|        7 | wrong valid_start_date, valid_end_date or invalid_reason for the concept |      4271 |
|       12 | wrong relationships: Maps to/Maps to value not to S                      |        36 |
|       16 | nonexistent domains                                                      |         1 |
+----------+--------------------------------------------------------------------------+-----------+

-- |       16 | nonexistent domains - fixed, will be applied at the next loading         |         1 |

-- -----------------------------------------------------------------------------------
-- 2020-12-10
-- TUF-??, Load custom mapping for after Full set Metrics report
-- -----------------------------------------------------------------------------------

+----------+--------------------------------------------------------------------------+-----------+
| check_id |                                check_name                                | row_count |
+----------+--------------------------------------------------------------------------+-----------+
|        5 | wrong relationships: Maps to TO D OR U or replacement relationships TO D |        29 |
|        7 | wrong valid_start_date, valid_end_date or invalid_reason for the concept |      4271 |
|       12 | wrong relationships: Maps to/Maps to value not to S                      |        38 |
+----------+--------------------------------------------------------------------------+-----------+

-- -----------------------------------------------------------------------------------
-- 2021-01-20
-- Load custom mapping added by comments in DQD error report
-- -----------------------------------------------------------------------------------

+----------+--------------------------------------------------------------------------+-----------+
| check_id |                                check_name                                | row_count |
+----------+--------------------------------------------------------------------------+-----------+
|        5 | wrong relationships: Maps to TO D OR U or replacement relationships TO D |        57 | +28
|        7 | wrong valid_start_date, valid_end_date or invalid_reason for the concept |      4271 |
|       12 | wrong relationships: Maps to/Maps to value not to S                      |        73 | +35
+----------+--------------------------------------------------------------------------+-----------+

-- -----------------------------------------------------------------------------------
-- 2021-01-21
-- Fix and load again: custom mapping added by comments in DQD error report
-- -----------------------------------------------------------------------------------

+----------+--------------------------------------------------------------------------+-----------+
| check_id |                                check_name                                | row_count |
+----------+--------------------------------------------------------------------------+-----------+
|        5 | wrong relationships: Maps to TO D OR U or replacement relationships TO D |        28 | -1
|        7 | wrong valid_start_date, valid_end_date or invalid_reason for the concept |      4271 |
|       12 | wrong relationships: Maps to/Maps to value not to S                      |        36 | -2
+----------+--------------------------------------------------------------------------+-----------+

-- -----------------------------------------------------------------------------------
-- 2021-01-25
-- Re-mapped by comments in DQD error report
-- -----------------------------------------------------------------------------------

+----------+--------------------------------------------------------------------------+-----------+
| check_id |                                check_name                                | row_count |
+----------+--------------------------------------------------------------------------+-----------+
|        7 | wrong valid_start_date, valid_end_date or invalid_reason for the concept |      4271 |
+----------+--------------------------------------------------------------------------+-----------+


-- -----------------------------------------------------------------------------------

SELECT DISTINCT
    12 check_id,
    'wrong relationships: "Maps to"/"Maps to value" not to "S"' AS check_name,
    c1.vocabulary_id AS source_vocabulary_id,
    c1.concept_id AS source_concept_id,
    c1.concept_code AS source_concept_code,
    r.relationship_id AS relationship_id,
    c2.concept_id AS target_concept_id,
    c2.concept_code AS target_concept_code,
    c2.concept_name AS target_concept_name,
    c2.invalid_reason AS target_invalid_reason,
    c2.valid_end_date AS target_valid_end_date
FROM 
    `odysseus-mimic-dev`.vocabulary_2020_09_11.concept c1,
    `odysseus-mimic-dev`.vocabulary_2020_09_11.concept c2,
    `odysseus-mimic-dev`.vocabulary_2020_09_11.concept_relationship r
WHERE 
    c1.concept_id = r.concept_id_1
    AND c2.concept_id = r.concept_id_2
    AND coalesce(c2.standard_concept,'C')<>'S'
    AND r.relationship_id IN ('Maps to','Maps to value')
    AND r.invalid_reason IS NULL
ORDER BY source_vocabulary_id, source_concept_id
;
