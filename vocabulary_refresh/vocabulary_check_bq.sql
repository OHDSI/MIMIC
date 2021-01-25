-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- Verify vocabularies
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- The script is provided by Vocabulary team.
-- Initial name is checks.sql.
-- Prefix "voc_" is added to all vocab tables.
--
-- Run the script in the vocabulary scheme
-- -------------------------------------------------------------------

--relationships cycle
DROP TABLE IF EXISTS `@bq_target_project.@bq_target_dataset`.z_check_voc_1;
CREATE TABLE `@bq_target_project.@bq_target_dataset`.z_check_voc_1
AS
SELECT
    1 check_id,
    'relationships cycle' AS check_name,
    r.*
FROM `@bq_target_project.@bq_target_dataset`.concept_relationship r,
    `@bq_target_project.@bq_target_dataset`.concept_relationship r_int
WHERE r.invalid_reason IS NULL
    AND r_int.concept_id_1 = r.concept_id_2
    AND r_int.concept_id_2 = r.concept_id_1
    AND r.concept_id_1 <> r.concept_id_2
    AND r_int.relationship_id = r.relationship_id
    AND r_int.invalid_reason IS NULL
;

--opposing relationships between same pair of concepts
DROP TABLE IF EXISTS `@bq_target_project.@bq_target_dataset`.z_check_voc_2;
CREATE TABLE `@bq_target_project.@bq_target_dataset`.z_check_voc_2
AS
SELECT 
    2 check_id,
    'opposing relationships between same pair of concepts' AS check_name,
    r.*
FROM `@bq_target_project.@bq_target_dataset`.concept_relationship r,
    `@bq_target_project.@bq_target_dataset`.concept_relationship r_int,
    `@bq_target_project.@bq_target_dataset`.relationship rel
WHERE r.invalid_reason IS NULL
    AND r.relationship_id = rel.relationship_id
    AND r_int.concept_id_1 = r.concept_id_1
    AND r_int.concept_id_2 = r.concept_id_2
    AND r.concept_id_1 <> r.concept_id_2
    AND r_int.relationship_id = rel.reverse_relationship_id
    AND r_int.invalid_reason IS NULL
;

--relationships without reverse
DROP TABLE IF EXISTS `@bq_target_project.@bq_target_dataset`.z_check_voc_3;
CREATE TABLE `@bq_target_project.@bq_target_dataset`.z_check_voc_3
AS
SELECT 
    3 check_id,
    'relationships without reverse' AS check_name,
    r.*
FROM `@bq_target_project.@bq_target_dataset`.concept_relationship r,
    `@bq_target_project.@bq_target_dataset`.relationship rel
WHERE r.relationship_id = rel.relationship_id
    AND NOT EXISTS (
        SELECT 1
        FROM `@bq_target_project.@bq_target_dataset`.concept_relationship r_int
        WHERE r_int.relationship_id = rel.reverse_relationship_id
            AND r_int.concept_id_1 = r.concept_id_2
            AND r_int.concept_id_2 = r.concept_id_1
        )
;

-- What is check_id = 4?
-- Skip check_id = 5, since its result is part of check_id = 12 (map to non-standard)
--wrong relationships: 'Maps to' to 'D' or 'U' or replacement relationships to 'D'


--direct and reverse mappings are not same
DROP TABLE IF EXISTS `@bq_target_project.@bq_target_dataset`.z_check_voc_6;
CREATE TABLE `@bq_target_project.@bq_target_dataset`.z_check_voc_6
AS
SELECT 
    6 check_id,
    'direct and reverse mappings are not same' AS check_name,
    r.*
FROM `@bq_target_project.@bq_target_dataset`.concept_relationship r,
    `@bq_target_project.@bq_target_dataset`.relationship rel,
    `@bq_target_project.@bq_target_dataset`.concept_relationship r_int
WHERE r.relationship_id = rel.relationship_id
    AND r_int.relationship_id = rel.reverse_relationship_id
    AND r_int.concept_id_1 = r.concept_id_2
    AND r_int.concept_id_2 = r.concept_id_1
    AND (
        r.valid_end_date <> r_int.valid_end_date
        OR COALESCE(r.invalid_reason, 'X') <> COALESCE(r_int.invalid_reason, 'X')
        )
;


--wrong valid_start_date, valid_end_date or invalid_reason for the concept
DROP TABLE IF EXISTS `@bq_target_project.@bq_target_dataset`.z_check_voc_7;
CREATE TABLE `@bq_target_project.@bq_target_dataset`.z_check_voc_7
AS
SELECT 
    7 check_id,
    'wrong valid_start_date, valid_end_date or invalid_reason for the concept' AS check_name,
    c.concept_id,
    NULL AS concept_id_2, 
    c.vocabulary_id,
    c.valid_start_date,
    c.valid_end_date,
    c.invalid_reason
FROM `@bq_target_project.@bq_target_dataset`.concept c
-- JOIN vocabulary_conversion vc ON vc.vocabulary_id_v5 = c.vocabulary_id
-- table vocabulary_conversion is absent FROM a usual vocab package
WHERE (
        c.valid_end_date < c.valid_start_date
        OR (
            c.valid_end_date = PARSE_DATE('%Y%m%d', '20991231')
            AND c.invalid_reason IS NOT NULL
            )
        OR (
            c.valid_end_date <> PARSE_DATE('%Y%m%d', '20991231')
            AND c.invalid_reason IS NULL
            AND c.vocabulary_id NOT IN (
                'CPT4',
                'HCPCS',
                'ICD9Proc'
                )
            )
        -- OR c.valid_start_date > DATE_ADD(COALESCE(vc.latest_update, CURRENT_DATE()), INTERVAL 15 YEAR) --some concepts might be FROM near future (e.g. GGR, HCPCS) [AVOF-1015]/increased 20180928 for some NDC concepts
        OR c.valid_start_date < PARSE_DATE('%Y%m%d', '19000101') -- some concepts have a real date < 1970
        )
;

--wrong valid_start_date, valid_end_date or invalid_reason for the voc_concept_relationship
DROP TABLE IF EXISTS `@bq_target_project.@bq_target_dataset`.z_check_voc_8;
CREATE TABLE `@bq_target_project.@bq_target_dataset`.z_check_voc_8
AS
SELECT 
    8 check_id,
    'wrong valid_start_date, valid_end_date or invalid_reason for the voc_concept_relationship' AS check_name,
    s0.concept_id_1,
    s0.concept_id_2,
    s0.relationship_id,
    s0.valid_start_date,
    s0.valid_end_date,
    s0.invalid_reason
FROM (
    SELECT r.*,
        CASE 
            WHEN (
                    r.valid_end_date = PARSE_DATE('%Y%m%d', '20991231')
                    AND r.invalid_reason IS NOT NULL
                    )
                OR (
                    r.valid_end_date <> PARSE_DATE('%Y%m%d', '20991231')
                    AND r.invalid_reason IS NULL
                    )
                OR r.valid_start_date > CURRENT_DATE
                OR r.valid_start_date < PARSE_DATE('%Y%m%d', '19700101')
                THEN 1
            ELSE 0
            END check_flag
    FROM `@bq_target_project.@bq_target_dataset`.concept_relationship r
    ) AS s0
WHERE check_flag = 1
;

--RxE to Rx name duplications
DROP TABLE IF EXISTS `@bq_target_project.@bq_target_dataset`.z_check_voc_9;
CREATE TABLE `@bq_target_project.@bq_target_dataset`.z_check_voc_9
AS
SELECT 
    9 check_id,
    'RxE to Rx name duplications' AS check_name,
    c2.concept_id as c2_concept_id,
    c1.concept_id as c1_concept_id,
    'Concept replaced by' AS relationship_id,
    NULL AS valid_start_date,
    NULL AS valid_end_date,
    NULL AS invalid_reason
FROM `@bq_target_project.@bq_target_dataset`.concept c1
JOIN `@bq_target_project.@bq_target_dataset`.concept c2 ON upper(c2.concept_name) = upper(c1.concept_name)
    AND c2.concept_class_id = c1.concept_class_id
    AND c2.vocabulary_id = 'RxNorm Extension'
    AND c2.invalid_reason IS NULL
WHERE c1.vocabulary_id = 'RxNorm'
    AND c1.standard_concept = 'S'
;

--one concept has multiple replaces
DROP TABLE IF EXISTS `@bq_target_project.@bq_target_dataset`.z_check_voc_10;
CREATE TABLE `@bq_target_project.@bq_target_dataset`.z_check_voc_10
AS
SELECT 
    10 check_id,
    'one concept has multiple replaces' AS check_name,
    r.*
FROM `@bq_target_project.@bq_target_dataset`.concept_relationship r
INNER JOIN
    (
        SELECT r_int.concept_id_1,
            r_int.relationship_id
        FROM `@bq_target_project.@bq_target_dataset`.concept_relationship r_int
        WHERE r_int.relationship_id IN (
                'Concept replaced by',
                'Concept same_as to',
                'Concept alt_to to',
                'Concept poss_eq to',
                'Concept was_a to'
                )
            AND r_int.invalid_reason IS NULL
        GROUP BY r_int.concept_id_1,
            r_int.relationship_id
        HAVING COUNT(*) > 1
    ) r_dup
    ON  r.concept_id_1 = r_dup.concept_id_1
    AND r.relationship_id = r_dup.relationship_id

;

--wrong concept_name [AVOF-1438]
DROP TABLE IF EXISTS `@bq_target_project.@bq_target_dataset`.z_check_voc_11;
CREATE TABLE `@bq_target_project.@bq_target_dataset`.z_check_voc_11
AS
SELECT 
    11 check_id,
    'wrong concept_name ("OMOP generated", but should be OMOPxxx)' AS check_name,
    c.concept_id,
    -- NULL,
    c.vocabulary_id,
    c.valid_start_date,
    c.valid_end_date,
    c.invalid_reason
FROM `@bq_target_project.@bq_target_dataset`.concept c
WHERE c.domain_id <> 'Metadata'
    AND c.concept_code = 'OMOP generated'
;

--wrong relationships: 'Maps to'/'Maps to value' not to 'S'
DROP TABLE IF EXISTS `@bq_target_project.@bq_target_dataset`.z_check_voc_12;
CREATE TABLE `@bq_target_project.@bq_target_dataset`.z_check_voc_12
AS
SELECT 
    12 check_id,
    'wrong relationships: "Maps to"/"Maps to value" not to "S"' AS check_name,
    r.*
FROM `@bq_target_project.@bq_target_dataset`.concept c2,
    `@bq_target_project.@bq_target_dataset`.concept_relationship r
WHERE c2.concept_id = r.concept_id_2
    AND coalesce(c2.standard_concept,'C')<>'S'
    AND r.relationship_id IN ('Maps to','Maps to value')
    AND r.invalid_reason IS NULL
;

--wrong relationships: 'Maps to'/'Maps to value' not FROM 'C' or NULL (unless it is to self)
DROP TABLE IF EXISTS `@bq_target_project.@bq_target_dataset`.z_check_voc_13;
CREATE TABLE `@bq_target_project.@bq_target_dataset`.z_check_voc_13
AS
SELECT 
    13 check_id,
    'wrong relationships: "Maps to"/"Maps to value" not FROM "C" or NULL (unless it is to self)' AS check_name,
    r.*
FROM `@bq_target_project.@bq_target_dataset`.concept c1,
    `@bq_target_project.@bq_target_dataset`.concept_relationship r
WHERE c1.concept_id = r.concept_id_1
    AND coalesce(c1.standard_concept,'C')='S'
    AND r.relationship_id IN ('Maps to','Maps to value')
    AND r.invalid_reason IS NULL
    AND r.concept_id_1<>r.concept_id_2
;

--nonexistent relationships, classes, domains and vocabularies
DROP TABLE IF EXISTS `@bq_target_project.@bq_target_dataset`.z_check_voc_14;
CREATE TABLE `@bq_target_project.@bq_target_dataset`.z_check_voc_14
AS
SELECT 
    14 check_id,
    'nonexistent relationships' AS check_name,
    relationship_id 
FROM
(
    SELECT relationship_id FROM `@bq_target_project.@bq_target_dataset`.concept_relationship
    EXCEPT DISTINCT
    SELECT relationship_id FROM `@bq_target_project.@bq_target_dataset`.relationship
);

DROP TABLE IF EXISTS `@bq_target_project.@bq_target_dataset`.z_check_voc_15;
CREATE TABLE `@bq_target_project.@bq_target_dataset`.z_check_voc_15
AS
SELECT 
    15 check_id,
    'nonexistent classes' AS check_name,
    concept_class_id
FROM
(
    SELECT concept_class_id FROM `@bq_target_project.@bq_target_dataset`.concept
    EXCEPT DISTINCT
    SELECT concept_class_id FROM `@bq_target_project.@bq_target_dataset`.concept_class
);

DROP TABLE IF EXISTS `@bq_target_project.@bq_target_dataset`.z_check_voc_16;
CREATE TABLE `@bq_target_project.@bq_target_dataset`.z_check_voc_16
AS
SELECT 
    16 check_id,
    'nonexistent domains' AS check_name,
    domain_id
FROM
(
    SELECT domain_id FROM `@bq_target_project.@bq_target_dataset`.concept
    EXCEPT DISTINCT
    SELECT domain_id FROM `@bq_target_project.@bq_target_dataset`.domain
);

DROP TABLE IF EXISTS `@bq_target_project.@bq_target_dataset`.z_check_voc_17;
CREATE TABLE `@bq_target_project.@bq_target_dataset`.z_check_voc_17
AS
SELECT 
    17 check_id,
    'nonexistent vocabularies' AS check_name,
    vocabulary_id
FROM
(
    SELECT vocabulary_id FROM `@bq_target_project.@bq_target_dataset`.concept
    EXCEPT DISTINCT
    SELECT vocabulary_id FROM `@bq_target_project.@bq_target_dataset`.vocabulary
);

-- create and show summary

DROP TABLE IF EXISTS `@bq_target_project.@bq_target_dataset`.z_check_voc_errors_summary;
CREATE TABLE `@bq_target_project.@bq_target_dataset`.z_check_voc_errors_summary
AS
SELECT check_id, check_name, COUNT(*) AS row_count FROM `@bq_target_project.@bq_target_dataset`.z_check_voc_1
    GROUP BY check_id, check_name
UNION ALL
SELECT check_id, check_name, COUNT(*) AS row_count FROM `@bq_target_project.@bq_target_dataset`.z_check_voc_2
    GROUP BY check_id, check_name
UNION ALL
SELECT check_id, check_name, COUNT(*) AS row_count FROM `@bq_target_project.@bq_target_dataset`.z_check_voc_3
    GROUP BY check_id, check_name
UNION ALL
SELECT check_id, check_name, COUNT(*) AS row_count FROM `@bq_target_project.@bq_target_dataset`.z_check_voc_6
    GROUP BY check_id, check_name
UNION ALL
SELECT check_id, check_name, COUNT(*) AS row_count FROM `@bq_target_project.@bq_target_dataset`.z_check_voc_7
    GROUP BY check_id, check_name
UNION ALL
SELECT check_id, check_name, COUNT(*) AS row_count FROM `@bq_target_project.@bq_target_dataset`.z_check_voc_8
    GROUP BY check_id, check_name
UNION ALL
SELECT check_id, check_name, COUNT(*) AS row_count FROM `@bq_target_project.@bq_target_dataset`.z_check_voc_9
    GROUP BY check_id, check_name
UNION ALL
SELECT check_id, check_name, COUNT(*) AS row_count FROM `@bq_target_project.@bq_target_dataset`.z_check_voc_10
    GROUP BY check_id, check_name
UNION ALL
SELECT check_id, check_name, COUNT(*) AS row_count FROM `@bq_target_project.@bq_target_dataset`.z_check_voc_11
    GROUP BY check_id, check_name
UNION ALL
SELECT check_id, check_name, COUNT(*) AS row_count FROM `@bq_target_project.@bq_target_dataset`.z_check_voc_12
    GROUP BY check_id, check_name
UNION ALL
SELECT check_id, check_name, COUNT(*) AS row_count FROM `@bq_target_project.@bq_target_dataset`.z_check_voc_13
    GROUP BY check_id, check_name
UNION ALL
SELECT check_id, check_name, COUNT(*) AS row_count FROM `@bq_target_project.@bq_target_dataset`.z_check_voc_14
    GROUP BY check_id, check_name
UNION ALL
SELECT check_id, check_name, COUNT(*) AS row_count FROM `@bq_target_project.@bq_target_dataset`.z_check_voc_15
    GROUP BY check_id, check_name
UNION ALL
SELECT check_id, check_name, COUNT(*) AS row_count FROM `@bq_target_project.@bq_target_dataset`.z_check_voc_16
    GROUP BY check_id, check_name
UNION ALL
SELECT check_id, check_name, COUNT(*) AS row_count FROM `@bq_target_project.@bq_target_dataset`.z_check_voc_17
    GROUP BY check_id, check_name
;

SELECT * FROM `@bq_target_project.@bq_target_dataset`.z_check_voc_errors_summary
ORDER BY check_id;

