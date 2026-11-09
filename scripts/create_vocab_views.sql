-- ============================================================================
-- Create unified vocabulary views combining Athena master + custom vocabs
-- Source datasets:
--   - <project>.<dataset>athena (Athena master)
--   - <project>.<dataset>cvb_mimic (Custom MIMIC vocabs)
--   - <project>.<dataset>cvb_waveform (Custom Waveform vocabs)
-- ============================================================================

-- ============================================================================
-- CONCEPT table
-- Includes all Athena concepts + custom concepts not in Athena
-- ============================================================================
-- When Athena or custom vocab is updated, run this to update the view:
-- CALL BQ.REFRESH_MATERIALIZED_VIEW('<project>.<dataset>cdm.voc_concept');


DROP MATERIALIZED VIEW IF EXISTS `<project>.<dataset>cdm.voc_concept`;

CREATE MATERIALIZED VIEW `<project>.<dataset>cdm.voc_concept` AS
SELECT * FROM `<project>.<dataset>athena.concept`
UNION ALL
SELECT * FROM `<project>.<dataset>cvb_mimic.voc_concept`
-- WHERE concept_id NOT IN (
--   SELECT concept_id FROM `<project>.<dataset>athena.concept`
-- )
UNION ALL
SELECT * FROM `<project>.<dataset>cvb_waveform.voc_concept`;
-- WHERE concept_id NOT IN (
--   SELECT concept_id FROM `<project>.<dataset>athena.concept`
-- )
--   AND concept_id NOT IN (
--   SELECT concept_id FROM `<project>.<dataset>cvb_mimic.concept`
-- );

-- ============================================================================
-- CONCEPT_RELATIONSHIP table
-- Includes all Athena relationships + custom relationships
-- ============================================================================
DROP MATERIALIZED VIEW IF EXISTS `<project>.<dataset>cdm.voc_concept_relationship`;

CREATE MATERIALIZED VIEW `<project>.<dataset>cdm.voc_concept_relationship` AS
SELECT * FROM `<project>.<dataset>athena.concept_relationship`
UNION ALL
SELECT * FROM `<project>.<dataset>cvb_mimic.voc_concept_relationship`
UNION ALL
SELECT * FROM `<project>.<dataset>cvb_waveform.voc_concept_relationship`;

-- ============================================================================
-- CONCEPT_ANCESTOR table
-- Includes all Athena ancestor relationships + custom
-- ============================================================================
DROP MATERIALIZED VIEW IF EXISTS `<project>.<dataset>cdm.voc_concept_ancestor`;

CREATE MATERIALIZED VIEW `<project>.<dataset>cdm.voc_concept_ancestor` AS
SELECT * FROM `<project>.<dataset>athena.concept_ancestor`
UNION ALL
SELECT * FROM `<project>.<dataset>cvb_mimic.voc_concept_ancestor`
UNION ALL
SELECT * FROM `<project>.<dataset>cvb_waveform.concept_ancestor`;

-- ============================================================================
-- CONCEPT_SYNONYM table
-- Includes all Athena synonyms + custom
-- ============================================================================
DROP MATERIALIZED VIEW IF EXISTS `<project>.<dataset>cdm.voc_concept_synonym`;

CREATE MATERIALIZED VIEW `<project>.<dataset>cdm.voc_concept_synonym` AS
SELECT * FROM `<project>.<dataset>athena.concept_synonym`
UNION ALL
SELECT * FROM `<project>.<dataset>cvb_mimic.voc_concept_synonym`
UNION ALL
SELECT * FROM `<project>.<dataset>cvb_waveform.voc_concept_synonym`;

-- ============================================================================
-- DRUG_STRENGTH table
-- Includes all Athena drug strengths + custom
-- ============================================================================
DROP MATERIALIZED VIEW IF EXISTS `<project>.<dataset>cdm.voc_drug_strength`;

CREATE MATERIALIZED VIEW `<project>.<dataset>cdm.voc_drug_strength` AS
SELECT * FROM `<project>.<dataset>athena.drug_strength`
UNION ALL
SELECT * FROM `<project>.<dataset>cvb_mimic.voc_drug_strength`
UNION ALL
SELECT * FROM `<project>.<dataset>cvb_waveform.voc_drug_strength`;

-- ============================================================================
-- VOCABULARY table
-- Metadata about vocabularies themselves
-- Union all; custom vocabs may define new vocabulary IDs
-- ============================================================================
DROP MATERIALIZED VIEW IF EXISTS `<project>.<dataset>cdm.voc_vocabulary`;

CREATE MATERIALIZED VIEW `<project>.<dataset>cdm.voc_vocabulary` AS
SELECT * FROM `<project>.<dataset>athena.vocabulary`
UNION ALL
SELECT * FROM `<project>.<dataset>cvb_mimic.voc_vocabulary`
-- WHERE vocabulary_id NOT IN (
--   SELECT vocabulary_id FROM `<project>.<dataset>athena.vocabulary`
-- )
UNION ALL
SELECT * FROM `<project>.<dataset>cvb_waveform.voc_vocabulary`;
-- WHERE vocabulary_id NOT IN (
--   SELECT vocabulary_id FROM `<project>.<dataset>athena.vocabulary`
-- )
--   AND vocabulary_id NOT IN (
--   SELECT vocabulary_id FROM `<project>.<dataset>cvb_mimic.vocabulary`
-- );

-- ============================================================================
-- CONCEPT_CLASS table
-- Standard OMOP reference table; usually no custom entries
-- ============================================================================
DROP MATERIALIZED VIEW IF EXISTS `<project>.<dataset>cdm.voc_concept_class`;

CREATE MATERIALIZED VIEW `<project>.<dataset>cdm.voc_concept_class` AS
SELECT * FROM `<project>.<dataset>athena.concept_class`
UNION ALL
SELECT * FROM `<project>.<dataset>cvb_mimic.voc_concept_class`
-- WHERE concept_class_id NOT IN (
--   SELECT concept_class_id FROM `<project>.<dataset>athena.concept_class`
-- )
UNION ALL
SELECT * FROM `<project>.<dataset>cvb_waveform.voc_concept_class`;
-- WHERE concept_class_id NOT IN (
--   SELECT concept_class_id FROM `<project>.<dataset>athena.concept_class`
-- )
--   AND concept_class_id NOT IN (
--   SELECT concept_class_id FROM `<project>.<dataset>cvb_mimic.concept_class`
-- );

-- ============================================================================
-- DOMAIN table
-- Standard OMOP reference table; usually no custom entries
-- ============================================================================
DROP MATERIALIZED VIEW IF EXISTS `<project>.<dataset>cdm.voc_domain`;

CREATE MATERIALIZED VIEW `<project>.<dataset>cdm.voc_domain` AS
SELECT * FROM `<project>.<dataset>athena.domain`
UNION ALL
SELECT * FROM `<project>.<dataset>cvb_mimic.voc_domain`
-- WHERE domain_id NOT IN (
--   SELECT domain_id FROM `<project>.<dataset>athena.domain`
-- )
UNION ALL
SELECT * FROM `<project>.<dataset>cvb_waveform.voc_domain`;
-- WHERE domain_id NOT IN (
--   SELECT domain_id FROM `<project>.<dataset>athena.domain`
-- )
--   AND domain_id NOT IN (
--   SELECT domain_id FROM `<project>.<dataset>cvb_mimic.domain`
-- );

-- ============================================================================
-- RELATIONSHIP table
-- Standard OMOP reference table; maps relationship types
-- ============================================================================
DROP MATERIALIZED VIEW IF EXISTS `<project>.<dataset>cdm.voc_relationship`;

CREATE MATERIALIZED VIEW `<project>.<dataset>cdm.voc_relationship` AS
SELECT * FROM `<project>.<dataset>athena.relationship`
UNION ALL
SELECT * FROM `<project>.<dataset>cvb_mimic.voc_relationship`
-- WHERE relationship_id NOT IN (
--   SELECT relationship_id FROM `<project>.<dataset>athena.relationship`
-- )
UNION ALL
SELECT * FROM `<project>.<dataset>cvb_waveform.voc_relationship`;
-- WHERE relationship_id NOT IN (
--   SELECT relationship_id FROM `<project>.<dataset>athena.relationship`
-- )
--   AND relationship_id NOT IN (
--   SELECT relationship_id FROM `<project>.<dataset>cvb_mimic.relationship`
-- );

