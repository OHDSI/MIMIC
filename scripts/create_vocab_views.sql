-- ============================================================================
-- Create unified vocabulary views combining Athena master + custom vocabs
-- Views created in: lcp-internal.mimiciv_chorus_year3_vocab_views
-- Source datasets:
--   - lcp-internal.mimiciv_chorus_year3_athena (Athena master)
--   - lcp-internal.mimiciv_chorus_year3_cvb_mimic (Custom MIMIC vocabs)
--   - lcp-internal.mimiciv_chorus_year3_cvb_waveform (Custom Waveform vocabs)
-- ============================================================================

-- Create dataset if not exists
CREATE SCHEMA IF NOT EXISTS `lcp-internal.mimiciv_chorus_year3_vocab_views`
  OPTIONS(
    description="Unified vocabulary views combining Athena master and custom vocabularies"
  );

-- ============================================================================
-- CONCEPT table
-- Includes all Athena concepts + custom concepts not in Athena
-- ============================================================================
CREATE OR REPLACE VIEW `lcp-internal.mimiciv_chorus_year3_vocab_views.concept` AS
SELECT * FROM `lcp-internal.mimiciv_chorus_year3_athena.concept`
UNION ALL
SELECT * FROM `lcp-internal.mimiciv_chorus_year3_cvb_mimic.concept`
WHERE concept_id NOT IN (
  SELECT concept_id FROM `lcp-internal.mimiciv_chorus_year3_athena.concept`
)
UNION ALL
SELECT * FROM `lcp-internal.mimiciv_chorus_year3_cvb_waveform.concept`
WHERE concept_id NOT IN (
  SELECT concept_id FROM `lcp-internal.mimiciv_chorus_year3_athena.concept`
)
  AND concept_id NOT IN (
  SELECT concept_id FROM `lcp-internal.mimiciv_chorus_year3_cvb_mimic.concept`
);

-- ============================================================================
-- CONCEPT_RELATIONSHIP table
-- Includes all Athena relationships + custom relationships
-- ============================================================================
CREATE OR REPLACE VIEW `lcp-internal.mimiciv_chorus_year3_vocab_views.concept_relationship` AS
SELECT * FROM `lcp-internal.mimiciv_chorus_year3_athena.concept_relationship`
UNION ALL
SELECT * FROM `lcp-internal.mimiciv_chorus_year3_cvb_mimic.concept_relationship`
UNION ALL
SELECT * FROM `lcp-internal.mimiciv_chorus_year3_cvb_waveform.concept_relationship`;

-- ============================================================================
-- CONCEPT_ANCESTOR table
-- Includes all Athena ancestor relationships + custom
-- ============================================================================
CREATE OR REPLACE VIEW `lcp-internal.mimiciv_chorus_year3_vocab_views.concept_ancestor` AS
SELECT * FROM `lcp-internal.mimiciv_chorus_year3_athena.concept_ancestor`
UNION ALL
SELECT * FROM `lcp-internal.mimiciv_chorus_year3_cvb_mimic.concept_ancestor`
UNION ALL
SELECT * FROM `lcp-internal.mimiciv_chorus_year3_cvb_waveform.concept_ancestor`;

-- ============================================================================
-- CONCEPT_SYNONYM table
-- Includes all Athena synonyms + custom
-- ============================================================================
CREATE OR REPLACE VIEW `lcp-internal.mimiciv_chorus_year3_vocab_views.concept_synonym` AS
SELECT * FROM `lcp-internal.mimiciv_chorus_year3_athena.concept_synonym`
UNION ALL
SELECT * FROM `lcp-internal.mimiciv_chorus_year3_cvb_mimic.concept_synonym`
UNION ALL
SELECT * FROM `lcp-internal.mimiciv_chorus_year3_cvb_waveform.concept_synonym`;

-- ============================================================================
-- DRUG_STRENGTH table
-- Includes all Athena drug strengths + custom
-- ============================================================================
CREATE OR REPLACE VIEW `lcp-internal.mimiciv_chorus_year3_vocab_views.drug_strength` AS
SELECT * FROM `lcp-internal.mimiciv_chorus_year3_athena.drug_strength`
UNION ALL
SELECT * FROM `lcp-internal.mimiciv_chorus_year3_cvb_mimic.drug_strength`
UNION ALL
SELECT * FROM `lcp-internal.mimiciv_chorus_year3_cvb_waveform.drug_strength`;

-- ============================================================================
-- VOCABULARY table
-- Metadata about vocabularies themselves
-- Union all; custom vocabs may define new vocabulary IDs
-- ============================================================================
CREATE OR REPLACE VIEW `lcp-internal.mimiciv_chorus_year3_vocab_views.vocabulary` AS
SELECT * FROM `lcp-internal.mimiciv_chorus_year3_athena.vocabulary`
UNION ALL
SELECT * FROM `lcp-internal.mimiciv_chorus_year3_cvb_mimic.vocabulary`
WHERE vocabulary_id NOT IN (
  SELECT vocabulary_id FROM `lcp-internal.mimiciv_chorus_year3_athena.vocabulary`
)
UNION ALL
SELECT * FROM `lcp-internal.mimiciv_chorus_year3_cvb_waveform.vocabulary`
WHERE vocabulary_id NOT IN (
  SELECT vocabulary_id FROM `lcp-internal.mimiciv_chorus_year3_athena.vocabulary`
)
  AND vocabulary_id NOT IN (
  SELECT vocabulary_id FROM `lcp-internal.mimiciv_chorus_year3_cvb_mimic.vocabulary`
);

-- ============================================================================
-- CONCEPT_CLASS table
-- Standard OMOP reference table; usually no custom entries
-- ============================================================================
CREATE OR REPLACE VIEW `lcp-internal.mimiciv_chorus_year3_vocab_views.concept_class` AS
SELECT * FROM `lcp-internal.mimiciv_chorus_year3_athena.concept_class`
UNION ALL
SELECT * FROM `lcp-internal.mimiciv_chorus_year3_cvb_mimic.concept_class`
WHERE concept_class_id NOT IN (
  SELECT concept_class_id FROM `lcp-internal.mimiciv_chorus_year3_athena.concept_class`
)
UNION ALL
SELECT * FROM `lcp-internal.mimiciv_chorus_year3_cvb_waveform.concept_class`
WHERE concept_class_id NOT IN (
  SELECT concept_class_id FROM `lcp-internal.mimiciv_chorus_year3_athena.concept_class`
)
  AND concept_class_id NOT IN (
  SELECT concept_class_id FROM `lcp-internal.mimiciv_chorus_year3_cvb_mimic.concept_class`
);

-- ============================================================================
-- DOMAIN table
-- Standard OMOP reference table; usually no custom entries
-- ============================================================================
CREATE OR REPLACE VIEW `lcp-internal.mimiciv_chorus_year3_vocab_views.domain` AS
SELECT * FROM `lcp-internal.mimiciv_chorus_year3_athena.domain`
UNION ALL
SELECT * FROM `lcp-internal.mimiciv_chorus_year3_cvb_mimic.domain`
WHERE domain_id NOT IN (
  SELECT domain_id FROM `lcp-internal.mimiciv_chorus_year3_athena.domain`
)
UNION ALL
SELECT * FROM `lcp-internal.mimiciv_chorus_year3_cvb_waveform.domain`
WHERE domain_id NOT IN (
  SELECT domain_id FROM `lcp-internal.mimiciv_chorus_year3_athena.domain`
)
  AND domain_id NOT IN (
  SELECT domain_id FROM `lcp-internal.mimiciv_chorus_year3_cvb_mimic.domain`
);

-- ============================================================================
-- RELATIONSHIP table
-- Standard OMOP reference table; maps relationship types
-- ============================================================================
CREATE OR REPLACE VIEW `lcp-internal.mimiciv_chorus_year3_vocab_views.relationship` AS
SELECT * FROM `lcp-internal.mimiciv_chorus_year3_athena.relationship`
UNION ALL
SELECT * FROM `lcp-internal.mimiciv_chorus_year3_cvb_mimic.relationship`
WHERE relationship_id NOT IN (
  SELECT relationship_id FROM `lcp-internal.mimiciv_chorus_year3_athena.relationship`
)
UNION ALL
SELECT * FROM `lcp-internal.mimiciv_chorus_year3_cvb_waveform.relationship`
WHERE relationship_id NOT IN (
  SELECT relationship_id FROM `lcp-internal.mimiciv_chorus_year3_athena.relationship`
)
  AND relationship_id NOT IN (
  SELECT relationship_id FROM `lcp-internal.mimiciv_chorus_year3_cvb_mimic.relationship`
);

-- ============================================================================
-- Summary
-- ============================================================================
-- All views created successfully in lcp-internal.mimiciv_chorus_year3_vocab_views
-- Next step: Update your ETL config to point to this dataset
-- Example in full.etlconf:
--   "@voc_dataset": "mimiciv_chorus_year3_vocab_views"
