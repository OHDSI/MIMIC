

-- -------------------------------------------------------------------------
-- d_items coverage
-- -------------------------------------------------------------------------

SELECT 
    di.linksto, COALESCE(vc.vocabulary_id, vc_proc.vocabulary_id) AS vocabulary_id, 
    COUNT(*) AS count_itemid, COUNT(COALESCE(vc.vocabulary_id, vc_proc.vocabulary_id)) AS count_mapped
FROM 
    `physionet-data.mimic_icu.d_items` di
LEFT JOIN
    `odysseus-mimic-dev.mimiciv_full_current_cdm_531.concept` vc
        ON  di.label = vc.concept_code
        AND vc.vocabulary_id in (
            'mimiciv_proc_datetimeevents', -- procedures
            'mimiciv_meas_chart',       -- measurement from chartevents (HR, RR, O2)
            'mimiciv_meas_chartevents_value' -- values for Heart Rythm etc
        )
LEFT JOIN
    `odysseus-mimic-dev.mimiciv_full_current_cdm_531.concept` vc_proc
        ON  CAST(di.itemid AS STRING) = vc_proc.concept_code
        AND vc_proc.vocabulary_id = 'mimiciv_proc_itemid' -- procedures from procedureevents
GROUP BY
    di.linksto, vocabulary_id
ORDER BY
    di.linksto, vocabulary_id
;

-- CROSSWALK TABLES (POC version)
-- to add to unload workflow

-- -------------------------------------------------------------------------
-- Crosswalk table from icu.d_items.itemid to concept.concept_id
-- -------------------------------------------------------------------------

-- don't forget unload procedure

CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_full_current_cdm_531.d_items_to_concept AS
WITH 
d_item AS 
(
    SELECT
        di.linksto                                  AS linksto,
        di.itemid                                   AS itemid,
        di.label                                    AS label,
        IF(
            di.linksto = 'procedureevents',
                CAST(di.itemid AS STRING),
                di.label
        )                                           AS source_code
    FROM 
        `physionet-data`.mimic_icu.d_items di
            -- lk_itemid_concept -- proc
            -- union all lk_datetimeevents_concept -- proc
            -- union all lk_chartevents_concept -- meas, voc = 'mimiciv_meas_chart'
)
SELECT
    -- d_items data
    di.linksto                                  AS linksto,
    di.itemid                                   AS itemid,
    di.label                                    AS label,
    di.source_code                              AS source_code,
    -- source concept data
    vc.vocabulary_id                            AS source_vocabulary_id, -- replace with clean.voc
    vc.domain_id                                AS source_domain_id,
    vc.concept_id                               AS source_concept_id,
    vc.concept_name                             AS source_concept_name,
    -- target concept data
    vc2.vocabulary_id                           AS target_vocabulary_id,
    vc2.domain_id                               AS target_domain_id,
    vc2.concept_id                              AS target_concept_id,
    vc2.concept_name                            AS target_concept_name,
    vc2.standard_concept                        AS target_standard_concept, -- for double-check sake
    CAST(NULL AS INT64)                         AS row_count
FROM 
    d_item di
LEFT JOIN
    `odysseus-mimic-dev`.mimiciv_full_current_cdm_531.concept vc
        ON  di.source_code = vc.concept_code
        AND vc.vocabulary_id in (
            'mimiciv_proc_datetimeevents', -- procedures
            'mimiciv_meas_chart',       -- measurement from chartevents (HR, RR, O2)
            'mimiciv_proc_itemid' -- procedures from procedureevents
        )
LEFT JOIN
    `odysseus-mimic-dev`.mimiciv_full_current_cdm_531.concept_relationship vcr
        ON  vc.concept_id = vcr.concept_id_1
        AND vcr.relationship_id = 'Maps to'
LEFT JOIN
    `odysseus-mimic-dev`.mimiciv_full_current_cdm_531.concept vc2
        ON vc2.concept_id = vcr.concept_id_2
        AND vc2.standard_concept = 'S'
        AND vc2.invalid_reason IS NULL
ORDER BY
    di.linksto, di.itemid
;

-- -------------------------------------------------------------------------
-- Crosswalk table from hosp.d_labitems.itemid to concept.concept_id
-- -------------------------------------------------------------------------


CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_full_current_cdm_531.d_labitems_to_concept AS
WITH 
d_labitem AS 
(
    SELECT
        dlab.itemid                                 AS itemid,
        dlab.label                                  AS label,
        dlab.fluid                                  AS fluid,
        dlab.category                               AS category,
        dlab.loinc_code                             AS loinc_code,
        COALESCE(dlab.loinc_code, 
            CONCAT(dlab.label, '|', dlab.fluid, '|', dlab.category)
        )                                           AS source_code,
        IF(dlab.loinc_code IS NOT NULL, 
            'LOINC', 
            'mimiciv_meas_lab_loinc'
        )                                           AS source_vocabulary_id
    FROM 
        `physionet-data`.mimic_hosp.d_labitems dlab
            -- lk_meas_labevents_clean
)
SELECT
    -- d_items data
    di.itemid                                   AS itemid,
    di.label                                    AS label,
    di.fluid                                    AS fluid,
    di.category                                 AS category,
    di.loinc_code                               AS loinc_code,
    di.source_code                              AS source_code,
    di.source_vocabulary_id                     AS source_vocabulary_id,
    -- source concept data
    vc.domain_id                                AS source_domain_id,
    vc.concept_id                               AS source_concept_id,
    vc.concept_name                             AS source_concept_name,
    -- target concept data
    vc2.vocabulary_id                           AS target_vocabulary_id,
    vc2.domain_id                               AS target_domain_id,
    vc2.concept_id                              AS target_concept_id,
    vc2.concept_name                            AS target_concept_name,
    vc2.standard_concept                        AS target_standard_concept, -- for double-check sake
    CAST(NULL AS INT64)                         AS row_count
FROM 
    d_labitem di
LEFT JOIN
    `odysseus-mimic-dev`.mimiciv_full_current_cdm_531.concept vc
        ON  di.source_code = vc.concept_code
        AND vc.vocabulary_id = di.source_vocabulary_id
LEFT JOIN
    `odysseus-mimic-dev`.mimiciv_full_current_cdm_531.concept_relationship vcr
        ON  vc.concept_id = vcr.concept_id_1
        AND vcr.relationship_id = 'Maps to'
LEFT JOIN
    `odysseus-mimic-dev`.mimiciv_full_current_cdm_531.concept vc2
        ON vc2.concept_id = vcr.concept_id_2
        AND vc2.standard_concept = 'S'
        AND vc2.invalid_reason IS NULL
ORDER BY
    di.itemid -- another sort order?
;


-- -------------------------------------------------------------------------
-- Crosswalk table from hosp.d_micro.itemid to concept.concept_id
--      additionally: hosp.microbiologyevents.interpretation to concept_id
-- -------------------------------------------------------------------------

CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_full_current_cdm_531.d_micro_to_concept AS
WITH 
d_item AS 
(
    -- lk_d_micro_concept
    SELECT
        dm.itemid                                       AS itemid,
        dm.category                                     AS category,
        dm.label                                        AS source_code, -- gcpt.label
        CONCAT('mimiciv_micro_', LOWER(dm.category))    AS source_vocabulary_id
    FROM
        `odysseus-mimic-dev`.mimiciv_full_cdm_2021_03_05.src_d_micro dm
    UNION ALL
    SELECT DISTINCT
        CAST(NULL AS INT64)             AS itemid,
        CAST(NULL AS STRING)            AS category,
        src.interpretation              AS source_code,
        'mimiciv_micro_resistance'      AS source_vocabulary_id
    FROM
        `odysseus-mimic-dev`.mimiciv_full_cdm_2021_03_05.lk_meas_ab_clean src
    WHERE
        src.interpretation IS NOT NULL
)
SELECT
    -- d_items data
    di.itemid                                   AS itemid,
    di.category                                 AS category,
    di.source_code                              AS source_code,
    di.source_vocabulary_id                     AS source_vocabulary_id,
    -- source concept data
    vc.domain_id                                AS source_domain_id,
    vc.concept_id                               AS source_concept_id,
    vc.concept_name                             AS source_concept_name,
    -- target concept data
    vc2.vocabulary_id                           AS target_vocabulary_id,
    vc2.domain_id                               AS target_domain_id,
    vc2.concept_id                              AS target_concept_id,
    vc2.concept_name                            AS target_concept_name,
    vc2.standard_concept                        AS target_standard_concept, -- for double-check sake
    CAST(NULL AS INT64)                         AS row_count
FROM 
    d_item di
LEFT JOIN
    `odysseus-mimic-dev`.mimiciv_full_current_cdm_531.concept vc
        ON  di.source_code = vc.concept_code
        AND vc.vocabulary_id = di.source_vocabulary_id
LEFT JOIN
    `odysseus-mimic-dev`.mimiciv_full_current_cdm_531.concept_relationship vcr
        ON  vc.concept_id = vcr.concept_id_1
        AND vcr.relationship_id = 'Maps to'
LEFT JOIN
    `odysseus-mimic-dev`.mimiciv_full_current_cdm_531.concept vc2
        ON vc2.concept_id = vcr.concept_id_2
        AND vc2.standard_concept = 'S'
        AND vc2.invalid_reason IS NULL
ORDER BY
    di.itemid, di.source_code
;



-- -------------------------------------------------------------------------
-- chartevents itemid analysis
-- -------------------------------------------------------------------------

CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_full_current_cdm_531.tmp_chartevents_to_concept AS
WITH 
d_item AS 
(
    SELECT
        di.linksto                                  AS linksto,
        di.itemid                                   AS itemid,
        di.label                                    AS label,
        di.label                                    AS source_code,
        COUNT(*)                                    AS row_count -- 2226
    FROM
        `physionet-data`.mimic_icu.d_items di
    INNER JOIN
        `physionet-data`.mimic_icu.chartevents src
            ON  di.itemid = src.itemid
    GROUP BY
        di.linksto, di.itemid, di.label
            -- lk_chartevents_concept -- meas, voc = 'mimiciv_meas_chart'
)
SELECT
    -- d_items data
    di.linksto                                  AS linksto,
    di.itemid                                   AS itemid,
    di.label                                    AS label,
    di.source_code                              AS source_code,
    -- source concept data
    vc.vocabulary_id                            AS source_vocabulary_id, -- replace with clean.voc
    vc.domain_id                                AS source_domain_id,
    vc.concept_id                               AS source_concept_id,
    vc.concept_name                             AS source_concept_name,
    -- target concept data
    vc2.vocabulary_id                           AS target_vocabulary_id,
    vc2.domain_id                               AS target_domain_id,
    vc2.concept_id                              AS target_concept_id,
    vc2.concept_name                            AS target_concept_name,
    vc2.standard_concept                        AS target_standard_concept, -- for double-check sake
    di.row_count                                AS row_count
FROM 
    d_item di
LEFT JOIN
    `odysseus-mimic-dev`.mimiciv_full_current_cdm_531.concept vc
        ON  di.source_code = vc.concept_code
LEFT JOIN
    `odysseus-mimic-dev`.mimiciv_full_current_cdm_531.concept_relationship vcr
        ON  vc.concept_id = vcr.concept_id_1
        AND vcr.relationship_id = 'Maps to'
LEFT JOIN
    `odysseus-mimic-dev`.mimiciv_full_current_cdm_531.concept vc2
        ON vc2.concept_id = vcr.concept_id_2
        AND vc2.standard_concept = 'S'
        AND vc2.invalid_reason IS NULL
ORDER BY
    di.linksto, di.itemid
;
