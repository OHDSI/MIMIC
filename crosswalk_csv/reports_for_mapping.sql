-- chartevents.itemid with context for more mapping:
--      current MIMIC IV mapping
--      previous MIMIC III mapping
--      top 5 values associated with the itemid
--      row counts for the itemid
-- 2021-05-17

-- -------------------------------------------------------------------------
-- chartevents itemid analysis
-- -------------------------------------------------------------------------

CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciv_analysis.chartevents_to_concept AS
WITH 
d_item AS 
(
    SELECT
        src.itemid                                  AS itemid,
        COUNT(*)                                    AS row_count -- 2226
    FROM
        `physionet-data`.mimic_icu.chartevents src
    GROUP BY
        src.itemid
            -- lk_chartevents_concept -- meas, voc = 'mimiciv_meas_chart'
),
d_values AS 
(
    SELECT
        src.itemid                                  AS itemid,
        src.value                                   AS value,
        COUNT(*)                                    AS row_count -- 2226
    FROM
        `physionet-data`.mimic_icu.chartevents src
    GROUP BY
        src.itemid, src.value
),
ranked_values AS
(
    -- # 1 - prepared source data
    SELECT
        itemid,
        value,
        row_count,
        ROW_NUMBER() OVER (
            PARTITION BY itemid
            ORDER BY row_count DESC, value
        ) AS row_num
    FROM
        d_values
),
pivot_values AS 
(
    SELECT *
    FROM
        ranked_values
    PIVOT
    (
        -- # 2 - aggregate
        MAX(value) AS value,
        MAX(row_count) AS row_count
        -- # 3 
        FOR row_num IN (1, 2, 3, 4, 5)
    )
),
d_source_concept AS 
(
    SELECT
        src.itemid                                  AS itemid,
        COALESCE(cl.measurement_concept_id, co.concept_id)      AS iii_concept_id,
        COALESCE(cl.measurement_concept_id, co.concept_id, vc.concept_id) AS source_concept_id
    FROM
        d_item src
    LEFT JOIN
        `physionet-data`.mimic_icu.d_items di
            ON  src.itemid = di.itemid
    LEFT JOIN
        `replace-it-with-actual-mimiciii-loaded-mapping-dataset`.chart_label_to_concept cl
            ON  cl.d_label = di.label
    LEFT JOIN
        `replace-it-with-actual-mimiciii-loaded-mapping-dataset`.chart_observation_to_concept co
            ON  src.itemid = co.itemid
    LEFT JOIN
        `replace-it-with-actual-cdm-dataset-name`.concept vc
            ON  di.label = vc.concept_code 
            AND vc.vocabulary_id IN (
                'mimiciv_meas_chart', -- MIMIC IV measurement mapping
                'mimiciv_mimic_generated' -- MIMIC III 2bil mapping
            )
)
SELECT
    -- d_items data
    di.itemid                                   AS itemid,
    di.linksto                                  AS linksto,
    di.category                                 AS category,
    di.label                                    AS label,
    di.unitname                                 AS unitname,
    di.param_type                               AS param_type,
    -- examples of values
    pv.value_1                                  AS value_1,
    pv.value_2                                  AS value_2,
    pv.value_3                                  AS value_3,
    pv.value_4                                  AS value_4,
    pv.value_5                                  AS value_5,
    -- mimic iii mapping
    cl.label_type                               AS iii_label_type,
    cl.value_lb                                 AS iii_value_lb,
    cl.value_ub                                 AS iii_value_ub,
    cl.unit_concept_id                          AS iii_unit_concept_id,
    vc_unit.concept_name                        AS unit_concept_name,
    -- source concept data
    vc.vocabulary_id                            AS source_vocabulary_id,
    vc.domain_id                                AS source_domain_id,
    vc.concept_id                               AS source_concept_id,
    vc.concept_name                             AS source_concept_name,
    -- target concept data
    vc2.vocabulary_id                           AS target_vocabulary_id,
    vc2.domain_id                               AS target_domain_id,
    vc2.concept_id                              AS target_concept_id,
    vc2.concept_name                            AS target_concept_name,
    vc2.standard_concept                        AS target_standard_concept, -- for double-check
    src.row_count                               AS row_count
FROM 
    d_item src
LEFT JOIN
    `physionet-data`.mimic_icu.d_items di
        ON  src.itemid = di.itemid
LEFT JOIN
    `replace-it-with-actual-mimiciii-loaded-mapping-dataset`.chart_label_to_concept cl
        ON  cl.d_label = di.label
LEFT JOIN
    d_source_concept sc
        ON sc.itemid = src.itemid
LEFT JOIN
    pivot_values pv
        ON src.itemid = pv.itemid
LEFT JOIN
    `replace-it-with-actual-cdm-dataset-name`.concept vc
        ON  sc.source_concept_id = vc.concept_id
LEFT JOIN
    `replace-it-with-actual-cdm-dataset-name`.concept_relationship vcr
        ON  vc.concept_id = vcr.concept_id_1
        AND vcr.relationship_id = 'Maps to'
LEFT JOIN
    `replace-it-with-actual-cdm-dataset-name`.concept vc2
        ON vc2.concept_id = vcr.concept_id_2
        AND vc2.standard_concept = 'S'
        AND vc2.invalid_reason IS NULL
LEFT JOIN
    `replace-it-with-actual-cdm-dataset-name`.concept vc_unit
        ON vc_unit.concept_id = cl.unit_concept_id
        AND vc_unit.standard_concept = 'S'
        AND vc_unit.invalid_reason IS NULL
ORDER BY
    src.row_count DESC
;



-- -- debug pivot

-- WITH
-- d_values AS 
-- (
--     SELECT
--         src.itemid                                  AS itemid,
--         src.value                                   AS value,
--         COUNT(*)                                    AS row_count -- 2226
--     FROM
--         `physionet-data`.mimic_icu.chartevents src
--     GROUP BY
--         src.itemid, src.value
-- ),
-- ranked_values AS
-- (
--     -- # 1 - prepared source data
--     SELECT
--         itemid,
--         value,
--         row_count,
--         ROW_NUMBER() OVER (
--             PARTITION BY itemid
--             ORDER BY row_count DESC, value
--         ) AS row_num
--     FROM
--         d_values
-- ),
-- pivot_values AS 
-- (
--     SELECT *
--     FROM
--         ranked_values
--     PIVOT
--     (
--         -- # 2 - aggregate
--         MAX(value) AS value,
--         MAX(row_count) AS row_count
--         -- # 3 
--         FOR row_num IN (1, 2, 3)
--     )
-- )
-- SELECT itemid, value_1, value_2, value_3
-- FROM pivot_values
-- ;


--------------------------
-- ranks are not applicable here
--
-- SELECT
--     src.itemid                                  AS itemid,
--     src.value                                   AS value,
--     RANK() OVER (
--         PARTITION BY itemid, value
--         ORDER BY itemid, value
--     )                                    AS rank_num,
--     DENSE_RANK() OVER (
--         PARTITION BY itemid, value
--         ORDER BY itemid, value
--     )                                    AS rankD_num,
--     PERCENT_RANK() OVER (
--         PARTITION BY itemid, value
--         ORDER BY itemid, value
--     )                                    AS rank_percent,
--     CUME_DIST() OVER (
--         PARTITION BY itemid, value
--         ORDER BY itemid, value
--     )                                    AS rank_cume_dist
-- FROM
--     `physionet-data`.mimic_icu.chartevents src
-- ORDER BY
--     src.itemid, src.value
-- ;
