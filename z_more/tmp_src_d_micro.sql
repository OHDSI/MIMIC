-- -------------------------------------------------------------------
-- tmp_src_d_micro - temporary table, proof of concept
-- MIMIC IV 2.0: generate src_d_micro from microbiologyevents
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.tmp_src_d_micro AS
WITH d_micro AS (

    SELECT DISTINCT
        ab_itemid                   AS itemid, -- numeric ID
        ab_name                     AS label, -- source_code for custom mapping
        'ANTIBIOTIC'                AS category, 
        --
        TO_JSON_STRING(STRUCT(
            'ab_itemid' AS field_name,
            ab_itemid AS itemid
        ))                                  AS trace_id
    FROM
        `@source_project`.@hosp_dataset.microbiologyevents
    WHERE
        ab_itemid IS NOT NULL
    UNION ALL
    SELECT DISTINCT
        test_itemid                 AS itemid, -- numeric ID
        test_name                   AS label, -- source_code for custom mapping
        'MICROTEST'                 AS category, 
        --
        TO_JSON_STRING(STRUCT(
            'test_itemid' AS field_name,
            test_itemid AS itemid
        ))                                  AS trace_id
    FROM
        `@source_project`.@hosp_dataset.microbiologyevents
    WHERE
        test_itemid IS NOT NULL
    UNION ALL
    SELECT DISTINCT
        org_itemid                  AS itemid, -- numeric ID
        org_name                    AS label, -- source_code for custom mapping
        'ORGANISM'                  AS category, 
        --
        TO_JSON_STRING(STRUCT(
            'org_itemid' AS field_name,
            org_itemid AS itemid
        ))                                  AS trace_id
    FROM
        `@source_project`.@hosp_dataset.microbiologyevents
    WHERE
        org_itemid IS NOT NULL
    UNION ALL
    SELECT DISTINCT
        spec_itemid                 AS itemid, -- numeric ID
        spec_type_desc              AS label, -- source_code for custom mapping
        'SPECIMEN'                  AS category, 
        --
        TO_JSON_STRING(STRUCT(
            'spec_itemid' AS field_name,
            spec_itemid AS itemid
        ))                                  AS trace_id
    FROM
        `@source_project`.@hosp_dataset.microbiologyevents
    WHERE
        spec_itemid IS NOT NULL
)
SELECT
    itemid                      AS itemid, -- numeric ID
    label                       AS label, -- source_code for custom mapping
    category                    AS category, 
    --
    'microbiologyevents'                AS load_table_id,
    FARM_FINGERPRINT(GENERATE_UUID())   AS load_row_id,
    trace_id                            AS trace_id
FROM
    d_micro
;

