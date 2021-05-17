-- Use the csv files loaded to identify itemids that had been used previously in MIMIC related research 
-- and are presumably useful but have not been mapped yet in our custom mapping. 
-- Create a list of “delta” itemids as a “to be mapped” list. 


-- -------------------------------------------------------------------
-- chartevents.itemid.csv
-- 04 May 2021, 02:49 PM
-- -------------------------------------------------------------------

-- all presumably useful itemid

SELECT
    -- previously used itemid
    src.itemid,
    src.label,
    src.abbreviation,
    src.linksto,
    src.category,
    src.unitname,
    src.param_type,
    src.lownormalvalue,
    src.highnormalvalue,
    -- itemid in MIMIC IV conversion
    miv.source_label,
    miv.source_vocabulary_id,
    miv.source_domain_id,
    miv.source_concept_id,
    miv.source_concept_name,
    miv.target_vocabulary_id,
    miv.target_domain_id,
    miv.target_concept_id,
    miv.target_concept_name,
    miv.target_standard_concept,
    miv.row_count
FROM 
    `replace-it-with-actual-working-dataset-name.chartevents_itemid` src
LEFT JOIN
    `replace-it-with-actual-cdm-dataset-name.d_items_to_concept` miv
        ON  src.itemid = miv.itemid
;

-- chartevents.itemid to be mapped
SELECT
    miv.*
FROM 
    `replace-it-with-actual-working-dataset-name.chartevents_itemid` src
INNER JOIN
    `replace-it-with-actual-working-dataset-name.chartevents_to_concept` miv
        ON  src.itemid = miv.itemid
WHERE
    miv.target_concept_id IS NULL
;

-- -------------------------------------------------------------------
-- lab.itemid.csv
-- 04 May 2021, 02:49 PM
-- -------------------------------------------------------------------
 
-- all presumably useful itemid
SELECT
    -- previously used itemid
    src.itemid,
    src.label,
    src.fluid,
    src.category,
    src.loinc_code,
    -- itemid in MIMIC IV conversion
    miv.source_label,
    miv.source_vocabulary_id,
    miv.source_domain_id,
    miv.source_concept_id,
    miv.source_concept_name,
    miv.target_vocabulary_id,
    miv.target_domain_id,
    miv.target_concept_id,
    miv.target_concept_name,
    miv.target_standard_concept,
    miv.row_count
FROM
    `replace-it-with-actual-working-dataset-name.lab_itemid` src
LEFT JOIN
    `replace-it-with-actual-cdm-dataset-name.d_labitems_to_concept` miv
        ON  src.itemid = miv.itemid
;

-- labitems.itemid to be mapped
SELECT
    miv.*
FROM 
    `replace-it-with-actual-working-dataset-name.lab_itemid` src
INNER JOIN
    `replace-it-with-actual-cdm-dataset-name.d_labitems_to_concept` miv
        ON  src.itemid = miv.itemid
WHERE
    miv.target_concept_id IS NULL
;


