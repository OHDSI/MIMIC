-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Populate cdm_measurement table
-- 
-- Dependencies: run after 
--      st_core.sql,
--      st_hosp.sql,
--      st_icu.sql, (rule 2)
--      cdm_person.sql,
--      cdm_visit_occurrence,
--      cdm_visit_detail(?)
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize create or replace
--
-- src_labevents: look closer to fields priority and specimen_id
-- src_labevents.value: 
--      investigate if there are formatted values with thousand separators,
--      and if we need to use more complicated parsing.
--      see `@etl_project`.@etl_dataset.an_labevents_full
-- -------------------------------------------------------------------



--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.cdm_measurement
(
    measurement_id                INT64     not null ,
    person_id                     INT64     not null ,
    measurement_concept_id        INT64     not null ,
    measurement_date              DATE      not null ,
    measurement_datetime          DATETIME           ,
    measurement_time              STRING             ,
    measurement_type_concept_id   INT64     not null ,
    operator_concept_id           INT64              ,
    value_as_number               FLOAT64            ,
    value_as_concept_id           INT64              ,
    unit_concept_id               INT64              ,
    range_low                     FLOAT64            ,
    range_high                    FLOAT64            ,
    provider_id                   INT64              ,
    visit_occurrence_id           INT64              ,
    visit_detail_id               INT64              ,
    measurement_source_value      STRING             ,
    measurement_source_concept_id INT64              ,
    unit_source_value             STRING             ,
    value_source_value            STRING             ,
    -- 
    unit_id                       STRING,
    load_table_id                 STRING,
    load_row_id                   INT64,
    trace_id                      STRING  
)
;


-- -------------------------------------------------------------------
-- Rule 1
-- LABS from labevents
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- tmp_labevents_clean
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.tmp_labevents_clean AS
SELECT
    FARM_FINGERPRINT(GENERATE_UUID())       AS measurement_id,
    src.subject_id                          AS subject_id,
    src.charttime                           AS charttime, -- measurement_datetime,
    src.hadm_id                             AS hadm_id,
    src.itemid                              AS itemid,
    src.value                               AS value, -- value_source_value
    REGEXP_EXTRACT(src.value, r'^(\<=|\>=|\>|\<|=|)')   AS value_operator,
    REGEXP_EXTRACT(src.value, r'[-]?[\d]+[.]?[\d]*')    AS value_number, -- assume "-0.34 etc"
    src.valueuom                            AS valueuom, -- unit_source_value,
    src.ref_range_lower                     AS ref_range_lower,
    src.ref_range_upper                     AS ref_range_upper,
    --
    src.load_table_id       AS load_table_id,
    src.load_row_id         AS load_row_id,
    src.trace_id            AS trace_id
FROM
    `@etl_project`.@etl_dataset.src_labevents src
;

-- -------------------------------------------------------------------
-- tmp_lab_loinc_concept
-- open point: Add 'Maps to value'
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.tmp_lab_loinc_concept AS
SELECT
    vc.concept_code         AS source_code, -- loinc_code,
    vc.domain_id            AS source_domain_id,
    vc.concept_id           AS source_concept_id,
    vc2.domain_id           AS target_domain_id,
    vc2.concept_id          AS target_concept_id,
    COALESCE(
        dlab.itemid, 
        CASE 
            WHEN vc.vocabulary_id = 'mimic_lab_loinc' THEN vc.source_code
            ELSE NULL
        END
    )                       AS itemid,
    dlab.fluid              AS fluid,
    dlab.category           AS category -- 'Blood Gas', 'Chemistry', 'Hematology'

FROM
    `@etl_project`.@etl_dataset.voc_concept vc
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept_relationship vcr
        ON  vc.concept_id = vcr.concept_id_1
        AND vcr.relationship_id = 'Maps to'
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept vc2
        ON vc2.concept_id = vcr.concept_id_2
        AND vc2.standard_concept = 'S'
        AND vc2.invalid_reason IS NULL
LEFT JOIN 
    `@etl_project`.@etl_dataset.src_d_labitems dlab
        ON dlab.loinc_code = vc.concept_code 
        -- double check loinc codes: do we need to use event dates in join?
        -- and do we need to do any parsing/replacement to match codes?
WHERE
    -- gcpt_lab_label_to_concept -- 
    vc.vocabulary_id IN ('LOINC', 'mimic_lab_loinc') AND domain_id = 'Measurement'
    AND 
        COALESCE(
            dlab.itemid, 
            CASE 
                WHEN vc.vocabulary_id = 'mimic_lab_loinc' THEN vc.source_code
                ELSE NULL
            END
        ) IS NOT NULL
;

-- -------------------------------------------------------------------
-- tmp_operator_concept
-- open point: operators are usually just hard-coded
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.tmp_operator_concept AS
SELECT
    vc.concept_name     AS source_code, -- operator_name,
    vc.concept_id       AS target_concept_id -- operator_concept_id
FROM
    `@etl_project`.@etl_dataset.voc_concept vc
WHERE
    vc.domain_id = 'Meas Value Operator'
;

-- -------------------------------------------------------------------
-- tmp_meas_unit_concept
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.tmp_meas_unit_concept AS
SELECT
    vc.concept_name     AS source_code, -- operator_name,
    vc.concept_id       AS target_concept_id -- operator_concept_id
FROM
    `@etl_project`.@etl_dataset.voc_concept vc
WHERE
    -- gcpt_lab_unit_to_concept
    vc.vocabulary_id IN ('UCUM', 'mimic_meas_unit')
    vc.domain_id = 'Unit'
;

-- -------------------------------------------------------------------
-- tmp_lab_specimen_concept
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.tmp_lab_specimen_concept AS
SELECT
    vc.concept_code     AS source_code, -- gcpt.label
    vc.domain_id        AS source_domain_id,
    vc.concept_id       AS source_concept_id,
    vc2.domain_id       AS target_domain_id,
    vc2.concept_id      AS target_concept_id
FROM
    `@etl_project`.@etl_dataset.voc_concept vc
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept_relationship vcr
        ON  vc.concept_id = vcr.concept_id_1
        AND vcr.relationship_id = 'Maps to'
LEFT JOIN
    `@etl_project`.@etl_dataset.voc_concept vc2
        ON vc2.concept_id = vcr.concept_id_2
        AND vc2.standard_concept = 'S'
        AND vc2.invalid_reason IS NULL
WHERE
    -- gcpt_labs_specimen_to_concept 
    vc.vocabulary_id = 'mimic_lab_specimen'
;

-- -------------------------------------------------------------------
-- cdm_measurement
-- Rule 1 (LABS from labevents)
-- -------------------------------------------------------------------

INSERT INTO `@etl_project`.@etl_dataset.cdm_measurement
SELECT
    src.measurement_id                      AS measurement_id,
    per.person_id                           AS person_id,
    COALESCE(labc.target_concept_id, 0)     AS measurement_concept_id,
    CAST(src.charttime AS DATE)             AS measurement_date,
    src.charttime                           AS measurement_datetime,
    CAST(NULL AS STRING)                    AS measurement_time,
    CASE
        WHEN LOWER(labc.category) = 'blood gas'  THEN  2000000010
        WHEN LOWER(labc.category) = 'chemistry'  THEN  2000000011
        WHEN LOWER(labc.category) = 'hematology' THEN  2000000009
        ELSE 44818702   -- Lab result
         -- open point: to add these concepts to custom mapping csv if it is not added yet
    END                                     AS measurement_type_concept_id,
    opc.target_concept_id                   AS operator_concept_id,
    src.value_number                        AS value_as_number,
    CAST(NULL AS INT64)                     AS value_as_concept_id,
    uc.target_concept_id                    AS unit_concept_id,
    src.ref_range_lower                     AS range_low,
    src.ref_range_upper                     AS range_high,
    CAST(NULL AS INT64)                     AS provider_id,
    vis.visit_occurrence_id                 AS visit_occurrence_id,
    CAST(NULL AS INT64)                     AS visit_detail_id,
    src.itemid                              AS measurement_source_value,
    labc.source_concept_id                  AS measurement_source_concept_id,
    src.valueuom                            AS unit_source_value,
    src.value                               AS value_source_value,
    --
    'measurement.rule.1.labevents'  AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
--     specimen_concept_id
FROM  
    `@etl_project`.@etl_dataset.tmp_labevents_clean src
LEFT JOIN 
    `@etl_project`.@etl_dataset.src_admissions adm
        ON adm.hadm_id = src.hadm_id
LEFT JOIN 
    `@etl_project`.@etl_dataset.tmp_lab_loinc_concept labc
        ON labc.itemid = src.itemid
LEFT JOIN 
    `@etl_project`.@etl_dataset.tmp_operator_concept opc
        ON opc.source_code = src.value_operator
LEFT JOIN 
    `@etl_project`.@etl_dataset.tmp_meas_unit_concept uc
        ON uc.source_code = src.valueuom
LEFT JOIN
    `@etl_project`.@etl_dataset.tmp_lab_specimen_concept spc
        ON labc.fluid = spc.source_code
LEFT JOIN 
    `@etl_project`.@etl_dataset.cdm_person per
        ON CAST(src.subject_id AS STRING) = per.person_source_value
LEFT JOIN 
    `@etl_project`.@etl_dataset.cdm_visit_occurrence vis
        ON CAST(src.hadm_id AS STRING) = vis.visit_source_value
;



-- "specimen_lab" AS ( --generated specimen: each lab is associated with a fictive specimen
-- SELECT
--   nextval('mimic_id_seq') as specimen_id    -- non NULL,
--     person_id                                 -- non NULL,
--     coalesce(specimen_concept_id,
--     0 ) as specimen_concept_id,
--     581378 as specimen_type_concept_id    -- non NULL,
--     measurement_date as specimen_date,
--     measurement_datetime as specimen_datetime,
--     null::double precision as quantity,
--     null::integer unit_concept_id,
--     null::integer anatomic_site_concept_id,
--     null::integer disease_status_concept_id,
--     null::integer specimen_source_id,
--     null::text specimen_source_value,
--     null::text unit_source_value,
--     null::text anatomic_site_source_value,
--     null::text disease_status_source_value,
--     row_to_insert.measurement_id -- usefull for fact_relationship
-- FROM row_to_insert
-- ),
-- "insert_specimen_lab" AS (
-- INSERT INTO :OMOP_SCHEMA.specimen
-- (
--       specimen_id,
--     person_id,
--     specimen_concept_id,
--     specimen_type_concept_id,
--     specimen_date,
--     specimen_datetime,
--     quantity,
--     unit_concept_id,
--     anatomic_site_concept_id,
--     disease_status_concept_id,
--     specimen_source_id,
--     specimen_source_value,
--     unit_source_value,
--     anatomic_site_source_value,
--     disease_status_source_value
-- )
-- SELECT
--   specimen_id,    -- non NULL,
--     person_id ,                        -- non NULL,
--     specimen_concept_id,         -- non NULL,
--     specimen_type_concept_id,    -- non NULL,
--     specimen_date,
--     specimen_datetime,
--     quantity,
--     unit_concept_id,
--     anatomic_site_concept_id,
--     disease_status_concept_id,
--     specimen_source_id,
--     specimen_source_value,
--     unit_source_value,
--     anatomic_site_source_value,
--     disease_status_source_value
-- FROM specimen_lab
-- RETURNING *
-- ),


-- "insert_fact_relationship_specimen_measurement" AS (
--     INSERT INTO :OMOP_SCHEMA.fact_relationship
--     (SELECT
--       36 AS domain_concept_id_1 -- Specimen,
--     specimen_id as fact_id_1,
--     21 AS domain_concept_id_2 -- Measurement,
--     measurement_id as fact_id_2,
--     44818854 as relationship_concept_id -- Specimen of (SNOMED)
   
-- FROM
--     specimen_lab
--     UNION ALL
-- SELECT
--       21 AS domain_concept_id_1 -- Measurement,
--     measurement_id as fact_id_1,
--     36 AS domain_concept_id_2 -- Specimen,
--     specimen_id as fact_id_2,
--     44818756 as relationship_concept_id -- Has specimen (SNOMED)
   
-- FROM
--     specimen_lab
--     )
-- )



-- -------------------------------------------------------------------
-- Rule 2
-- LABS FROM chartevents
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `@etl_project`.@etl_dataset.lk_chartevents_lab AS
SELECT
    ce.itemid                       AS itemid,
    ce.mimic_id                     AS measurement_id,
    ce.subject_id                   AS subject_id,
    ce.hadm_id                      AS,
    ce.storetime                    AS measurement_datetime, -- according to Alistair, storetime is the result time,
    ce.charttime                    AS specimen_datetime,    -- according to Alistair, charttime is the specimen time,
    ce.value                        AS value_source_value,
    EXTRACT_OPERATOR(value) AS operator_name,
    EXTRACT_VALUE_PERIOD_DECIMAL(value) AS value_as_number,
    COALESCE(valueuom, EXTRACT_UNIT(value)) AS unit_source_value
FROM
    `@etl_project`.@etl_dataset.src_chartevents ce
INNER JOIN 
    `@etl_project`.@etl_dataset.voc_concept vc -- concept driven dispatcher
        ON  vc.concept_code = CAST(ce.itemid AS STRING)
            AND vc.domain_id     = 'Measurement'
            AND vc.vocabulary_id = 'MIMIC d_items'
            AND vc.concept_class_id IN ( 
                'Labs',
                'Blood Gases',
                'Hematology',
                'Heme/Coag',
                'Coags',
                'CSF',
                'Enzymes',
                'Chemistry'
            )
WHERE ce.error IS NULL OR ce.error = 0
;

"d_items" AS
SELECT
    itemid,
    category,
    label,
    mimic_id
FROM
    d_items),

cdm_person
src_admissions (mimic_id, hadm_id)
tmp_operator_concept
create "mimic_d_items_to_concept" similar to tmp_lab_loinc_concept
tmp_meas_unit_concept

"gcpt_labs_from_chartevents_to_concept" AS
SELECT
    label,
    category,
    measurement_type_concept_id
FROM
    gcpt_labs_from_chartevents_to_concept),


"row_to_insert" AS (
SELECT
  chartevents_lab.measurement_id,
    src_patients.person_id,
    coalesce(omop_loinc.measurement_concept_id,
    gcpt_lab_label_to_concept.measurement_concept_id,
    0) as measurement_concept_id,
    chartevents_lab.measurement_datetime::date AS measurement_date,
    chartevents_lab.measurement_datetime AS measurement_datetime,
    CASE
     WHEN category ILIKE 'blood gases'  THEN  2000000010
     WHEN lower(category) IN ('chemistry','enzymes')  THEN  2000000011
     WHEN lower(category) IN ('hematology','heme/coag','csf','coags') THEN  2000000009
     WHEN lower(category) IN ('labs') THEN coalesce(gcpt_labs_from_chartevents_to_concept.measurement_type_concept_id,44818702)
     ELSE 44818702 -- there no trivial way to classify
  END AS measurement_type_concept_id -- Lab result,
    operator_concept_id AS operator_concept_id -- = operator,
    chartevents_lab.value_as_number AS value_as_number,
    null::bigint AS value_as_concept_id,
    gcpt_lab_unit_to_concept.unit_concept_id,
    null::double precision AS range_low,
    null::double precision AS range_high,
    null::bigint AS provider_id,
    src_admissions.visit_occurrence_id AS visit_occurrence_id,
    null::bigint As visit_detail_id,
    d_items.label AS measurement_source_value,
    d_items.mimic_id AS measurement_source_concept_id,
    gcpt_lab_unit_to_concept.unit_source_value,
    chartevents_lab.value_source_value,
    specimen_datetime
 
FROM
    chartevents_lab
LEFT JOIN src_patients USING (subject_id)
LEFT JOIN src_admissions USING (hadm_id)
LEFT JOIN d_items USING (itemid)
LEFT JOIN omop_loinc USING (label)
LEFT JOIN omop_operator USING (operator_name)
LEFT JOIN gcpt_lab_label_to_concept USING (label)
LEFT JOIN gcpt_labs_from_chartevents_to_concept USING (category,
    label)
LEFT JOIN gcpt_lab_unit_to_concept USING (unit_source_value)
),
"specimen_lab" AS ( -- generated specimen: each lab measurement is associated with a fictive specimen
SELECT
  nextval('mimic_id_seq') as specimen_id,
    person_id,
    0::integer as specimen_concept_id         -- no information right now about any specimen provenance,
    581378 as specimen_type_concept_id,
    specimen_datetime::date as specimen_date,
    specimen_datetime as specimen_datetime,
    null::double precision as quantity,
    null::integer unit_concept_id,
    null::integer anatomic_site_concept_id,
    null::integer disease_status_concept_id,
    null::integer specimen_source_id,
    null::text specimen_source_value,
    null::text unit_source_value,
    null::text anatomic_site_source_value,
    null::text disease_status_source_value,
    row_to_insert.measurement_id -- usefull for fact_relationship
FROM row_to_insert
),
"insert_specimen_lab" AS (
INSERT INTO :OMOP_SCHEMA.specimen
(
      specimen_id,
    person_id,
    specimen_concept_id,
    specimen_type_concept_id,
    specimen_date,
    specimen_datetime,
    quantity,
    unit_concept_id,
    anatomic_site_concept_id,
    disease_status_concept_id,
    specimen_source_id,
    specimen_source_value,
    unit_source_value,
    anatomic_site_source_value,
    disease_status_source_value
)
SELECT
  specimen_id    -- non NULL,
    person_id                         -- non NULL,
    specimen_concept_id         -- non NULL,
    specimen_type_concept_id    -- non NULL,
    specimen_date,
    specimen_datetime,
    quantity,
    unit_concept_id,
    anatomic_site_concept_id,
    disease_status_concept_id,
    specimen_source_id,
    specimen_source_value,
    unit_source_value,
    anatomic_site_source_value,
    disease_status_source_value
FROM specimen_lab
RETURNING *
),
"insert_fact_relationship_specimen_measurement" AS (
    INSERT INTO :OMOP_SCHEMA.fact_relationship
    (SELECT
      36 AS domain_concept_id_1 -- Specimen,
    specimen_id as fact_id_1,
    21 AS domain_concept_id_2 -- Measurement,
    measurement_id as fact_id_2,
    44818854 as relationship_concept_id -- Specimen of (SNOMED)
   
FROM
    specimen_lab
    UNION ALL
SELECT
      21 AS domain_concept_id_1 -- Measurement,
    measurement_id as fact_id_1,
    36 AS domain_concept_id_2 -- Specimen,
    specimen_id as fact_id_2,
    44818756 as relationship_concept_id -- Has specimen (SNOMED)
   
FROM
    specimen_lab
    )
)
INSERT INTO :OMOP_SCHEMA.measurement
(
      measurement_id,
    person_id,
    measurement_concept_id,
    measurement_date,
    measurement_datetime,
    measurement_type_concept_id,
    operator_concept_id,
    value_as_number,
    value_as_concept_id,
    unit_concept_id,
    range_low,
    range_high,
    provider_id,
    visit_occurrence_id,
    visit_detail_id --no visit_detail assignation since datetime is not relevant,
    measurement_source_value,
    measurement_source_concept_id,
    unit_source_value,
    value_source_value
)
SELECT
  row_to_insert.measurement_id,
    row_to_insert.person_id,
    row_to_insert.measurement_concept_id,
    row_to_insert.measurement_date,
    row_to_insert.measurement_datetime,
    row_to_insert.measurement_type_concept_id,
    row_to_insert.operator_concept_id,
    row_to_insert.value_as_number,
    row_to_insert.value_as_concept_id,
    row_to_insert.unit_concept_id,
    row_to_insert.range_low,
    row_to_insert.range_high,
    row_to_insert.provider_id,
    row_to_insert.visit_occurrence_id,
    row_to_insert.visit_detail_id --no visit_detail assignation since datetime is not relevant,
    row_to_insert.measurement_source_value,
    row_to_insert.measurement_source_concept_id,
    row_to_insert.unit_source_value,
    row_to_insert.value_source_value
FROM row_to_insert;

-- -------------------------------------------------------------------
-- Rule 3
-- Microbiology
-- NOTICE: the number of culture is complicated to determine (the distinct on (coalesce).. is a result)
-- -------------------------------------------------------------------

WITH
"culture" AS (
SELECT
            DISTINCT ON (subject_id,
    hadm_id,
    coalesce(charttime,chartdate)
        ,
    coalesce(spec_itemid,0)
        ,
    coalesce(org_name,'')) spec_itemid
            ,
    microbiologyevents.mimic_id as measurement_id
        ,
    chartdate as measurement_date
        ,
    charttime as measurement_datetime
        ,
    subject_id
        ,
    hadm_id
        ,
    org_name
        ,
    spec_type_desc as measurement_source_value
        ,
    spec_itemid as specimen_source_value -- TODO: add the specimen type local concepts
        --,
    specimen_source_id --TODO: wait for next mimic release that will ship the specimen details
        ,
    specimen_concept_id
   
FROM
    microbiologyevents
    LEFT JOIN gcpt_microbiology_specimen_to_concept ON (label = spec_type_desc)

),
"resistance" AS (
SELECT
    spec_itemid,
    ab_itemid,
    nextval('mimic_id_seq') as measurement_id,
    chartdate as measurement_date,
    charttime as measurement_datetime,
    subject_id,
    hadm_id,
    extract_operator(dilution_text) as operator_name,
    extract_value_period_decimal(dilution_text) as value_as_number,
    ab_name as measurement_source_value,
    interpretation,
    dilution_text as value_source_value,
    org_name
   
FROM
    microbiologyevents
    WHERE dilution_text IS NOT NULL
),
"fact_relationship" AS (
SELECT
      culture.measurement_id as fact_id_1,
    resistance.measurement_id AS fact_id_2
   
FROM
    resistance
    LEFT JOIN culture ON (
        resistance.subject_id = culture.subject_id
        and resistance.hadm_id = culture.hadm_id
        AND coalesce(culture.measurement_datetime,culture.measurement_date) = coalesce(resistance.measurement_datetime,resistance.measurement_date)
        AND coalesce(resistance.spec_itemid,0) = coalesce(culture.spec_itemid,0)
        AND coalesce(resistance.org_name,'') = coalesce(culture.org_name,''))
),
"insert_fact_relationship" AS (
    INSERT INTO :OMOP_SCHEMA.fact_relationship
    (SELECT
      21 AS domain_concept_id_1 -- Measurement,
    fact_id_1,
    21 AS domain_concept_id_2 -- Measurement,
    fact_id_2,
    44818757 as relationship_concept_id -- Has interpretation (SNOMED) TODO find a better predicate
   
FROM
    fact_relationship
UNION ALL
SELECT
      21 AS domain_concept_id_1 -- Measurement,
    fact_id_2 as fact_id_1,
    21 AS domain_concept_id_2 -- Measurement,
    fact_id_1 as  fact_id_2,
    44818855  as relationship_concept_id --  Interpretation of (SNOMED) TODO find a better predicate
   
FROM
    fact_relationship
    )
),
"src_patients" AS
SELECT
    mimic_id AS person_id,
    subject_id
FROM
    src_patients),
"src_admissions" AS
SELECT
    mimic_id AS visit_occurrence_id,
    hadm_id
FROM
    src_admissions),
"specimen_culture" AS ( --generated specimen
SELECT
  nextval('mimic_id_seq') as specimen_id,
    src_patients.person_id,
    coalesce(specimen_concept_id,
     0) as specimen_concept_id         -- found manually,
    581378 as specimen_type_concept_id,
    culture.measurement_date as specimen_date               -- this is not really the specimen date but better than nothing,
    culture.measurement_datetime as specimen_datetime,
    null::double precision as quantity,
    null::integer unit_concept_id,
    null::integer anatomic_site_concept_id,
    null::integer disease_status_concept_id,
    null::integer specimen_source_id            --TODO: wait for next mimic release that will ship the specimen details,
    specimen_source_value as specimen_source_value,
    null::text unit_source_value,
    null::text anatomic_site_source_value,
    null::text disease_status_source_value,
    culture.measurement_id -- usefull for fact_relationship
FROM culture
LEFT JOIN src_patients USING (subject_id)
),
"insert_specimen_culture" AS (
INSERT INTO :OMOP_SCHEMA.specimen
(
      specimen_id,
    person_id,
    specimen_concept_id,
    specimen_type_concept_id,
    specimen_date,
    specimen_datetime,
    quantity,
    unit_concept_id,
    anatomic_site_concept_id,
    disease_status_concept_id,
    specimen_source_id,
    specimen_source_value,
    unit_source_value,
    anatomic_site_source_value,
    disease_status_source_value
)
SELECT
  specimen_id    -- non NULL,
    person_id                         -- non NULL,
    specimen_concept_id         -- non NULL,
    specimen_type_concept_id    -- non NULL,
    specimen_date               -- this is not really the specimen date but better than nothing,
    specimen_datetime,
    quantity,
    unit_concept_id,
    anatomic_site_concept_id,
    disease_status_concept_id,
    specimen_source_id,
    specimen_source_value,
    unit_source_value,
    anatomic_site_source_value,
    disease_status_source_value
FROM specimen_culture
RETURNING *
),
"insert_fact_relationship_specimen_measurement" AS (
    INSERT INTO :OMOP_SCHEMA.fact_relationship
    (SELECT
      36 AS domain_concept_id_1 -- Specimen,
    specimen_id as fact_id_1,
    21 AS domain_concept_id_2 -- Measurement,
    measurement_id as fact_id_2,
    44818854 as relationship_concept_id -- Specimen of (SNOMED)
   
FROM
    specimen_culture
    UNION ALL
SELECT
      21 AS domain_concept_id_1 -- Measurement,
    measurement_id as fact_id_1,
    36 AS domain_concept_id_2 -- Specimen,
    specimen_id as fact_id_2,
    44818756 as relationship_concept_id -- Has specimen (SNOMED)
   
FROM
    specimen_culture
    )
),
"omop_operator" AS
SELECT
    concept_name as operator_name,
    concept_id as operator_concept_id
FROM
    :OMOP_SCHEMA.concept WHERE  domain_id ilike 'Meas Value Operator'),
"gcpt_resistance_to_concept" AS
SELECT
    *
FROM
    gcpt_resistance_to_concept),
"gcpt_org_name_to_concept" AS
SELECT
    org_name,
    concept_id AS value_as_concept_id
FROM
    gcpt_org_name_to_concept JOIN :OMOP_SCHEMA.concept ON (concept_code = snomed::text AND vocabulary_id = 'SNOMED')),
"gcpt_spec_type_to_concept" AS
SELECT
    concept_id as measurement_concept_id,
    spec_type_desc as measurement_source_value
FROM
    gcpt_spec_type_to_concept LEFT JOIN :OMOP_SCHEMA.concept ON (loinc = concept_code AND standard_concept ='S' AND domain_id = 'Measurement')),
"gcpt_atb_to_concept" AS
SELECT
    concept_id as measurement_concept_id,
    ab_name as measurement_source_value
FROM
    gcpt_atb_to_concept LEFT JOIN :OMOP_SCHEMA.concept ON (concept.concept_code = gcpt_atb_to_concept.concept_code AND standard_concept = 'S' AND domain_id = 'Measurement')),
"d_items" AS
SELECT
    mimic_id as measurement_source_concept_id,
    itemid
FROM
    d_items WHERE category IN ( 'SPECIMEN',
    'ORGANISM')),
"row_to_insert" AS (SELECT
  culture.measurement_id AS measurement_id,
    src_patients.person_id,
    coalesce(gcpt_spec_type_to_concept.measurement_concept_id,
    4098207) as measurement_concept_id      -- --30088009 -- Blood Culture but not done yet,
    measurement_date AS measurement_date,
    measurement_datetime AS measurement_datetime,
    2000000007 AS measurement_type_concept_id -- Lab result -- Microbiology - Culture,
    null AS operator_concept_id,
    null value_as_number,
    CASE WHEN org_name IS NULL THEN 9189 ELSE coalesce(gcpt_org_name_to_concept.value_as_concept_id,
    0) END AS value_as_concept_id           -- staphiloccocus OR negative in case nothing,
    null::bigint AS unit_concept_id,
    null::double precision AS range_low,
    null::double precision AS range_high,
    null::bigint AS provider_id,
    src_admissions.visit_occurrence_id AS visit_occurrence_id,
    null::bigint As visit_detail_id,
    culture.measurement_source_value AS measurement_source_value -- BLOOD,
    d_items.measurement_source_concept_id AS measurement_source_concept_id,
    null::text AS unit_source_value,
    culture.org_name AS value_source_value -- Staph...
FROM culture
LEFT JOIN d_items ON (spec_itemid = itemid)
LEFT JOIN gcpt_spec_type_to_concept USING (measurement_source_value)
LEFT JOIN gcpt_org_name_to_concept USING (org_name)
LEFT JOIN src_patients USING (subject_id)
LEFT JOIN src_admissions USING (hadm_id)
UNION ALL
SELECT
  measurement_id AS measurement_id,
    src_patients.person_id,
    coalesce(gcpt_atb_to_concept.measurement_concept_id,
    4170475) as measurement_concept_id      -- Culture Sensitivity,
    measurement_date AS measurement_date,
    measurement_datetime AS measurement_datetime,
    2000000008 AS measurement_type_concept_id -- Lab result,
    operator_concept_id AS operator_concept_id -- = operator,
    value_as_number AS value_as_number,
    gcpt_resistance_to_concept.value_as_concept_id AS value_as_concept_id,
    null::bigint AS unit_concept_id,
    null::double precision AS range_low,
    null::double precision AS range_high,
    null::bigint AS provider_id,
    src_admissions.visit_occurrence_id AS visit_occurrence_id,
    null::bigint As visit_detail_id,
    resistance.measurement_source_value AS measurement_source_value,
    d_items.measurement_source_concept_id AS measurement_source_concept_id,
    null::text AS unit_source_value,
    value_source_value AS  value_source_value
FROM resistance
LEFT JOIN d_items ON (ab_itemid = itemid)
LEFT JOIN gcpt_resistance_to_concept USING (interpretation)
LEFT JOIN gcpt_atb_to_concept USING (measurement_source_value)
LEFT JOIN src_patients USING (subject_id)
LEFT JOIN src_admissions USING (hadm_id)
LEFT JOIN omop_operator USING (operator_name)
)
INSERT INTO :OMOP_SCHEMA.measurement
(
      measurement_id,
    person_id,
    measurement_concept_id,
    measurement_date,
    measurement_datetime,
    measurement_type_concept_id,
    operator_concept_id,
    value_as_number,
    value_as_concept_id,
    unit_concept_id,
    range_low,
    range_high,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    measurement_source_value,
    measurement_source_concept_id,
    unit_source_value,
    value_source_value
)
SELECT
  row_to_insert.measurement_id,
    row_to_insert.person_id,
    row_to_insert.measurement_concept_id,
    row_to_insert.measurement_date,
    row_to_insert.measurement_datetime,
    row_to_insert.measurement_type_concept_id,
    row_to_insert.operator_concept_id,
    row_to_insert.value_as_number,
    row_to_insert.value_as_concept_id,
    row_to_insert.unit_concept_id,
    row_to_insert.range_low,
    row_to_insert.range_high,
    row_to_insert.provider_id,
    row_to_insert.visit_occurrence_id,
    row_to_insert.visit_detail_id --no visit_detail assignation since datetime is not relevant,
    row_to_insert.measurement_source_value,
    row_to_insert.measurement_source_concept_id,
    row_to_insert.unit_source_value,
    row_to_insert.value_source_value
FROM row_to_insert;


-- -------------------------------------------------------------------
-- Rule 4
-- MEASUREMENT
FROM
    chartevents (without labs)
-- -------------------------------------------------------------------

WITH
"chartevents" as (
SELECT
      c.mimic_id as measurement_id,
      c.subject_id,
      c.hadm_id,
      c.cgid,
      m.measurement_concept_id as measurement_concept_id,
      c.charttime as measurement_datetime,
      c.valuenum as value_as_number,
      v.concept_id as value_as_concept_id,
      m.unit_concept_id as unit_concept_id,
      concept.concept_id as measurement_source_concept_id,
      c.valueuom as unit_source_value,
      CASE
    WHEN d_items.category   = 'Text' THEN valuenum -- discreteous variable
        WHEN m.label_type = 'systolic_bp' AND value ~ '[0-9]+/[0-9]+' THEN regexp_replace(value,'([0-9]+)/[0-9]*',E'\\1','g')::double precision
        WHEN m.label_type = 'diastolic_bp' AND value ~ '[0-9]+/[0-9]+' THEN regexp_replace(value,'[0-9]*/([0-9]+)',E'\\1','g')::double precision
        WHEN m.label_type = 'map_bp' AND value ~ '[0-9]+/[0-9]+' THEN map_bp_calc(value)
        WHEN m.label_type = 'fio2' AND c.valuenum between 0 AND 1 THEN c.valuenum * 100
    WHEN m.label_type = 'temperature' AND c.VALUENUM > 85 THEN (c.VALUENUM - 32)*5/9
    WHEN m.label_type = 'pain_level' THEN CASE
        WHEN d_items.LABEL ~* 'level' THEN CASE
              WHEN c.VALUE ~* 'unable' THEN NULL
              WHEN c.VALUE ~* 'none' AND NOT c.VALUE ~* 'mild' THEN 0
              WHEN c.VALUE ~* 'none' AND c.VALUE ~* 'mild' THEN 1
              WHEN c.VALUE ~* 'mild' AND NOT c.VALUE ~* 'mod' THEN 2
              WHEN c.VALUE ~* 'mild' AND c.VALUE ~* 'mod' THEN 3
              WHEN c.VALUE ~* 'mod'  AND NOT c.VALUE ~* 'sev' THEN 4
              WHEN c.VALUE ~* 'mod'  AND c.VALUE ~* 'sev' THEN 5
              WHEN c.VALUE ~* 'sev'  AND NOT c.VALUE ~* 'wor' THEN 6
              WHEN c.VALUE ~* 'sev'  AND c.VALUE ~* 'wor' THEN 7
              WHEN c.VALUE ~* 'wor' THEN 8
              ELSE NULL
              END
        WHEN c.VALUE ~* 'no' THEN 0
        WHEN c.VALUE ~* 'yes' THEN  1
            END
        WHEN m.label_type = 'sas_rass'  THEN CASE
                WHEN d_items.LABEL ~ '^Riker' THEN CASE
                      WHEN c.VALUE = 'Unarousable' THEN 1
                      WHEN c.VALUE = 'Very Sedated' THEN 2
                      WHEN c.VALUE = 'Sedated' THEN 3
                      WHEN c.VALUE = 'Calm/Cooperative' THEN 4
                      WHEN c.VALUE = 'Agitated' THEN 5
                      WHEN c.VALUE = 'Very Agitated' THEN 6
                      WHEN c.VALUE = 'Dangerous Agitation' THEN 7
                      ELSE NULL
                END
        END
    WHEN m.label_type = 'height_weight'  THEN CASE
        WHEN d_items.LABEL ~ 'W' THEN CASE
               WHEN d_items.LABEL ~* 'lb' THEN 0.453592 * c.VALUENUM
           ELSE NULL
           END
        WHEN d_items.LABEL ~ 'cm' THEN c.VALUENUM / 100::numeric
        ELSE 0.0254 * c.VALUENUM
        END
    ELSE NULL
    END AS valuenum_fromvalue,
      c.value as value_source_value,
      m.value_lb as value_lb,
      m.value_ub as value_ub,
      concept.concept_code AS measurement_source_value
   
FROM
    chartevents as c
    JOIN :OMOP_SCHEMA.concept -- concept driven dispatcher
    ON (    concept_code  = c.itemid::Text
    AND domain_id     = 'Measurement'
    AND vocabulary_id = 'MIMIC d_items'
    AND concept_class_id IS DISTINCT
FROM
    'Labs'
    AND concept_class_id IS DISTINCT
FROM
    'Blood Gases'
    AND concept_class_id IS DISTINCT
FROM
    'Hematology'
    AND concept_class_id IS DISTINCT
FROM
    'Chemistry'
    AND concept_class_id IS DISTINCT
FROM
    'Heme/Coag'
    AND concept_class_id IS DISTINCT
FROM
    'Coags'
    AND concept_class_id IS DISTINCT
FROM
    'CSF'
    AND concept_class_id IS DISTINCT
FROM
    'Enzymes'
       )  -- remove the labs,
    because done before
    LEFT JOIN d_items USING (itemid)
    LEFT JOIN gcpt_chart_label_to_concept as m ON (label = d_label)
    LEFT JOIN
       (
    SELECT mimic_name,
    concept_id,
    'heart_rhythm'::text AS label_type
   
FROM
    gcpt_heart_rhythm_to_concept
       ) as v  ON m.label_type = v.label_type AND c.value = v.mimic_name
    WHERE error IS NULL OR error= 0
  ),
"src_patients" AS
SELECT
    mimic_id AS person_id,
    subject_id
FROM
    src_patients),
"caregivers" AS
SELECT
    mimic_id AS provider_id,
    cgid
FROM
    caregivers),
"src_admissions" AS
SELECT
    mimic_id AS visit_occurrence_id,
    hadm_id
FROM
    src_admissions),
"row_to_insert" AS (SELECT
  measurement_id AS measurement_id,
    src_patients.person_id,
    coalesce(measurement_concept_id,
    0) as measurement_concept_id,
    measurement_datetime::date AS measurement_date,
    measurement_datetime AS measurement_datetime,
    44818701 as measurement_type_concept_id  --
FROM
    physical examination,
    4172703 AS operator_concept_id,
    coalesce(valuenum_fromvalue,
    value_as_number) AS value_as_number,
    value_as_concept_id AS value_as_concept_id,
    unit_concept_id AS unit_concept_id,
    value_lb AS range_low,
    value_ub AS range_high,
    caregivers.provider_id AS provider_id,
    src_admissions.visit_occurrence_id AS visit_occurrence_id,
    null::bigint As visit_detail_id,
    measurement_source_value,
    measurement_source_concept_id AS measurement_source_concept_id,
    unit_source_value AS unit_source_value,
    value_source_value AS  value_source_value
FROM chartevents
LEFT JOIN src_patients USING (subject_id)
LEFT JOIN caregivers USING (cgid)
LEFT JOIN src_admissions USING (hadm_id))
INSERT INTO :OMOP_SCHEMA.measurement
(
      measurement_id,
    person_id,
    measurement_concept_id,
    measurement_date,
    measurement_datetime,
    measurement_type_concept_id,
    operator_concept_id,
    value_as_number,
    value_as_concept_id,
    unit_concept_id,
    range_low,
    range_high,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    measurement_source_value,
    measurement_source_concept_id,
    unit_source_value,
    value_source_value
)
SELECT
  row_to_insert.measurement_id,
    row_to_insert.person_id,
    row_to_insert.measurement_concept_id,
    row_to_insert.measurement_date,
    row_to_insert.measurement_datetime,
    row_to_insert.measurement_type_concept_id,
    row_to_insert.operator_concept_id,
    row_to_insert.value_as_number,
    row_to_insert.value_as_concept_id,
    row_to_insert.unit_concept_id,
    row_to_insert.range_low,
    row_to_insert.range_high,
    row_to_insert.provider_id,
    row_to_insert.visit_occurrence_id,
    visit_detail_assign.visit_detail_id,
    row_to_insert.measurement_source_value,
    row_to_insert.measurement_source_concept_id,
    row_to_insert.unit_source_value,
    row_to_insert.value_source_value
FROM row_to_insert
LEFT JOIN :OMOP_SCHEMA.visit_detail_assign
ON row_to_insert.visit_occurrence_id = visit_detail_assign.visit_occurrence_id
AND
(--only one visit_detail
(is_first IS TRUE AND is_last IS TRUE)
OR -- first
(is_first IS TRUE AND is_last IS FALSE AND row_to_insert.measurement_datetime <= visit_detail_assign.visit_end_datetime)
OR -- last
(is_last IS TRUE AND is_first IS FALSE AND row_to_insert.measurement_datetime > visit_detail_assign.visit_start_datetime)
OR -- middle
(is_last IS FALSE AND is_first IS FALSE AND row_to_insert.measurement_datetime > visit_detail_assign.visit_start_datetime AND row_to_insert.measurement_datetime <= visit_detail_assign.visit_end_datetime)
);


-- -------------------------------------------------------------------
-- Rule 5
-- OUTPUT events
-- -------------------------------------------------------------------

WITH
"outputevents" AS (SELECT
  subject_id,
    hadm_id,
    itemid,
    cgid,
    valueuom AS unit_source_value,
    CASE
    WHEN itemid IN (227488,227489) THEN -1 * value
    ELSE value
  END AS value,
    mimic_id as measurement_id,
    charttime as measurement_datetime
FROM outputevents
where iserror is null
),
"gcpt_output_label_to_concept" AS
SELECT
    item_id as itemid,
    concept_id as measurement_concept_id
FROM
    gcpt_output_label_to_concept),
"gcpt_lab_unit_to_concept" AS
SELECT
    unit as unit_source_value,
    concept_id as unit_concept_id
FROM
    gcpt_lab_unit_to_concept),
"src_patients" AS
SELECT
    mimic_id AS person_id,
    subject_id
FROM
    src_patients),
"caregivers" AS
SELECT
    mimic_id AS provider_id,
    cgid
FROM
    caregivers),
"src_admissions" AS
SELECT
    mimic_id AS visit_occurrence_id,
    hadm_id
FROM
    src_admissions),
"row_to_insert" AS (SELECT
  measurement_id AS measurement_id,
    src_patients.person_id,
    coalesce(measurement_concept_id,0) as measurement_concept_id,
    measurement_datetime::date AS measurement_date,
    measurement_datetime AS measurement_datetime,
    2000000003 as measurement_type_concept_id,
    4172703 AS operator_concept_id,
    value AS value_as_number,
    null::bigint AS value_as_concept_id,
    unit_concept_id AS unit_concept_id,
    null::double precision AS range_low,
    null::double precision AS range_high,
    caregivers.provider_id AS provider_id,
    src_admissions.visit_occurrence_id AS visit_occurrence_id,
    null::bigint As visit_detail_id,
    d_items.label AS measurement_source_value,
    d_items.mimic_id AS measurement_source_concept_id,
    outputevents.unit_source_value AS unit_source_value,
    null::text AS value_source_value
FROM outputevents
LEFT JOIN gcpt_output_label_to_concept USING (itemid)
LEFT JOIN gcpt_lab_unit_to_concept ON gcpt_lab_unit_to_concept.unit_source_value ilike outputevents.unit_source_value
LEFT JOIN d_items USING (itemid)
LEFT JOIN src_patients USING (subject_id)
LEFT JOIN caregivers USING (cgid)
LEFT JOIN src_admissions USING (hadm_id))
INSERT INTO :OMOP_SCHEMA.measurement
(
      measurement_id,
    person_id,
    measurement_concept_id,
    measurement_date,
    measurement_datetime,
    measurement_type_concept_id,
    operator_concept_id,
    value_as_number,
    value_as_concept_id,
    unit_concept_id,
    range_low,
    range_high,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    measurement_source_value,
    measurement_source_concept_id,
    unit_source_value,
    value_source_value
)
SELECT
  row_to_insert.measurement_id,
    row_to_insert.person_id,
    row_to_insert.measurement_concept_id,
    row_to_insert.measurement_date,
    row_to_insert.measurement_datetime,
    row_to_insert.measurement_type_concept_id,
    row_to_insert.operator_concept_id,
    row_to_insert.value_as_number,
    row_to_insert.value_as_concept_id,
    row_to_insert.unit_concept_id,
    row_to_insert.range_low,
    row_to_insert.range_high,
    row_to_insert.provider_id,
    row_to_insert.visit_occurrence_id,
    visit_detail_assign.visit_detail_id,
    row_to_insert.measurement_source_value,
    row_to_insert.measurement_source_concept_id,
    row_to_insert.unit_source_value,
    row_to_insert.value_source_value
FROM row_to_insert
LEFT JOIN :OMOP_SCHEMA.visit_detail_assign
ON row_to_insert.visit_occurrence_id = visit_detail_assign.visit_occurrence_id
AND
(--only one visit_detail
(is_first IS TRUE AND is_last IS TRUE)
OR -- first
(is_first IS TRUE AND is_last IS FALSE AND row_to_insert.measurement_datetime <= visit_detail_assign.visit_end_datetime)
OR -- last
(is_last IS TRUE AND is_first IS FALSE AND row_to_insert.measurement_datetime > visit_detail_assign.visit_start_datetime)
OR -- middle
(is_last IS FALSE AND is_first IS FALSE AND row_to_insert.measurement_datetime > visit_detail_assign.visit_start_datetime AND row_to_insert.measurement_datetime <= visit_detail_assign.visit_end_datetime)
);


-- -------------------------------------------------------------------
-- Rule 6
-- weight
FROM
    inputevent_mv
-- -------------------------------------------------------------------


with
"src_patients" AS
SELECT
    mimic_id AS person_id,
    subject_id
FROM
    src_patients),
"src_admissions" AS
SELECT
    mimic_id AS visit_occurrence_id,
    hadm_id
FROM
    src_admissions),
"caregivers" AS
SELECT
    mimic_id AS provider_id,
    cgid
FROM
    caregivers),
"row_to_insert" as
(
select
          nextval('mimic_id_seq') as measurement_id
        ,
    person_id
        ,
    3025315 as measurement_concept_id  --loinc weight
        ,
    starttime::date as measurement_date
        ,
    starttime as measurement_datetime
        ,
    44818701 as measurement_type_concept_id --
FROM
    physical examination,
    4172703 as operator_concept_id
        ,
    patientweight as value_as_number
        ,
    null::integer as value_as_concept_id
        ,
    9529 as unit_concept_id --kilogram,
    null::numeric as range_low,
    null::numeric as range_high
        ,
    caregivers.provider_id
        ,
    visit_occurrence_id
        ,
    null::text as measurement_source_value
        ,
    null::integer as measurement_source_concept_id
        ,
    null::text as unit_source_value
        ,
    null::text as value_source_value
   
FROM
    inputevents_mv
        LEFT JOIN src_patients USING (subject_id)
        LEFT JOIN caregivers USING (cgid)
        LEFT JOIN src_admissions USING (hadm_id)
    WHERE patientweight is not null
)
INSERT INTO :OMOP_SCHEMA.measurement
(
      measurement_id,
    person_id,
    measurement_concept_id,
    measurement_date,
    measurement_datetime,
    measurement_type_concept_id,
    operator_concept_id,
    value_as_number,
    value_as_concept_id,
    unit_concept_id,
    range_low,
    range_high,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    measurement_source_value,
    measurement_source_concept_id,
    unit_source_value,
    value_source_value
)
SELECT
  row_to_insert.measurement_id,
    row_to_insert.person_id,
    row_to_insert.measurement_concept_id,
    row_to_insert.measurement_date,
    row_to_insert.measurement_datetime,
    row_to_insert.measurement_type_concept_id,
    row_to_insert.operator_concept_id,
    row_to_insert.value_as_number,
    row_to_insert.value_as_concept_id,
    row_to_insert.unit_concept_id,
    row_to_insert.range_low,
    row_to_insert.range_high,
    row_to_insert.provider_id,
    row_to_insert.visit_occurrence_id,
    visit_detail_assign.visit_detail_id,
    row_to_insert.measurement_source_value,
    row_to_insert.measurement_source_concept_id,
    row_to_insert.unit_source_value,
    row_to_insert.value_source_value
FROM row_to_insert
LEFT JOIN :OMOP_SCHEMA.visit_detail_assign
ON row_to_insert.visit_occurrence_id = visit_detail_assign.visit_occurrence_id
AND
(--only one visit_detail
(is_first IS TRUE AND is_last IS TRUE)
OR -- first
(is_first IS TRUE AND is_last IS FALSE AND row_to_insert.measurement_datetime <= visit_detail_assign.visit_end_datetime)
OR -- last
(is_last IS TRUE AND is_first IS FALSE AND row_to_insert.measurement_datetime > visit_detail_assign.visit_start_datetime)
OR -- middle
(is_last IS FALSE AND is_first IS FALSE AND row_to_insert.measurement_datetime > visit_detail_assign.visit_start_datetime AND row_to_insert.measurement_datetime <= visit_detail_assign.visit_end_datetime)
)
;
