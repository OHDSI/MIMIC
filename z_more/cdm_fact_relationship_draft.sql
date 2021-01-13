-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Populate cdm_observation table
-- 
-- Dependencies: run after 
--      cdm_care_site,
--      cdm_drug_exposure
-- -------------------------------------------------------------------



-- -------------------------------------------------------------------
-- care_site
-- -------------------------------------------------------------------

WITH
wardid as (
    select distinct coalesce(curr_careunit,'UNKNOWN') as curr_careunit, curr_wardid
    from transfers
),
gcpt_care_site AS (
    SELECT
    nextval('mimic_id_seq') as care_site_id
    , CASE
    WHEN wardid.curr_careunit IS NOT NULL THEN format_ward(care_site_name, curr_wardid)
    ELSE care_site_name end as care_site_name
    , place_of_service_concept_id as place_of_service_concept_id
    , care_site_name as care_site_source_value
    , place_of_service_source_value
    FROM gcpt_care_site
    left join wardid on care_site_name = curr_careunit
),
insert_relationship_itself AS (
    INSERT INTO :OMOP_SCHEMA.fact_relationship
    (domain_concept_id_1, fact_id_1, domain_concept_id_2, fact_id_2, relationship_concept_id)
    SELECT
    57 AS domain_concept_id_1 -- 57    Care site
    , care_site_id AS fact_id_1
    , 57 AS domain_concept_id_2 -- 57    Care site
    , care_site_id AS fact_id_2 
    , 46233688 as relationship_concept_id -- care site has part of care site (any level is part of himself)
    FROM gcpt_care_site
),
insert_relationship_ward_hospit AS ( --link the wards to BIDMC hospital
    INSERT INTO :OMOP_SCHEMA.fact_relationship
    (domain_concept_id_1, fact_id_1, domain_concept_id_2, fact_id_2, relationship_concept_id)
    SELECT
    57 AS domain_concept_id_1 -- 57    Care site
    , gc1.care_site_id AS fact_id_1
    , 57 AS domain_concept_id_2 -- 57    Care site
    , gc2.care_site_id AS fact_id_2 
    , 46233688 as relationship_concept_id -- care site has part of care site (any level is part of himself)
    FROM gcpt_care_site gc1
    JOIN gcpt_care_site gc2 ON gc2.care_site_name = 'BIDMC' 
    WHERE gc1.care_site_name ~ ' ward '
)
INSERT INTO :OMOP_SCHEMA.CARE_SITE
(
    care_site_id
    , care_site_name
    , place_of_service_concept_id
    , care_site_source_value
    , place_of_service_source_value
)
SELECT 
    gcpt_care_site.care_site_id
    , gcpt_care_site.care_site_name
    , gcpt_care_site.place_of_service_concept_id
    , care_site_source_value
    , place_of_service_source_value
FROM gcpt_care_site;



-- -------------------------------------------------------------------
-- drug_exposure
-- Rule 2, inputevents
-- -------------------------------------------------------------------


INSERT INTO `@etl_project`.@etl_dataset.cdm_fact_relationship
SELECT DISTINCT
    13 As fact_id_1 --Drug,
    mv2.drug_exposure_id AS domain_concept_id_1,
    13 As fact_id_2 --Drug,
    mv1.drug_exposure_id AS domain_concept_id_2,
    44818791 AS relationship_concept_id -- Has temporal context [SNOMED]
FROM
    `@etl_project`.@etl_dataset.imv mv1
LEFT JOIN imv mv2
        ON  (mv2.orderid = mv1.linkorderid AND mv2.is_leader IS TRUE)
;

INSERT INTO `@etl_project`.@etl_dataset.cdm_fact_relationship
SELECT DISTINCT
    13 As fact_id_1 --Drug,
    mv2.drug_exposure_id AS domain_concept_id_1,
    13 As fact_id_2 --Drug,
    mv1.drug_exposure_id AS domain_concept_id_2,
    44818784 AS relationship_concept_id -- Has associated procedure [SNOMED]
FROM
    `@etl_project`.@etl_dataset.imv mv1
LEFT JOIN imv mv2
        ON  (mv2.orderid = mv1.orderid AND mv2.is_orderid_leader IS TRUE)
;



-- -------------------------------------------------------------------
-- measurement-specimen
-- Rule 1 (labevents)
-- -------------------------------------------------------------------

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

-- -------------------------------------------------------------------
-- measurement-specimen
-- Rule 2 (chartevents)
-- -------------------------------------------------------------------

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
