/*
MIMIC 2.0
Analysis or troubleshooting by tables
to run the ETL against the new version of MIMIC
-- */

-- -------------------------------------------------------------------
-- mimiciv_hosp.microbiologyevents 
-- analysis to remove d_micro table from the ETL since the table is not longer available
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- old itemid:
--
-- spec_itemid
-- test_itemid
-- org_itemid
-- -------------------------------------------------------------------

-- 
SELECT 
    COUNT(DISTINCT CONCAT(org_itemid, '|', org_name)) AS dist_pair_count,
    COUNT(DISTINCT org_itemid) AS dist_item_count,
    COUNT(DISTINCT org_name) AS dist_name_count
from `physionet-data`.mimiciv_hosp.microbiologyevents
where org_name is not null or org_itemid is not null
-- dist_pair_count	dist_item_count	dist_name_count
-- 651	651	647
;

SELECT 
    COUNT(DISTINCT CONCAT(ab_itemid, '|', ab_name)) AS dist_pair_count,
    COUNT(DISTINCT ab_itemid) AS dist_item_count,
    COUNT(DISTINCT ab_name) AS dist_name_count
from `physionet-data`.mimiciv_hosp.microbiologyevents
where ab_name is not null or ab_itemid is not null
-- dist_pair_count dist_item_count dist_name_count
-- 27  27  27
;

-- d_micro is no longer available both in mimic_hosp and mimiciv_hosp
select distinct category
from `odysseus-mimic-dev.mimiciv_full_cdm_2021_04_19.src_d_micro`
order by category
-- category
-- ANTIBIOTIC
-- MICROTEST
-- ORGANISM
-- SPECIMEN
;

-- how does microbiologyevents cover src_d_micro
with org AS 
(
    select distinct org_itemid
    from `physionet-data`.mimiciv_hosp.microbiologyevents
    where org_itemid is not null
)
select dm.category, count(*) as org_count, count(dm.itemid) as dm_count
from org
left join `odysseus-mimic-dev.mimiciv_full_cdm_2021_04_19.src_d_micro` dm
on org_itemid = dm.itemid
group by dm.category
-- category    org_count   dm_count
-- ORGANISM    646 646
--     5   0
;

-- generate proof of concept temporary table
-- python scripts/run_workflow.py -e conf/full.etlconf z_more/tmp_src_d_micro.sql

SELECT COUNT(*)
FROM  `odysseus-mimic-dev.mimiciv_full_cdm_2022_07_15.tmp_src_d_micro` tdm
-- 960
;
SELECT COUNT(*)
FROM `odysseus-mimic-dev.mimiciv_full_cdm_2021_04_19.src_d_micro` dm
-- 1335
;

-- compare the generated tmp_src_d_micro and old src_d_micro
SELECT
    COALESCE(tdm.category, dm.category) AS category, 
    COUNT(tdm.itemid) AS new_count, 
    COUNT(dm.itemid) AS old_count,
    COUNT(IF(tdm.label = dm.label, 1, NULL)) AS names_match_count
FROM  `odysseus-mimic-dev.mimiciv_full_cdm_2022_07_15.tmp_src_d_micro` tdm
FULL JOIN `odysseus-mimic-dev.mimiciv_full_cdm_2021_04_19.src_d_micro` dm
ON tdm.itemid = dm.itemid
GROUP BY category
ORDER BY category, new_count, old_count
-- category    new_count   old_count   names_match_count
-- ANTIBIOTIC  27  35  27
-- MICROTEST   177 235 169
-- ORGANISM    651 938 646
-- SPECIMEN    105 127 102
;


-- -------------------------------------------------------------------
-- mimiciv_hosp.admissions
-- -------------------------------------------------------------------

SELECT distinct ethnicity
FROM `odysseus-mimic-dev.mimiciv_full_cdm_2021_04_19.src_admissions`

select distinct race
from `physionet-data.mimiciv_hosp.admissions`

-- rename race to ethnicity on staging
