-- ----------------------------------------------------------------------------------------------
-- Alternative statistics for CDM tables
--
-- Number of  persons
-- Total
-- Race/Ethnicity Breakdown
-- Number of visits
-- Total
-- Stratified by visit_concept_id
-- Top 100 Mapped and Unmapped values on event tables
-- Total count on event tables
-- ----------------------------------------------------------------------------------------------

-- ----------------------------------------------------------------------------------------------
-- assuming that databases are:
-- cdm:             omop
-- vocabularies:    mimic_vocabulary
-- metrics output:  mimic_ metrics
--
-- create database mimic_ metrics;
-- ----------------------------------------------------------------------------------------------

drop table if exists @metrics_project.@metrics_dataset.me_persons_visits;
create table @metrics_project.@metrics_dataset.me_persons_visits
(
        category string,
        name string,
        count int64
);

-- ----------------------------------------------------------------------------------------------
-- Number of  persons
-- ----------------------------------------------------------------------------------------------
insert into @metrics_project.@metrics_dataset.me_persons_visits
select 'Number of persons' as category, 'Total' as name,  count(*) as count
from @etl_project.@etl_dataset.cdm_person
;

insert into @metrics_project.@metrics_dataset.me_persons_visits
select 'Number of persons by Race' as category, vc.concept_name as name,  count(*) as count
from @etl_project.@etl_dataset.cdm_person per
left join @etl_project.@etl_dataset.voc_concept vc
    on per.race_concept_id = vc.concept_id
group by vc.concept_name
order by vc.concept_name
;


insert into @metrics_project.@metrics_dataset.me_persons_visits
select 'Number of persons by Ethnicity' as category, vc.concept_name as name,  count(*) as count
from @etl_project.@etl_dataset.cdm_person per
left join @etl_project.@etl_dataset.voc_concept vc
    on per.ethnicity_concept_id = vc.concept_id
group by vc.concept_name
order by vc.concept_name
;

-- ----------------------------------------------------------------------------------------------
-- Number of visits
-- Total
-- Stratified by visit_concept_id
-- ----------------------------------------------------------------------------------------------

insert into @metrics_project.@metrics_dataset.me_persons_visits
select 'Number of visits' as category, 'Total' as name,  count(*) as count
from @etl_project.@etl_dataset.cdm_visit_occurrence
;


insert into @metrics_project.@metrics_dataset.me_persons_visits
select 'Number of visits by visit_concept_id' as category, vc.concept_name as name,  count(*) as count
from @etl_project.@etl_dataset.cdm_visit_occurrence vis
left join @etl_project.@etl_dataset.voc_concept vc
    on vis.visit_concept_id = vc.concept_id
group by vc.concept_name
order by vc.concept_name
;

select * from @metrics_project.@metrics_dataset.me_persons_visits order by category, name;
