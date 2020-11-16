-- -------------------------------------------------------------------
-- MIMIC III custom mapping
-- mimic-omop/etl/StandardizedVocabularies/CONCEPT/etl.sql
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE `odysseus-mimic-dev`.mimiciii_extras_concept.concept_iii
(
    concept_id          INT64, 
    concept_name        STRING, 
    domain_id           STRING, 
    vocabulary_id       STRING, 
    concept_class_id    STRING, 
    concept_code        STRING, 
    valid_start_date    DATE, 
    valid_end_date      DATE
);

INSERT INTO `odysseus-mimic-dev`.mimiciii_extras_concept.concept_iii
(
    concept_id, 
    concept_name, 
    domain_id, 
    vocabulary_id, 
    concept_class_id, 
    concept_code, 
    valid_start_date, 
    valid_end_date
)
 VALUES
  (2000000000,'Stroke Volume Variation','Measurement','','Clinical Observation','MIMIC Generated','1979-01-01','2099-01-01')
, (2000000001,'L/min/m2','Unit','','','MIMIC Generated','1979-01-01','2099-01-01')
, (2000000002,'dynes.sec.cm-5/m2','Unit','','','MIMIC Generated','1979-01-01','2099-01-01')
, (2000000003,'Output Event','Type Concept','','Meas Type','MIMIC Generated','1979-01-01','2099-01-01')
, (2000000004,'Intravenous Bolus','Type Concept','','Drug Type','MIMIC Generated','1979-01-01','2099-01-01')
, (2000000005,'Intravenous Continous','Type Concept','','Drug Type','MIMIC Generated','1979-01-01','2099-01-01')
, (2000000006,'Ward and physical location','Type Concept','','Visit Type','MIMIC Generated','1979-01-01','2099-01-01')
, (2000000007,'Labs - Culture Organisms','Type Concept','','Meas Type','MIMIC Generated','1979-01-01','2099-01-01')
, (2000000008,'Labs - Culture Sensitivity','Type Concept','','Meas Type','MIMIC Generated','1979-01-01','2099-01-01')
, (2000000009,'Labs - Hemato','Type Concept','','Meas Type','MIMIC Generated','1979-01-01','2099-01-01')
, (2000000010,'Labs - Blood Gaz','Type Concept','','Meas Type','MIMIC Generated','1979-01-01','2099-01-01')
, (2000000011,'Labs - Chemistry','Type Concept','','Meas Type','MIMIC Generated','1979-01-01','2099-01-01')
, (2000000012,'Visit Detail','Metadata','Domain','Domain','MIMIC Generated','1979-01-01','2099-01-01')
, (2000000013,'Unwkown Ward','Visit Detail','Visit Detail','Visit_detail','MIMIC Generated','1979-01-01','2099-01-01')
;
