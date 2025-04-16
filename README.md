# MIMIC IV to OMOP CDM Conversion #

(Demo data link:  https://doi.org/10.13026/p1f5-7x35)

The project implements an ETL conversion of MIMIC IV PhysioNet dataset to OMOP CDM format.

* Version TBD (1+)


## Concepts / Philosophy

### The ETL is based on five logic steps:
* Create a snapshot of the source data. The snapshot data is stored in staging source tables with prefix "src_".
* Clean source data: filter out rows to be not used, format values, apply some business rules. This step results in creating "clean" intermediate tables with prefix "lk_" and suffix "clean".
* Map distinct source codes to concepts in vocabulary tables. The step results in creating intermediate tables with prefix "lk_" and suffix "concept".
    * Custom mapping is implemented in custom concepts generated in vocabulary tables beforehand.
* Join cleaned data and mapped codes. The step results in creating intermediate tables with prefix "lk_" and suffix "mapped".
* Distribute mapped data by target CDM tables according to target_domain_id values.

### The ETL process encapsulates the following workflows:
* **vocabulary refresh**, which loads vocabulary and custom mapping data from local folders to the vocabulary dataset. 
* **waveform collecting**, which loads parsed waveform data from google storage bucket to the waveform staging dataset.
* **ddl**, which creates empty cdm tables in the ETL dataset.
* **staging**, which creates a snapshot of the source tables and vocabulary tables in the ETL dataset.
* **etl**, which performs the ETL logic.
* **ut**, which runs internal unit tests.
* **metrics**, which builds metric report data for internal QA.
* **unload**, which copies CDM and vocabulary tables to the final CDM OMOP dataset.

* On the POC level **waveform collecting** workflow is represented by a single script, `scripts/wf_read.py`, which iterates through subfolders in the given bucket path. It populates `wf_header` table with folder structure data, and `wf_details` table with data from CSV files found there. These tables are used as source tables for poc_2 unit in `measurement` and `condition_occurrence` tables. POC_3 data is loaded and prepared manually (todo: provide both manual scripts)
* All workflows from **ddl** to **metrics** operate with so called "ETL" dataset, where intermediate tables are created, and all tables have prefixes according to their roles. I.e. voc for vocabulary tables, src for snapshot of source tables, lk for intermediate aka lookup tables, cdm for target CDM tables. Most of the tables have additional fields: unit_id, load_table_id, load_row_id, trace_id.
* The last step, **unload**, populates the final OMOP CDM dataset, also referred as "ATLAS" dataset. Only CDM and vocabulary tables are kept here, prefixes and additional fields are removed. The final OMOP CDM dataset can be analysed with OHDSI tools as ATLAS or DQD.


### How to run the conversion

#### To run the ETL pipeline end-to-end
* load the latest standard OMOP vocabularies from http://athena.ohdsi.org 
    * create a working copy of the loaded vocabularies, where custom mapping data will be added to
* get custom mapping vocabulary _delta tables from https://github.com/TuftsCTSI/CVB/tree/main/MIMIC/Ontology
* get other custom mapping vocabulary _delta tables, if needed
* set variables in vocabulary_refresh/README.md
    * run vocabulary refresh commands given below from directory "vocabulary_refresh"
* set the project variables in `conf/*.etlconf`
    * run script "wf_read" to load waveform sample data if needed
    * run workflow commands below in the given sequence 
        * in the workflow commands <env> is the "environment" name, which equals "dev" for the demo dataset and "full" for the full set

* set the project root (location of this file) as the current directory
```
cd vocabulary_refresh
python vocabulary_refresh.py -s10
python vocabulary_refresh.py -s20
python vocabulary_refresh.py -s30
cd ../
python scripts/wf_read.py -e conf/<env>.etlconf
python scripts/run_workflow.py -e conf/<env>.etlconf -c conf/workflow_ddl.conf
python scripts/run_workflow.py -e conf/<env>.etlconf -c conf/workflow_staging.conf
python scripts/run_workflow.py -e conf/<env>.etlconf -c conf/workflow_etl.conf
python scripts/run_workflow.py -e conf/<env>.etlconf -c conf/workflow_ut.conf
python scripts/run_workflow.py -e conf/<env>.etlconf -c conf/workflow_metrics.conf
python scripts/run_workflow.py -e conf/<env>.etlconf -c conf/workflow_unload.conf
```

#### To look at UT and Metrics reports
* see metrics dataset name in the corresponding `.etlconf` file

```SQL
-- Metrics - row count
SELECT * FROM metrics_dataset.me_total ORDER BY table_name;
-- Metrics - person and visit summary
SELECT
    category, name, count AS row_count
FROM metrics_dataset.me_persons_visits ORDER BY category, name;
-- Metrics - Mapping rates
SELECT
    table_name, concept_field, 
    count   AS rows_mapped, 
    percent AS percent_mapped,
    total   AS rows_total
FROM metrics_dataset.me_mapping_rate 
ORDER BY table_name, concept_field
;
-- Metrics - Mapped and Unmapped source values
SELECT 
    table_name, concept_field, category, source_value, concept_id, concept_name,
    count       AS row_count,
    percent     AS rows_percent
FROM metrics_dataset.me_tops_together 
ORDER BY table_name, concept_field, category, count DESC;
-- UT report
SELECT report_starttime, table_id, test_type, field_name, test_passed
FROM mimiciv_full_metrics_2023_02_17.report_unit_test
order by table_id, report_starttime
-- WHERE NOT test_passed 
;
```

#### More option to run ETL parts
* Run a workflow:
    * with local variables:
        ```
        python scripts/run_workflow.py -c conf/workflow_etl.conf
        ```
        * copy "variables" section from file.etlconf
    * with global variables:
        ```
        python scripts/run_workflow.py -e conf/dev.etlconf -c conf/workflow_etl.conf
        ```
* Run explicitly named scripts (space delimited):
    ```
    python scripts/run_workflow.py -e conf/dev.etlconf etl/etl/cdm_drug_era.sql
    ```
* Run in background:
    ```
    nohup python scripts/run_workflow.py -e conf/full.etlconf -c conf/workflow_etl.conf > ../out_full_etl.out &
    ```
* Continue after an error:
    ```
    nohup python scripts/run_workflow.py -e conf/full.etlconf -c conf/workflow_etl.conf etl/etl/cdm_observation.sql etl/etl/cdm_observation_period.sql etl/etl/cdm_fact_relationship.sql etl/etl/cdm_condition_era.sql etl/etl/cdm_drug_era.sql etl/etl/cdm_dose_era.sql etl/etl/cdm_cdm_source.sql >> ../out_full_etl.out &
    ```


### Changelog (latest first)

**2024-04-16**
* Initial update from v1.0.0 to a new approach for custom vocabularies
* Update to vocabulary setup to use _delta custom vocabulary tables instead of gcpt tables

**2023-02-17**

* MIMIC 2.2 is issued. Run ETL on MIMIC 2.2.
* minor change to measurement.value_source_value: 
    * populate the field always instead of populating only when value_as_number is null
* minor change to custom mapping vocabularies: 
    * mimiciv_drug_ndc,
    * mimiciv_drug_route,
    * mimiciv_meas_lab_loinc,
    * mimiciv_obs_drgcodes,
    * mimiciv_proc_itemid
* run with OMOP vocabularies v16-JAN-23

**2022-09-09**

* MIMIC 2.0 is issued: run ETL on MIMIC 2.0.
* scripts/bq_run_script.py is updated to fit current BQ requirement of single line queries
* minor changes in the ETL code to match MIMIC 2.0
    * @core_dataset now points to @hosp_dataset
    * table d_micro is no longer available in physionet-data. A replacement src_d_micro is generated from microbiologyevents. (see "z_more/MIMIC 2.0 affected tables.sql", etl/staging/st_hosp.sql)

**2021-05-17**

* etl/unload/\*extra.sql is added
* custom mapping review scripts are added to crosswalk_csv folder
* bugfixes


**2021-03-08**

* Date shift bugfix
    * cdm_person - birth_year is updated
    * lk_procedure - events earlier than one year before birth_year are filtered out

**2021-02-22**

* All events tables
    * review and re-map type_concept_id
* Specimen
    * due to target domain in custom mapping some specimen are put to Procedure and Observation
* Procedure
    * a bug is fixed in lk_hcpcs_concept table. HCPCS procedures are in the target table now
* Measurement
    * trim values in chartevents
    * pick antibiotic resistance custom vocabulary
* Observation
    * add 'mimiciv_obs_language' custom vocaulary (just english, but it covers 17% of observation rows)
* Dose_era
    * a bug is fixed. Now dose_era is populated properly
* Custom mapping is added and updated
    * for visit_detail, specimen, measurement, drug, units, waveforms


**2021-02-08**

* Set version v.1.0

* Drug_exposure table
    * pharmacy.medication is replacing particular values of prescription.drug
    * source value format is changed to COALESCE(pharmacy.medication.selected, prescription.drug) || prescription.prod_strength
* Labevents mapping is replaced with new reviewed version
    * vocabulary affected: mimiciv_meas_lab_loinc
    * lk_meas_labevents_clean and lk_meas_labevents_mapped are changed accordingly
* Unload for Atlas
    * Technical fields unit_id, load_row_id, load_table_id, trace_id are removed from Atlas devoted tables
* Delivery export script
    * tables are exported to a single directory and single files. If a table is too large, it is exported to multiple files
* Bugfixes and cleanup
* Real environmental names are replaced with placeholders


**2021-02-01**

* Waveform POC-2 is created for 4 MIMIC III Waveform files uploaded to the bucket
    * iterate through the folders tree, capture metadata and load the CSVs
* Bugfixes


**2021-01-25**

* Mapping improvement
* New visit logic for measurement (picking visits by event datetime)
* Bugfixes


**2021-01-13**

* Export and import python scripts:
    * `scripts/delivery/export_from_bq.py`
        * the script is adjusted to export BQ tables from Atlas dataset to GS bucket, and optionally to the local storage to CSV files
    * `scripts/delivery/load_to_bq.py`
        * the script is adjusted to load the exported CSVs from local storage or from the bucket to the given target BQ dataset, created automatically


**2020-12-14**

* TUF-55 Custom mapping derived from MIMIC III
    * A little more custom mapping
* TUF-77 UT, QA and Metrics for tables with basic logic
    * UT for all tables:
        * Unique fields
        * FK to person_id, visit_occurrence_id, some individual FK


**2020-12-03**

* TUF-10 Basic logic for CDM tables based on MIMIC-III logic
    * Bugfix: Visit and Visit_detail FK violation - fixed
* TUF-55 Custom mapping derived from MIMIC III
    * Measurement.value_as_concept_id - mapped partially
    * Unit - mapping is loaded
* TUF-77 UT, QA and Metrics for tables with basic logic
    * Make Top100 step faster - started

**2020-12-01**

* TUF-10 Basic logic for CDM tables based on MIMIC-III logic
    * Bugfix: Person.race_concept_id and Co - is fixed
    * Bugfix: Measurement.unit_concept_id - is fixed
    * Bugfix: Measurement.value_as_concept_id - copied for mapping
    * Bugfix: Visit_detail.services.visit_detail_concept_id and Co - copied for mapping
    * Bugfix: Visit and Visit_detail FK violation - tables implementation is refactored
* TUF-75 Basic Orchestration
    * Bugfix: runs end-to-end if a given script is not found - fixed
        * bq_run_script.py now stops if any of given scripts are not found, and reports the names


**2020-11-30**

* TUF-10 Basic logic for CDM tables based on MIMIC-III logic
    * Minor update to Care_site
    * Updates to Measurement (chartevents)
        * 4 types are added: Heart Rate, Respiratory Rate, O2 Saturation, Heart Rythm
    * Fact_relationship is added
    * Bugfix to visit_source_value (hadm_id -> concat(subject_id, '|', hadm_id))
        * Visit_occurrence, Visit_detail, and all event tables
* TUF-55 Custom mapping derived from MIMIC III
    * Microbiology mapping
        * Microtest is created
        * Organism is updated
    * Admission mapping - 3 vocabularies added
    * Place of service is added
* TUF-75 Basic Orchestration
    * global config (etlconf) is added to bq_run_scirpt.py and run_workflow.py
    * Bug: run_workflow.py runs end-to-end if the given script is not found
* TUF-77 UT, QA and Metrics for tables with basic logic
    * metrics_gen is added (from mimic POC in May 2020)
    * gen_bq_ut_basic.py is created, in progress
    * src_statistics.sql is started (source row counts)
* TUF-83 Complete run on the Full set
    * full.etlconf is added, end-to-end run is started


**2020-11-20**

* TUF-10 Basic logic for CDM tables based on MIMIC-III logic
    * Mapping for Visits is updated
    * Condition_era, Drug_era, Dose_era are added
* TUF-55 Custom mapping derived from MIMIC III
    * Microbiology mapping - 4 vocabularies are added, 1 to be created
    * Analysis for other mapping
    * Minor updates


**2020-11-13**

* TUF-10 Basic logic for CDM tables based on MIMIC-III logic
    * TUF-19 Measurement - Rule 3 (microbiology) - done (without tests)
    * TUF-58 Specimen - done (without tests)
* TUF-75 Basic Orchestration
    * run_job.py is renamed to run_workflow.py, 
    * Variables section is added to configs for bq_run_script.py and run_workflow.py
    * Dataset names are replaced in ut/\*.sql and qa/\*.sql
    * run_workflow.py is updated, the issue with not stopping on an error is fixed
    * Nice output is added to bq_run_script


**2020-11-12**

* TUF-10 Basic logic for CDM tables based on MIMIC-III logic
    * TUF-19 Measurement - Rule 3 (microbiology) ready to run


**2020-11-10**

* TUF-10 Basic logic for CDM tables based on MIMIC-III logic
    * TUF-19 Measurement - Rule 3 (microbiology) is in progress
    * TUF-58 Specimen - ready to run
    * Other updates


**2020-10-27**

* TUF-10 Basic logic for CDM tables based on MIMIC-III logic
    * TUF-20 Death table - done
    * TUF-19 Measurement - Rule 2 (chartevent) is started
* TUF-76 Bugfix according to Achilles Heel Report
    * Person count - done
    * Condition mapping rate - done
    * Observation periods - done
    * For next updates:
        * Drug concepts in Condition
        * Observation value_as fields all null
        * Event dates before year of birth
* TUF-75 Basic Orchestration
    * Fixed: dealing with quotes (") inside a query
    * Known issue: do not use semicolon (;) inside comments


**2020-10-26**

* TUF-10 Basic logic for CDM tables based on MIMIC-III logic
    * TUF-19 Minor fixes for Measurement lookups
    * TUF-51 Device_exposure - done with Rule 1 lk_drug_mapped
    * TUF-58 Specimen - is started
* TUF-75 Basic Orchestration
    * run_job.sql is created
* ATLAS dataset is populated: `mimiciv_202010_cdm_531`


**2020-10-21**

* TUF-10 Basic logic for CDM tables based on MIMIC-III logic
    * TUF-19 Measurement - basic is done, Rule 1 (labevents) and Rule 10 (wf poc demo) are implemented
    * TUF-53 Observation_period - basic is done
* TUF-75 Basic Orchestration
    * Config design in progress


**2020-10-21**

* TUF-10 Basic logic for CDM tables based on MIMIC-III logic
    * TUF-23 Drug_exposure - basic is done, Rule 1 is implemented
    * TUF-55 Custom mapping - vocabulary references in the code are updated, CSV's folder is moved
* TUF-50 Create dataset for ATLAS - the SQL script generator is created, script is generated


**2020-10-18**

* TUF-10 Basic logic for CDM tables based on MIMIC-III logic
    * TUF-23 Drug_exposure - in progress
    * TUF-20 Death - in progress
    * TUF-55 Custom mapping - CSV's for implemented tables are converted (todo: rename custom vocabularies in the code)


**2020-10-12**

* TUF-10 Basic logic for CDM tables based on MIMIC-III logic
    * TUF-18 Condition_occurrence - basic logic, UT, QA are created
    * TUF-22 Procedure_occurrence - basic logic is created, UT and QA - to do
    * TUF-21 Observation - from Procedure III is started, from Observation III is next
    * TUF-23 Drug_exposure - started
    * TUF-20 Death - in progress


**2020-10-01**

* TUF-10 Basic logic for CDM tables based on MIMIC-III logic
    * TUF-19 Measurement - Rule 1 (lab from labevents) is implemented, dry run is done
    * MIMIC III extras/concept custom mapping CSVs are loaded to BQ to raw tables (for reference)


**2020-09-24**

* TUF-10 Basic logic for CDM tables based on MIMIC-III logic
    * TUF-17 Visit_detail - basic ETL, UT, QA are done (see Known issues in the ETL script)
    * TUF-19 Measurement - start


**2020-09-22**

* TUF-10 Basic logic for CDM tables based on MIMIC-III logic
    * TUF-15 Visit_occurrence - basic ETL, UT, QA are done, proper to do
    * TUF-16 Orchestration overhead - config design
    * TUF-17 Visit_detail - basic ETL is in progress


**2020-09-18**

* TUF-11 Load Vocabularies
    * vocabulary_refresh.conf - updated
    * sql scripts - project.dataset template is set
    * py scripts - some error handling is added, minor bug is fixed
    * Athena vocabularies are loaded to `vocabulary_2020_09_11`


**2020-09-15**

* TUF-10 Basic logic for CDM tables based on MIMIC-III logic
    * TUF-12 Location - Hardcode the single location
    * TUF-13 Care_site - basic ETL, UT, QA are done, to add proper mapping and ID
    * TUF-14 Person - basic ETL, UT, QA are done, to add proper mapping and ID


**2020-09-11**

Start development
* The project includes:
    * folders
    * DDL script
    * vocabulary_refresh scripts set

### The End ###


### Concepts / best practice / lessons learned ###

* keep vocabularies in a separate dataset standard, and add custom mapping each time vocabs are copied to the cdm dataset? 
    * usual practice: apply custom mapping to vocabs in the vocab dataset
    * plus of this alternative: no clean-up is needed, we just add the custom concepts
* working on a task, 
    * create a branch / complete prev PR
    * declare a comparison dataset, i.e. the result of previously completed task
    * create a new working dataset named mimiciv_cdm_task_id_developer_yyyy_mm_dd
    * create a UAT dataset when a significant piece of work is completed and tested
        * the name is mimiciv_uat_yyyy_mm_dd
        * uat = copy of the best and lates cdm
    * cleanup cdm datasets when uat is created or earlier if they are not needed anymore

### Concepts / Phylosophy ###

The ETL is based on the four steps.
* Clean source data: filter out rows to be not used, format values, apply some business rules. Create intermediate tables with postfix "clean".
* Map distinct source codes to concepts in vocabulary tables. Create intermediate tables with postfix "concept".
    * Custom mapping is implemented in custom concepts generated in vocabulary tables beforehand.
* Join cleaned data and mapped codes. Create intermediate tables with postfix "mapped".
* Distribute mapped data by target cdm tables according to target_domain_id values.

Field unit_id is composed during the ETL steps. From right to left: source table name, initial target table name abbreviation, final target table name or abbreviation. For example: unit_id = 'drug.cond.diagnoses_icd' means that the rows in this unit_id belong to Drug_exposure table, inially were prepared for Condition_occurrence table, and its original is source table diagnoses_icd.




### How do I get set up? ###

* Summary of set up
* Configuration
* Dependencies
* Database configuration
* How to run tests
* Deployment instructions

### Contribution guidelines ###

* Writing tests
* Code review
* Other guidelines
