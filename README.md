# MIMIC IV to OMOP CDM Conversion #

### What is this repository for? ###

* Quick summary
* Version
* [Learn Markdown](https://bitbucket.org/tutorials/markdowndemo)

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

### Who do I talk to? ###

* Repo owner or admin
* Other community or team contact

### Change Log (latest first) ###

**2020-09-21**

* TUF-10 Implement basic logic based on MIMIC III conversion
    * Tables implemented: cdm_person, cdm_care_site, cdm_location, cdm_visit_occurrence
    * Tables in progress: cdm_visit_detail, cdm_death
* TUF-11 Load Vocabularies
    * Athena vocabularies are loaded to `vocabulary_2020_09_11`

Start development
* The project includes:
    * folders
    * DDL script
    * vocabulary_refresh scripts set

### The End ###


### Concepts / best practice / lessons learned ###

* keep vocabularies in a separate dataset standard, and add custom mapping each time vocabs are copied to the cdm dataset? 
    * usual practice: apply custom mapping to vocabs in the vocab dataset
    * plus of this alternative: no clean-up is neede, we just add the custom concepts
* working on a task, 
    * create a branch / complete prev PR
    * declare a comparison dataset, i.e. the result of previously completed task
    * create a new working dataset named mimiciv_cdm_task_id_developer_yyyy_mm_dd
    * create a UAT dataset when a significant piece of work is completed and tested
        * the name is mimiciv_uat_yyyy_mm_dd
        * uat = copy of the best and lates cdm
    * cleanup cdm datasets when uat is created or earlier if they are not needed anymore

