



run generated code to build metrics:

python ../../scripts/bq_run_script.py me_totals_from_conf.sql
python ../../scripts/bq_run_script.py me_top100_from_conf.sql
python ../../scripts/bq_run_script.py me_mapping_rate_from_conf.sql
python ../../scripts/bq_run_script.py me_tops_together_from_conf.sql
python ../../scripts/bq_run_script.py me_see_result_from_conf.sql




## metrics workflow ##

1) Prepare scripts to build metrics (15 min?)
    * Update me_concept_fields.conf if needed.
    * Update ETL and metrics dataset names in me_gen_queries_from_conf.py script. 
    * Run from current folder: 
        `python me_gen_queries_from_conf.py`
    * Update ETL and metrics dataset names in me_persons_visits.sql

1) Build the metrics
    * Create metrics dataset if needed.
    * Run:
        2.1)
        `python bq_run_script.py me_totals_from_conf.sql`
        2.2) (10 min?)
        `python bq_run_script.py me_persons_visits.sql`
        2.3) (15 min?)
        `python bq_run_script.py me_mapping_rate_from_conf.sql`
        2.4) (~30 min)
        `python bq_run_script.py me_top100_from_conf.sql`
        2.5)
        `python bq_run_script.py me_tops_together_from_conf.sql`


1) View results and copy it to XSLX
    * Update metrics dataset name in the queries below.
    * Run in web BQ:
        3.1)
        select * from metrics_dataset.me_persons_visits order by category, name;
        3.2)
        select * from metrics_dataset.me_mapping_rate order by table_name, concept_field;
        3.3)
        each query in `me_see_totals_from_conf.sql`
        or all together
        select * from metrics_dataset.me_tops_together order by table_name, concept_field, category;
        
