-- Create date shift table with person_id column for joining on during ETL
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.person_date_shift_lookup AS
SELECT 
    p.person_id,
    dsl.subject_id AS subject_id,
    dsl.offset_days
FROM 
    @etl_project.@etl_dataset.person p
INNER JOIN 
    @etl_project.@etl_dataset.date_shift_lookup dsl
    ON CAST(p.person_source_value AS INT64) = dsl.subject_id