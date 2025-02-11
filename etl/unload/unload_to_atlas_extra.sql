-- Unload to ATLAS
-- extra tables (d_items to concept)

CREATE OR REPLACE TABLE @atlas_project.@atlas_dataset.d_items_to_concept AS 
SELECT
    *
FROM @etl_project.@etl_dataset.d_items_to_concept
;

CREATE OR REPLACE TABLE @atlas_project.@atlas_dataset.d_labitems_to_concept AS 
SELECT
    *
FROM @etl_project.@etl_dataset.d_labitems_to_concept
;

CREATE OR REPLACE TABLE @atlas_project.@atlas_dataset.d_micro_to_concept AS 
SELECT
    *
FROM @etl_project.@etl_dataset.d_micro_to_concept
;

