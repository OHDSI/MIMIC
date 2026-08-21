-- -------------------------------------------------------------------
-- MIMIC Waveform ETL
-- -------------------------------------------------------------------

DECLARE bad_groups INT64;
DECLARE bad_time INT64;

-- Preflight 1: validate group_id grain is unique for person/visit/session interval

SET bad_groups = (
  SELECT COUNT(*) FROM (
    SELECT group_id
    FROM @etl_project.@etl_dataset.waveform_files
    GROUP BY group_id
    HAVING COUNT(DISTINCT person_id) > 1
       OR COUNT(DISTINCT visit_occurrence_id) > 1
       OR COUNT(DISTINCT visit_detail_id) > 1
       OR COUNT(DISTINCT session_start) > 1
       OR COUNT(DISTINCT session_end) > 1
  )
);
ASSERT bad_groups = 0 AS 'group_id violates expected grain (person, visit, session_start/end)';

-- Preflight 2: occurrence end must be >= start
SET bad_time = (
  SELECT COUNT(*) FROM (
    SELECT
      MIN(DATETIME(file_start)) AS s,
      MAX(DATETIME(file_end))   AS e
    FROM @etl_project.@etl_dataset.waveform_files
    GROUP BY group_id
    HAVING e < s
  )
);
ASSERT bad_time = 0 AS 'occurrence end < start for at least one group_id';

TRUNCATE TABLE @etl_project.@etl_dataset.cdm_waveform_occurrence;

INSERT INTO @etl_project.@etl_dataset.cdm_waveform_occurrence
SELECT
  `@etl_project.@etl_dataset`.obf_id(f.group_id, 32)  AS waveform_occurrence_id,
  -- Physiological monitoring from TuftsCTSI
  CAST(2081500001 AS INT64)                           AS waveform_occurrence_concept_id,
  MIN(f.person_id)                                    AS person_id,
  MIN(DATETIME(f.session_start))                      AS waveform_occurrence_start_datetime,
  MAX(DATETIME(f.session_end))                        AS waveform_occurrence_end_datetime,
  MIN(f.visit_occurrence_id)                          AS visit_occurrence_id,
  MIN(f.visit_detail_id)                              AS visit_detail_id,
  CAST(NULL AS INT64)                                 AS preceding_waveform_occurrence_id,
  -- WFDB from TuftsCTSI
  CAST(2082499975 AS INT64)                           AS waveform_format_concept_id,
  CAST(f.group_id AS STRING)                          AS waveform_occurrence_source_value,
  COUNT(*)                                            AS num_of_files,
  'WFDB'                                              AS waveform_format_source_value

  FROM @etl_project.@etl_dataset.waveform_files f
  GROUP BY
    f.group_id
;
