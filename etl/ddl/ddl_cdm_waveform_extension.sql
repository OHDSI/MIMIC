

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_waveform_occurrence
(
  waveform_occurrence_id                  INT64     not null,
  waveform_occurrence_concept_id          INT64     not null,
  person_id                               INT64     not null,
  waveform_occurrence_start_datetime      DATETIME  not null,
  waveform_occurrence_end_datetime        DATETIME  not null,
  visit_occurrence_id                     INT64     not null,
  visit_detail_id                         INT64             ,
  preceding_waveform_occurrence_id        INT64             ,
  waveform_format_concept_id              INT64             ,
  waveform_occurrence_source_value        STRING            ,
  num_of_files                            INT64             ,
  waveform_format_source_value            STRING
)
;


CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_waveform_registry
(
  waveform_registry_id                    INT64     not null,
  waveform_occurrence_id                  INT64     not null,
  waveform_feature_id                     INT64             ,
  person_id                               INT64     not null,
  waveform_file_start_datetime	          DATETIME  not null,
  waveform_file_end_datetime	          DATETIME  not null,
  visit_occurrence_id                     INT64     not null,
  visit_detail_id                         INT64             ,
  file_extension_concept_id               INT64             ,
  file_extension_source_value             STRING    not null,
  waveform_source_file_uri                STRING            ,
  waveform_target_file_uri                STRING    not null
);


CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_waveform_channel_metadata
(
  waveform_channel_metadata_id            INT64     not null,
  waveform_registry_id                    INT64     not null,
  procedure_occurrence_id                 INT64             ,
  device_exposure_id                      INT64             ,
  waveform_channel_source_value           STRING            ,
  channel_concept_id                      INT64     not null,
  metadata_source_value                   STRING    not null,
  metadata_concept_id                     INT64     not null,
  value_as_number                         FLOAT64           ,
  value_as_concept_id                     INT64             ,
  value_as_string                         STRING            ,
  unit_concept_id                         INT64             ,
  unit_source_value                       STRING
);


CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_waveform_feature
(
  waveform_feature_id                     INT64     not null,
  waveform_occurrence_id                  INT64     not null,
  waveform_registry_id                    INT64     not null,
  waveform_channel_metadata_id            INT64     not null,
  measurement_id                          INT64             ,
  observation_id                          INT64             ,
  algorithm_concept_id                    INT64     not null,
  algorithm_source_value                  STRING            ,
  anatomic_site_concept_id                INT64             ,
  waveform_feature_start_timestamp        TIME              ,
  waveform_feature_end_timestamp          TIME              ,
  is_feature_overflow                     BOOLEAN           ,
  value_as_number                         FLOAT64           ,
  value_as_concept_id                     INT64             ,
  value_as_string                         STRING            ,
  value_is_a_registry_file                BOOLEAN           ,
  unit_concept_id                         INT64             ,
  unit_source_value                       STRING
);