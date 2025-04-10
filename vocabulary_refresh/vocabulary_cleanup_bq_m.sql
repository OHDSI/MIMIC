-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- Verify vocabularies
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- clean-up temp tables generated by vocabulary_check_bq.sql
-- delete only empty tables
-- run manually
-- -------------------------------------------------------------------


IF NOT EXISTS (SELECT * FROM @bq_target_project.@bq_target_dataset.z_check_voc_1)
THEN
    DROP TABLE @bq_target_project.@bq_target_dataset.z_check_voc_1;
END IF
;

IF NOT EXISTS (SELECT * FROM @bq_target_project.@bq_target_dataset.z_check_voc_2)
THEN
    DROP TABLE @bq_target_project.@bq_target_dataset.z_check_voc_2;
END IF
;

IF NOT EXISTS (SELECT * FROM @bq_target_project.@bq_target_dataset.z_check_voc_3)
THEN
    DROP TABLE @bq_target_project.@bq_target_dataset.z_check_voc_3;
END IF
;

IF NOT EXISTS (SELECT * FROM @bq_target_project.@bq_target_dataset.z_check_voc_5)
THEN
    DROP TABLE @bq_target_project.@bq_target_dataset.z_check_voc_5;
END IF
;

IF NOT EXISTS (SELECT * FROM @bq_target_project.@bq_target_dataset.z_check_voc_6)
THEN
    DROP TABLE @bq_target_project.@bq_target_dataset.z_check_voc_6;
END IF
;

IF NOT EXISTS (SELECT * FROM @bq_target_project.@bq_target_dataset.z_check_voc_7)
THEN
    DROP TABLE @bq_target_project.@bq_target_dataset.z_check_voc_7;
END IF
;

IF NOT EXISTS (SELECT * FROM @bq_target_project.@bq_target_dataset.z_check_voc_8)
THEN
    DROP TABLE @bq_target_project.@bq_target_dataset.z_check_voc_8;
END IF
;

IF NOT EXISTS (SELECT * FROM @bq_target_project.@bq_target_dataset.z_check_voc_9)
THEN
    DROP TABLE @bq_target_project.@bq_target_dataset.z_check_voc_9;
END IF
;

IF NOT EXISTS (SELECT * FROM @bq_target_project.@bq_target_dataset.z_check_voc_10)
THEN
    DROP TABLE @bq_target_project.@bq_target_dataset.z_check_voc_10;
END IF
;

IF NOT EXISTS (SELECT * FROM @bq_target_project.@bq_target_dataset.z_check_voc_11)
THEN
    DROP TABLE @bq_target_project.@bq_target_dataset.z_check_voc_11;
END IF
;

IF NOT EXISTS (SELECT * FROM @bq_target_project.@bq_target_dataset.z_check_voc_12)
THEN
    DROP TABLE @bq_target_project.@bq_target_dataset.z_check_voc_12;
END IF
;

IF NOT EXISTS (SELECT * FROM @bq_target_project.@bq_target_dataset.z_check_voc_13)
THEN
    DROP TABLE @bq_target_project.@bq_target_dataset.z_check_voc_13;
END IF
;

IF NOT EXISTS (SELECT * FROM @bq_target_project.@bq_target_dataset.z_check_voc_14)
THEN
    DROP TABLE @bq_target_project.@bq_target_dataset.z_check_voc_14;
END IF
;

IF NOT EXISTS (SELECT * FROM @bq_target_project.@bq_target_dataset.z_check_voc_15)
THEN
    DROP TABLE @bq_target_project.@bq_target_dataset.z_check_voc_15;
END IF
;

IF NOT EXISTS (SELECT * FROM @bq_target_project.@bq_target_dataset.z_check_voc_16)
THEN
    DROP TABLE @bq_target_project.@bq_target_dataset.z_check_voc_16;
END IF
;

IF NOT EXISTS (SELECT * FROM @bq_target_project.@bq_target_dataset.z_check_voc_17)
THEN
    DROP TABLE @bq_target_project.@bq_target_dataset.z_check_voc_17;
END IF
;
