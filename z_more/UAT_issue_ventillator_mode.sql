UAT_issue_ventillator_mode

-- ------------------------------------------------------------------------------------
-- from Mik:
-- ------------------------------------------------------------------------------------

we came across something odd when doing the UAT in MIMIC. When looking for the mode of the ventilator, we found that the source value list in the OMOP CDM is much smaller than the one in MIMIC:

SELECT DISTINCT(value_source_value) FROM `odysseus-mimic-dev.mimiciv_full_current_cdm_20.measurement` 
WHERE measurement_concept_id = 3004921 --ventilator mode

value_source_value
PSV/SBT
SPONT
SPONTANEOUS.
CPAP/PSV+ApnVol
PRES/AC
P-CMV
PRVC/AC
SIMV/PRES
SIMV/VOL
CONTROLLED.
CPAP/PSV+ApnPres
VOL/AC
APV (cmv)
CPAP/PSV+Apn TCPL
APRV/Biphasic+ApnVol
PRVC/SIMV
APRV/Biphasic+ApnPress

IMV.
Ambient
(S) CMV


select distinct(value)
from `physionet-data.mimiciv_icu.chartevents`
WHERE itemid = 223849
-- Anna: I cannot access the table, access denied

value
CPAP
PRVC/AC
CMV/ASSIST
PRVC/SIMV
(S) CMV
CPAP/PSV
MMV
SPONT
APRV/Biphasic+ApnPress
SIMV
SIMV/PRES
APRV
CMV/AutoFlow
VOL/AC
CMV/ASSIST/AutoFlow
PCV+
PSV/SBT
CMV
MMV/PSV
PCV+/PSV
MMV/AutoFlow
SIMV/VOL
APV (cmv)
P-CMV
MMV/PSV/AutoFlow
PRES/AC
SIMV/PSV
CPAP/PSV+Apn TCPL
SYNCHRON MASTER
SYNCHRON SLAVE
Apnea Ventilation
CPAP/PSV+ApnPres
Ambient
PCV+Assist
SIMV/PSV/AutoFlow
Standby
SIMV/AutoFlow
APRV/Biphasic+ApnVol
CPAP/PPS
CPAP/PSV+ApnVol

-- ------------------------------------------------------------------------------------
-- looking into the issue
-- ------------------------------------------------------------------------------------


SELECT DISTINCT unit_id, count(distinct value_source_value) 
FROM `odysseus-mimic-dev.mimiciv_full_cdm_2022_09_09.cdm_measurement` 
WHERE measurement_concept_id = 3004921 --ventilator mode
group by 1
order by 1
-- unit_id  f0_
-- measurement.meas.chartevents 17
-- measurement.meas.labevents   3

-- from chartevents: lk_chartevents_mapped.value_source_value
SELECT COUNT(DISTINCT value_source_value)
FROM mimiciv_full_cdm_2022_09_09.lk_chartevents_mapped
WHERE target_concept_id = 3004921 --ventilator mode
-- 17

SELECT DISTINCT 
    IF(src.valuenum IS NULL, src.value, NULL)   AS value_source_value,
    src.valuenum,
    src.value
FROM mimiciv_full_cdm_2022_09_09.lk_chartevents_clean src
LEFT JOIN
    mimiciv_full_cdm_2022_09_09.lk_chartevents_concept c_main -- main
        ON c_main.source_code = src.source_code 
        AND c_main.source_vocabulary_id = 'mimiciv_meas_chart'
WHERE c_main.target_concept_id = 3004921 --ventilator mode
LIMIT 6

value_source_value  valuenum    value
PSV/SBT                         PSV/SBT
SIMV/VOL                        SIMV/VOL
                    49.0        CMV/ASSIST/AutoFlow
                    53.0        CPAP/PPS
                    46.0        SIMV/AutoFlow
                    10.0        CPAP

-- this logic eliminates the ventilator mode values
-- please see etl/etl/lk_meas_chartevents.sql, ln 172
