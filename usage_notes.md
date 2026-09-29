# Usage notes

This file documents various follow up steps with the demo or full MIMIC data.

## Demo data

Files can be loaded into SQLite database. (see https://rpubs.com/vojtech_huser/OMOPedMIMIC1 for illustrative code; requires some modification)

## inputevents → drug_exposure

`icu.inputevents` (medications and fluids administered in the ICU) is staged as
`src_inputevents` and mapped into `cdm_drug_exposure` with
`load_table_id = 'inputevents'` and
`drug_type_concept_id = 32839` (OMOP4976912 EHR administration record).
`prescriptions` continues to cover *ordered* medications;
`inputevents` covers *administered* ICU medications (vasopressors, sedatives,
IV fluids).

The custom vocabulary `mimiciv_drug_inputevents`
(`custom_mapping_csv/gcpt_drug_inputevents.csv`, concept IDs 2000052001+)
seeds RxNorm mappings for the highest-value ICU itemids.
Item IDs without a mapping still land in `cdm_drug_exposure` with
`drug_concept_id = 0`.

Known limitations (see open issues #16-#21):
- Route is inferred as IV only for `Continuous Med` / `Drug Push` order
  categories; oral meds occasionally listed as Drug Push (issue #18) need review.
- Dose-form ambiguity for non-IV meds is unresolved (issues #19, #20).
- Duplicate item IDs for the same drug (issues #21, #17) share one
  ingredient-level mapping until the item IDs are reconciled.
