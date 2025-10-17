-- -------------------------------------------------------------------
-- Obfuscation UDF
-- Uses global constants from configuration: @obf_c, @obf_o, @obf64_c, @obf64_o
-- Deterministic permutation over 32-bit or 64-bit unsigned space:
--   32-bit: y = (x * C + O) mod 2^32, implemented via masking with 0xFFFFFFFF
--   64-bit: y = (x * C + O) mod 2^64
-- NOTE: Input assumed non-negative INT64. User must specify bit width (32 or 64).
-- -------------------------------------------------------------------

CREATE OR REPLACE FUNCTION `@etl_project.@etl_dataset.obf_id`(x INT64, bits INT64)
RETURNS INT64 AS (
  CASE 
    WHEN bits = 32 THEN ((x * CAST(@obf_c AS INT64) + CAST(@obf_o AS INT64)) & 0xFFFFFFFF)
    WHEN bits = 64 THEN (x * CAST(@obf64_c AS INT64) + CAST(@obf64_o AS INT64))
    ELSE ERROR('Unsupported bit width')
  END
);
