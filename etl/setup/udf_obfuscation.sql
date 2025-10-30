-- -------------------------------------------------------------------
-- Obfuscation UDF
-- Uses global constants from configuration: @obf_c, @obf_o, @obf64_c, @obf64_o
-- Deterministic permutation over 32-bit or 64-bit unsigned space:
--   For integers: y = (x * C + O) mod 2^32/2^64
--   For strings: y = (FARM_FINGERPRINT(x) * C + O) mod 2^32/2^64
--   32-bit: implemented via masking with 0xFFFFFFFF
--   64-bit: full 64-bit range
-- NOTE: Two functions provided:
--       obf_id(x INT64, bits INT64) - for integer inputs
--       obf_id_str(x STRING, bits INT64) - for string inputs
--       Strings are first converted to hash via FARM_FINGERPRINT, then obfuscated.
-- -------------------------------------------------------------------

-- Integer version of obf_id
CREATE OR REPLACE FUNCTION `@etl_project.@etl_dataset.obf_id`(x INT64, bits INT64)
RETURNS INT64 AS (
  CASE 
    WHEN bits = 32 THEN 
      CAST((CAST(x AS BIGNUMERIC) * CAST(@obf_c AS BIGNUMERIC) + CAST(@obf_o AS BIGNUMERIC)) AS INT64) & 0xFFFFFFFF
    WHEN bits = 64 THEN 
      CAST(MOD(CAST(x AS BIGNUMERIC) * CAST(@obf64_c AS BIGNUMERIC) + CAST(@obf64_o AS BIGNUMERIC), CAST(9223372036854775807 AS BIGNUMERIC)) AS INT64)
    ELSE ERROR('Unsupported bit width')
  END
);

-- String version of obf_id
CREATE OR REPLACE FUNCTION `@etl_project.@etl_dataset.obf_id_str`(x STRING, bits INT64)
RETURNS INT64 AS (
  CASE 
    WHEN bits = 32 THEN 
      CAST((CAST(FARM_FINGERPRINT(x) & 0x7FFFFFFF AS BIGNUMERIC) * CAST(@obf_c AS BIGNUMERIC) + CAST(@obf_o AS BIGNUMERIC)) AS INT64) & 0xFFFFFFFF
    WHEN bits = 64 THEN 
      CAST(MOD(CAST(FARM_FINGERPRINT(x) & 0x7FFFFFFFFFFFFFFF AS BIGNUMERIC) * CAST(@obf64_c AS BIGNUMERIC) + CAST(@obf64_o AS BIGNUMERIC), CAST(9223372036854775807 AS BIGNUMERIC)) AS INT64)
    ELSE ERROR('Unsupported bit width')
  END
);
