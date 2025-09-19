-- -------------------------------------------------------------------
-- Obfuscation UDF
-- Uses global constants from configuration: @obf_c, @obf_o
-- Deterministic permutation over 32-bit unsigned space:
--   y = (x * C + O) mod 2^32
-- Implemented via masking with 0xFFFFFFFF.
-- NOTE: Input assumed non-negative INT64 fitting in 32 bits.
-- -------------------------------------------------------------------

CREATE OR REPLACE FUNCTION `@etl_project.@etl_dataset`.obf_id(x INT64)
RETURNS INT64 AS (
  ((x * CAST(@obf_c AS INT64) + CAST(@obf_o AS INT64)) & 0xFFFFFFFF)
);
