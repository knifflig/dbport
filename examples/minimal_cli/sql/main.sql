-- Main transform for cli_table1
-- This file is the run_hook executed by `dbp run`.

-- Stage: clean and filter raw input
CREATE OR REPLACE VIEW staging AS
SELECT
    geo,
    CAST(substr(time_period, 1, 4) AS INTEGER) AS year,
    obs_value                                  AS value
FROM estat.nama_10r_3empers
WHERE obs_value IS NOT NULL;

-- Final: populate output table
INSERT INTO test.cli_table1
SELECT geo, year, value
FROM staging;
