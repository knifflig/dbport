-- Staging: clean and filter the raw input before final insert.
CREATE OR REPLACE VIEW staging AS
SELECT
    geo,
    CAST(substr(time_period, 1, 4) AS INTEGER) AS year,
    obs_value                                  AS value
FROM estat.nama_10r_3empers
WHERE obs_value IS NOT NULL;
