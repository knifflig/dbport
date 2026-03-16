-- Final: populate the output table from the staging view.
INSERT INTO test.table1
SELECT geo, year, value
FROM staging;
