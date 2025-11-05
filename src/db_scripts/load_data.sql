CREATE TABLE all_units AS SELECT * FROM read_csv('./*.csv', filename=True);
ALTER TABLE all_units ADD COLUMN bm_unit VARCHAR;
UPDATE all_units SET bm_unit = SUBSTRING(filename, 3, LENGTH(filename) - 6);
ALTER TABLE all_units DROP COLUMN filename;