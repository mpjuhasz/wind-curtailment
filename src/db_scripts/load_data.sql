CREATE TABLE unit_generation AS SELECT * FROM read_csv('./*.csv', filename=True);
ALTER TABLE unit_generation ADD COLUMN bm_unit VARCHAR;
UPDATE unit_generation SET bm_unit = SUBSTRING(filename, 3, LENGTH(filename) - 6);
ALTER TABLE unit_generation DROP COLUMN filename;

CREATE TABLE bm_units AS SELECT * FROM read_json('../../raw/bm_units.json');
