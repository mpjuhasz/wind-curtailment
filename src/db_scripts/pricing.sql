CREATE TABLE bo AS SELECT * FROM read_csv(
    './bid_offer/*.csv',
    filename=True,
    union_by_name=True
);
ALTER TABLE bo ADD COLUMN bm_unit VARCHAR;
UPDATE bo SET bm_unit = SUBSTRING(filename, 13, LENGTH(filename) - 16);


CREATE TABLE gen AS SELECT * FROM read_csv('./generation/*.csv', filename=True, union_by_name=True);
ALTER TABLE gen DROP COLUMN column0;
ALTER TABLE gen ADD COLUMN bm_unit VARCHAR;
UPDATE gen SET bm_unit = SUBSTRING(filename, 14, LENGTH(filename) - 17);


CREATE TABLE all_bm_units AS SELECT * FROM read_json("./../../raw/bm_units.json");

CREATE TABLE merged AS SELECT * FROM bo JOIN gen ON bo.settlementDate = gen.settlementDate AND bo.settlementPeriod = gen.settlementPeriod AND bo.bm_unit = gen.bm_unit;
DROP TABLE bo;
DROP TABLE gen;
