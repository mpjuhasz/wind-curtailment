CREATE TABLE calculated_cashflow AS SELECT * FROM read_csv('./calculated_cashflow/*.csv', filename=True);
ALTER TABLE calculated_cashflow ADD COLUMN bm_unit VARCHAR;
UPDATE calculated_cashflow SET bm_unit = SUBSTRING(filename, 23, LENGTH(filename) - 26);


CREATE TABLE indicative_cashflow AS SELECT * FROM read_csv('./indicative_cashflow/*.csv', filename=True, union_by_name=True);
ALTER TABLE indicative_cashflow DROP COLUMN column0;
ALTER TABLE indicative_cashflow ADD COLUMN bm_unit VARCHAR;
UPDATE calculated_cashflow SET bm_unit = SUBSTRING(filename, 23, LENGTH(filename) - 26);


CREATE TABLE all_bm_units AS SELECT * FROM read_json("./../../raw/bm_units.json");

CREATE VIEW ic AS SELECT sum(totalCashflow) AS sumIC, bm_unit FROM indicative_cashflow GROUP BY bm_unit  ORDER BY sum(totalCashflow) DESC;
CREATE VIEW cc AS SELECT sum(calculated_cashflow_extra + calculated_cashflow_curtailment) AS sumCC, bm_unit FROM calculated_cashflow GROUP BY bm_unit  ORDER BY sum(calculated_cashflow_curtailment + calculated_cashflow_extra) DESC;
