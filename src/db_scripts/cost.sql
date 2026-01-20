CREATE TABLE ic AS SELECT * FROM read_csv('./*/indicative_cashflow/*/*.csv', filename=True, union_by_name=True);
ALTER TABLE ic DROP COLUMN column0;
ALTER TABLE ic ADD COLUMN bm_unit VARCHAR;
UPDATE ic SET bm_unit = SUBSTRING(filename, 32, LENGTH(filename) - 35);
UPDATE ic SET bm_unit = SUBSTRING(filename, 34, LENGTH(filename) - 37) WHERE filename LIKE '%/offer/%';
ALTER TABLE ic ADD COLUMN flow_type VARCHAR;
UPDATE ic SET flow_type =  split(SUBSTRING(filename, 28, LENGTH(filename)), '/')[1];

CREATE TABLE bo AS SELECT * FROM read_csv(
    './*/bid_offer/*.csv',
    filename=True,
    union_by_name=True
);
ALTER TABLE bo ADD COLUMN bm_unit VARCHAR;
UPDATE bo SET bm_unit = SUBSTRING(filename, 18, LENGTH(filename) - 21);

-- System-level data (buy- and sell-price, imbalance, etc)
CREATE TABLE system AS SELECT * FROM read_csv('./*/imbalance_settlement.csv', filename=True, union_by_name=True);

-- Total generation
CREATE TABLE gen AS SELECT * FROM read_csv('./*/generation/total/*.csv', filename=True, union_by_name=True);
ALTER TABLE gen DROP COLUMN column0;
ALTER TABLE gen ADD COLUMN bm_unit VARCHAR;
UPDATE gen SET bm_unit = SUBSTRING(filename, 25, LENGTH(filename) - 28);

-- Average system buy price recalculated from all the cashflows and all the extra generation for each period
CREATE TABLE avg_price AS (
    SELECT gen.settlementDate AS settlementDate, gen.settlementPeriod AS settlementPeriod, SUM(totalCashflow) / SUM(extra) AS price FROM (
        ic JOIN gen ON gen.bm_unit = ic.bm_unit AND gen.settlementDate = ic.settlementDate AND gen.settlementPeriod = ic.settlementPeriod
    ) WHERE gen.extra > 0 AND ic.flow_type = 'offer'
    GROUP BY gen.settlementDate, gen.settlementPeriod
);

-- Generation with only SO-flagged acceptances (note, that only the EXTRA and CURTAILMENT values are usable in this)
CREATE TABLE so AS SELECT * FROM read_csv('./*/generation/so_only/*.csv', filename=True, union_by_name=True);
ALTER TABLE so DROP COLUMN column0;
ALTER TABLE so ADD COLUMN bm_unit VARCHAR;
UPDATE so SET bm_unit = SUBSTRING(filename, 27, LENGTH(filename) - 30);

CREATE TABLE wind AS SELECT * FROM read_csv('./wind_bm_units.csv');
ALTER TABLE wind ADD COLUMN general_unit VARCHAR;
UPDATE wind SET general_unit = split(bm_unit, '-')[1];

CREATE TABLE unit_metadata AS SELECT * FROM read_csv('../bm_unit_with_repd.csv');
ALTER TABLE unit_metadata ADD COLUMN general_unit VARCHAR;
UPDATE unit_metadata SET general_unit = split(bm_unit, '-')[1];

-- Metadata for only the wind units (used to filter)
CREATE TABLE wind_with_metadata AS (
    SELECT * FROM wind INNER JOIN (
        SELECT general_unit, FIRST(repd_site_name) AS site_name, FIRST(repd_lat) AS lat, FIRST(repd_long) AS long, FIRST(capacity) AS capacity, FIRST(region) AS region
        FROM unit_metadata GROUP BY general_unit
    ) AS tmp ON wind.general_unit = tmp.general_unit
);

-- Generation from WIND sites only, with their corresponding bid cashflows for the curtailment costs
CREATE TABLE wind_gen AS (
    SELECT * FROM gen JOIN (SELECT * FROM ic INNER JOIN wind_with_metadata ON ic.bm_unit = wind_with_metadata.bm_unit WHERE ic.flow_type = 'bid') AS other
    ON gen.bm_unit = other.bm_unit AND gen.settlementDate = other.settlementDate AND gen.settlementPeriod = other.settlementPeriod
);

CREATE TABLE wind_gen_so AS (
    SELECT * FROM so JOIN (SELECT * FROM ic INNER JOIN wind_with_metadata ON ic.bm_unit = wind_with_metadata.bm_unit WHERE ic.flow_type = 'bid') AS other
    ON so.bm_unit = other.bm_unit AND so.settlementDate = other.settlementDate AND so.settlementPeriod = other.settlementPeriod
);

-- Am I looking at the right thing with SO? Should only be wind generation!
CREATE TABLE replacement_cost AS (
    SELECT avg_price.settlementDate AS settlementDate, avg_price.settlementPeriod AS settlementPeriod, missing_after_so.extra_for_so AS extra_for_so, avg_price.price AS price, missing_after_so.extra_for_so * avg_price.price AS total FROM (
        (SELECT settlementDate, settlementPeriod, -1 * sum(curtailment) AS extra_for_so FROM wind_gen_so GROUP BY settlementDate, settlementPeriod) AS missing_after_so
        JOIN avg_price ON avg_price.settlementDate = missing_after_so.settlementDate AND avg_price.settlementPeriod = missing_after_so.settlementPeriod
    )
);


CREATE TABLE replacement_cost_system AS (
    SELECT system.settlementDate AS settlementDate, system.settlementPeriod AS settlementPeriod, missing_after_so.extra_for_so AS extra_for_so, system.systemBuyPrice AS price, missing_after_so.extra_for_so * system.systemBuyPrice AS total FROM (
        (SELECT settlementDate, settlementPeriod, -1 * sum(curtailment) AS extra_for_so FROM wind_gen_so GROUP BY settlementDate, settlementPeriod) AS missing_after_so
        JOIN system ON system.settlementDate = missing_after_so.settlementDate AND system.settlementPeriod = missing_after_so.settlementPeriod
    )
);

-- These three are given very distinct unit names, so merging them by hand
UPDATE wind_gen SET general_unit = 'T_CLDW' WHERE general_unit IN ('T_CLDCW', 'T_CLDNW', 'T_CLDSW');

-- Getting the yearly aggregates
COPY (SELECT general_unit, sum(totalCashflow) AS total, sum(curtailment) * -1 AS totalCurtailment, FIRST(site_name) AS site_name, FIRST(lat) AS lat, FIRST(long) AS long FROM wind_gen WHERE YEAR(settlementDate) = 2021 GROUP BY general_unit ORDER BY total DESC) TO './analysis/aggregate_curtailments/2021.csv';
COPY (SELECT general_unit, sum(totalCashflow) AS total, sum(curtailment) * -1 AS totalCurtailment, FIRST(site_name) AS site_name, FIRST(lat) AS lat, FIRST(long) AS long FROM wind_gen WHERE YEAR(settlementDate) = 2022 GROUP BY general_unit ORDER BY total DESC) TO './analysis/aggregate_curtailments/2022.csv';
COPY (SELECT general_unit, sum(totalCashflow) AS total, sum(curtailment) * -1 AS totalCurtailment, FIRST(site_name) AS site_name, FIRST(lat) AS lat, FIRST(long) AS long FROM wind_gen WHERE YEAR(settlementDate) = 2023 GROUP BY general_unit ORDER BY total DESC) TO './analysis/aggregate_curtailments/2023.csv';
COPY (SELECT general_unit, sum(totalCashflow) AS total, sum(curtailment) * -1 AS totalCurtailment, FIRST(site_name) AS site_name, FIRST(lat) AS lat, FIRST(long) AS long FROM wind_gen WHERE YEAR(settlementDate) = 2024 GROUP BY general_unit ORDER BY total DESC) TO './analysis/aggregate_curtailments/2024.csv';
COPY (SELECT general_unit, sum(totalCashflow) AS total, sum(curtailment) * -1 AS totalCurtailment, FIRST(site_name) AS site_name, FIRST(lat) AS lat, FIRST(long) AS long FROM wind_gen WHERE YEAR(settlementDate) = 2025 GROUP BY general_unit ORDER BY total DESC) TO './analysis/aggregate_curtailments/2025.csv';

-- Coverage for SO spending
SELECT YEAR(settlementDate) AS "year", sum(total) AS totalCost FROM replacement_cost GROUP BY YEAR(settlementDate);

-- 