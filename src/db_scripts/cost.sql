CREATE TABLE ic AS SELECT * FROM read_csv('./indicative_cashflow/*/*.csv', filename=True, union_by_name=True);
ALTER TABLE ic DROP COLUMN column0;
ALTER TABLE ic ADD COLUMN bm_unit VARCHAR;
UPDATE ic SET bm_unit = SUBSTRING(filename, 27, LENGTH(filename) - 30);
UPDATE ic SET bm_unit = SUBSTRING(filename, 29, LENGTH(filename) - 32) WHERE filename LIKE '%/offer/%';
ALTER TABLE ic ADD COLUMN flow_type VARCHAR;
UPDATE ic SET flow_type =  split(SUBSTRING(filename, 23, LENGTH(filename)), '/')[1];

CREATE TABLE bo AS SELECT * FROM read_csv(
    './bid_offer/*.csv',
    filename=True,
    union_by_name=True
);
ALTER TABLE bo ADD COLUMN bm_unit VARCHAR;
UPDATE bo SET bm_unit = SUBSTRING(filename, 13, LENGTH(filename) - 16);

-- System-level data (buy- and sell-price, imbalance, etc)
CREATE TABLE system AS SELECT * FROM read_csv('./imbalance_settlement.csv', filename=True, union_by_name=True);

-- Total generation
CREATE TABLE gen AS SELECT * FROM read_csv('./generation/total/*.csv', filename=True, union_by_name=True);
ALTER TABLE gen DROP COLUMN column0;
ALTER TABLE gen ADD COLUMN bm_unit VARCHAR;
UPDATE gen SET bm_unit = SUBSTRING(filename, 20, LENGTH(filename) - 23);

-- Average system buy price recalculated from all the cashflows and all the extra generation for each period
CREATE TABLE avg_price AS (
    SELECT gen.settlementDate AS settlementDate, gen.settlementPeriod AS settlementPeriod, SUM(totalCashflow) / SUM(extra) AS price FROM (
        ic JOIN gen ON gen.bm_unit = ic.bm_unit AND gen.settlementDate = ic.settlementDate AND gen.settlementPeriod = ic.settlementPeriod
    ) WHERE gen.extra > 0 AND ic.flow_type = 'offer'
    GROUP BY gen.settlementDate, gen.settlementPeriod
);

-- Generation with only SO-flagged acceptances (note, that only the EXTRA and CURTAILMENT values are usable in this)
CREATE TABLE so AS SELECT * FROM read_csv('./generation/so_only/*.csv', filename=True, union_by_name=True);
ALTER TABLE so DROP COLUMN column0;
ALTER TABLE so ADD COLUMN bm_unit VARCHAR;
UPDATE so SET bm_unit = SUBSTRING(filename, 22, LENGTH(filename) - 25);

CREATE TABLE wind AS SELECT * FROM read_csv('./wind_bm_units.csv');
ALTER TABLE wind ADD COLUMN general_unit VARCHAR;
UPDATE wind SET general_unit = split(bm_unit, '-')[1];

CREATE TABLE units AS SELECT * FROM read_json("../../raw/bm_units.json");
ALTER TABLE units ADD COLUMN bm_unit VARCHAR;
UPDATE units SET bm_unit = elexonBmUnit;
ALTER TABLE units ADD COLUMN general_unit VARCHAR;
UPDATE units SET general_unit = split(bm_unit, '-')[1];
UPDATE units SET fuelType = 'BESS' WHERE (bmUnitName LIKE '%BESS%' OR bmUnitName LIKE '%Battery%') AND (fuelType IS NULL OR fuelType == 'OTHER');

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

CREATE TABLE wind_gen_no_ic AS (
    SELECT * FROM gen INNER JOIN wind_with_metadata ON gen.bm_unit = wind_with_metadata.bm_unit
);

-- Am I looking at the right thing with SO? Should only be wind generation!
CREATE TABLE replacement_cost AS (
    SELECT avg_price.settlementDate AS settlementDate, avg_price.settlementPeriod AS settlementPeriod, missing_after_so.extra_for_so AS extra_for_so, avg_price.price AS price, missing_after_so.extra_for_so * avg_price.price AS total FROM (
        (SELECT settlementDate, settlementPeriod, -1 * sum(curtailment) AS extra_for_so FROM wind_gen_so GROUP BY settlementDate, settlementPeriod) AS missing_after_so
        JOIN avg_price ON avg_price.settlementDate = missing_after_so.settlementDate AND avg_price.settlementPeriod = missing_after_so.settlementPeriod
    )
);

-- CREATE TABLE replacement_cost_system AS (
--     SELECT system.settlementDate AS settlementDate, system.settlementPeriod AS settlementPeriod, missing_after_so.extra_for_so AS extra_for_so, system.systemBuyPrice AS price, missing_after_so.extra_for_so * system.systemBuyPrice AS total FROM (
--         (SELECT settlementDate, settlementPeriod, -1 * sum(curtailment) AS extra_for_so FROM wind_gen_so GROUP BY settlementDate, settlementPeriod) AS missing_after_so
--         JOIN system ON system.settlementDate = missing_after_so.settlementDate AND system.settlementPeriod = missing_after_so.settlementPeriod
--     )
-- );

-- These three are given very distinct unit names, so merging them by hand
UPDATE wind_gen SET general_unit = 'T_CLDW' WHERE general_unit IN ('T_CLDCW', 'T_CLDNW', 'T_CLDSW');


SELECT count(*) AS totalRows FROM wind_gen;

-- Getting the yearly aggregates
COPY (SELECT general_unit, sum(totalCashflow) AS total, sum(curtailment) * -1 AS totalCurtailment, FIRST(site_name) AS site_name, FIRST(lat) AS lat, FIRST(long) AS long FROM wind_gen WHERE YEAR(settlementDate) = 2021 AND totalCashflow > 0 GROUP BY general_unit ORDER BY total DESC) TO './analysis/aggregate_curtailments/2021.csv';
COPY (SELECT general_unit, sum(totalCashflow) AS total, sum(curtailment) * -1 AS totalCurtailment, FIRST(site_name) AS site_name, FIRST(lat) AS lat, FIRST(long) AS long FROM wind_gen WHERE YEAR(settlementDate) = 2022 AND totalCashflow > 0 GROUP BY general_unit ORDER BY total DESC) TO './analysis/aggregate_curtailments/2022.csv';
COPY (SELECT general_unit, sum(totalCashflow) AS total, sum(curtailment) * -1 AS totalCurtailment, FIRST(site_name) AS site_name, FIRST(lat) AS lat, FIRST(long) AS long FROM wind_gen WHERE YEAR(settlementDate) = 2023 AND totalCashflow > 0 GROUP BY general_unit ORDER BY total DESC) TO './analysis/aggregate_curtailments/2023.csv';
COPY (SELECT general_unit, sum(totalCashflow) AS total, sum(curtailment) * -1 AS totalCurtailment, FIRST(site_name) AS site_name, FIRST(lat) AS lat, FIRST(long) AS long FROM wind_gen WHERE YEAR(settlementDate) = 2024 AND totalCashflow > 0 GROUP BY general_unit ORDER BY total DESC) TO './analysis/aggregate_curtailments/2024.csv';
COPY (SELECT general_unit, sum(totalCashflow) AS total, sum(curtailment) * -1 AS totalCurtailment, FIRST(site_name) AS site_name, FIRST(lat) AS lat, FIRST(long) AS long FROM wind_gen WHERE YEAR(settlementDate) = 2025 AND totalCashflow > 0 GROUP BY general_unit ORDER BY total DESC) TO './analysis/aggregate_curtailments/2025.csv';

-- Coverage for SO spending
SELECT YEAR(settlementDate) AS "year", sum(total) AS totalCost FROM replacement_cost GROUP BY YEAR(settlementDate);

-- Total curtailment and direct cost per year:
-- This is in agreement with the REF values!
SELECT YEAR(settlementDate) AS date, sum(curtailment) * -1 / 1000000 AS totalCurtailmentTwh, SUM(totalCashflow) / 1000000 AS totalCostMillion FROM wind_gen WHERE totalCashflow > 0 GROUP BY YEAR(settlementDate);

-- Total replacement cost per year:
select year(settlementDate) AS year, sum(total) /1000000 as totalMillion from replacement_cost GROUP BY year(settlementDate);

-- 2024 top units
SELECT general_unit, sum(totalCashflow) / 1000000 AS totalMillion, sum(curtailment) * -1 / 1000 AS totalCurtailmentGWh, FIRST(site_name) AS site_name, FIRST(lat) AS lat, FIRST(long) AS long FROM wind_gen WHERE YEAR(settlementDate) = 2024 GROUP BY general_unit ORDER BY totalCurtailmentGWh DESC LIMIT 10;

-- Random day: 12/06/2025
SELECT settlementDate, ROUND(SUM(curtailment) * -1) AS totalCurtailment, ROUND(SUM(totalCashflow)) / 1000000 AS costMillion FROM wind_gen WHERE settlementDate = '2025-06-12' GROUP BY settlementDate;
SELECT settlementDate, ROUND(SUM(total)) / 1000000 AS costMillion FROM replacement_cost WHERE settlementDate = '2025-06-12' GROUP BY settlementDate;
SELECT FIRST(settlementDate), settlementPeriod, ROUND(SUM(curtailment) * -1, 2) AS totalCurtailment, ROUND(SUM(totalCashflow)) / 1000 AS costThousand FROM wind_gen WHERE settlementDate = '2025-06-12' AND totalCashflow > 0 GROUP BY settlementPeriod ORDER BY settlementPeriod;

-- Validate generation
SELECT sums.settlementDate, sums.settlementPeriod, system.totalAcceptedOfferVolume - up AS upDiff, system.totalAcceptedBidVolume - down AS downDiff FROM system JOIN (
    SELECT settlementDate, settlementPeriod, sum(extra) AS up, sum(curtailment) AS down FROM gen GROUP BY settlementDate, settlementPeriod
) AS sums ON system.settlementDate = sums.settlementDate AND system.settlementPeriod = sums.settlementPeriod
ORDER BY ABS(downDiff) DESC;

-- Yearly aggregate of total generation, curtailment and ratio:
select sum(curtailment) * -1 / 1000000 AS totalCurtailmentTWh, sum(generated) / 1000000 AS totalGeneratedTWh, ROUND(sum(curtailment) * -100 / sum(generated), 2) AS percentadeDiscarded, YEAR("settlementDate") as "year" from wind_gen_no_ic where "year" > 2020 AND "year" < 2026 group by "year";

-- Total costs:
COPY(
    SELECT curtailment.date, ROUND(totalCurtailmentTwh, 3) AS totalCurtailmentTwh, ROUND(curtailmentCost, 1) AS curtailmentCost, ROUND(replacementCost, 1) AS replacementCost FROM (
        SELECT YEAR(settlementDate) AS date, sum(curtailment) * -1 / 1000000 AS totalCurtailmentTwh, SUM(totalCashflow) AS curtailmentCost FROM wind_gen WHERE totalCashflow > 0 GROUP BY YEAR(settlementDate)
    ) AS curtailment JOIN (
        SELECT YEAR(settlementDate) AS date, SUM(total) AS replacementCost FROM replacement_cost GROUP BY YEAR(settlementDate)
    ) AS replacement ON curtailment.date = replacement.date
    WHERE curtailment.date < 2026
) TO "./analysis/yearly.csv";

SELECT YEAR(settlementDate) AS "year", MONTH(settlementDate) AS "month", SUM(curtailment) * -1 AS totalCurtailmnet, SUM(totalCashflow) AS curtailmentCost FROM wind_gen WHERE totalCashflow > 0 GROUP BY "year", "month";

--  What types are getting turned on WHEN there's curtailment?


COPY (SELECT YEAR(settlementDate) AS "year", MONTH(settlementDate) AS "month", fuel, sum(extra) / 1000 AS totalUpGWh FROM (
    SELECT up.bm_unit AS bm_unit, units.fuelType AS fuel, settlementDate, settlementPeriod, extra FROM (SELECT gen.settlementDate, gen.settlementPeriod, bm_unit, extra FROM gen JOIN (
        SELECT settlementDate, settlementPeriod, SUM(curtailment) < 0 AS curtailed FROM wind_gen  GROUP BY settlementDate, settlementPeriod
    ) AS curtailmentPeriods
    ON curtailmentPeriods.settlementDate = gen.settlementDate AND curtailmentPeriods.settlementPeriod = gen.settlementPeriod
    WHERE gen.extra > 0 AND curtailmentPeriods.curtailed) AS up JOIN units ON up.bm_unit = units.bm_unit
) WHERE YEAR(settlementDate) < 2026 GROUP BY fuel, YEAR(settlementDate), MONTH(settlementDate) ORDER BY YEAR(settlementDate), MONTH(settlementDate), sum(extra) DESC) TO "./analysis/yearly_up_fuel_type.csv";


-- TODO: this could be another one of the bar charts. 
-- Which units make the biggest killing on this?
-- Need to group by general_unit AND fuelType, because WBURB for example has both Battery AND CCGT!
SELECT general_unit, fuelType, sum(extra) / 1000 AS totalUpGWh FROM (
    SELECT units.general_unit as general_unit, up.bm_unit AS bm_unit, units.fuelType AS fuelType, settlementDate, settlementPeriod, extra
    FROM (
        SELECT gen.settlementDate, gen.settlementPeriod, bm_unit, extra FROM gen JOIN (
            SELECT settlementDate, settlementPeriod, SUM(curtailment) < 0 AS curtailed FROM wind_gen  GROUP BY settlementDate, settlementPeriod
        ) AS curtailmentPeriods ON curtailmentPeriods.settlementDate = gen.settlementDate AND curtailmentPeriods.settlementPeriod = gen.settlementPeriod
        WHERE gen.extra > 0 AND curtailmentPeriods.curtailed
    ) AS up JOIN units ON up.bm_unit = units.bm_unit
) GROUP BY general_unit, fuelType ORDER BY sum(extra) DESC;


-- What was the offer price when the UP was accepted?
SELECT settlementDate, settlementPeriod, 
    MIN(offer) FILTER (WHERE accepted) AS avg_offer_accepted,
    MIN(offer) FILTER (WHERE NOT accepted) AS avg_offer_not_accepted
FROM (
    SELECT gen.extra > 0 AS accepted, offer, gen.settlementDate AS settlementDate, gen.settlementPeriod AS settlementPeriod
    FROM (SELECT * FROM bo WHERE pairId = 1 AND offer < 999.0) AS relevant_bo 
    JOIN gen
    ON gen.settlementDate = relevant_bo.settlementDate AND gen.settlementPeriod = relevant_bo.settlementPeriod AND gen.bm_unit = relevant_bo.bm_unit
) GROUP BY settlementDate, settlementPeriod;