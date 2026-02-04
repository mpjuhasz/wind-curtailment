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

CREATE TABLE raw_units AS SELECT * FROM read_json("../../raw/bm_units.json");
ALTER TABLE raw_units ADD COLUMN bm_unit VARCHAR;
UPDATE raw_units SET bm_unit = elexonBmUnit;
ALTER TABLE raw_units ADD COLUMN general_unit VARCHAR;
UPDATE raw_units SET general_unit = split(bm_unit, '-')[1];

CREATE TABLE unit_fuel_types AS SELECT * FROM read_xlsx("../../raw/BMUFuelType.xlsx", header=true, empty_as_varchar=true);
CREATE TABLE units AS SELECT * FROM raw_units JOIN unit_fuel_types ON raw_units.bm_unit = unit_fuel_types."SETT UNIT ID";
DROP TABLE raw_units;
DROP TABLE unit_fuel_types;
UPDATE units SET "BMRS FUEL TYPE" = "REG FUEL TYPE" WHERE "BMRS FUEL TYPE" = 'OTHER';
UPDATE units SET fuelType = "BMRS FUEL TYPE" WHERE fuelType IS NULL OR fuelType = 'OTHER';
UPDATE units SET fuelType = 'CCGT' WHERE fuelType = 'GAS';
UPDATE units SET fuelType = 'BESS' WHERE fuelType = 'BATTERY';

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

CREATE TABLE extra_gen AS (
    SELECT * FROM gen JOIN (SELECT * FROM ic JOIN units ON ic.bm_unit = units.bm_unit WHERE ic.flow_type = 'offer') AS other
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


-- Comparing the replacement cost with the system price
COPY (
SELECT system.settlementDate, system.settlementPeriod, price as replacementPrice, systemBuyPrice
FROM system JOIN replacement_cost ON system.settlementDate = replacement_cost.settlementDate AND system.settlementPeriod = replacement_cost.settlementPeriod
) TO './analysis/system_vs_replacement.csv';


-- These three are given very distinct unit names, so merging them by hand
UPDATE wind_gen SET general_unit = 'T_CLDW' WHERE general_unit IN ('T_CLDCW', 'T_CLDNW', 'T_CLDSW');


SELECT count(*) AS totalRows FROM wind_gen;

-- Getting the yearly aggregates
COPY (SELECT general_unit, sum(totalCashflow) AS totalCost, sum(curtailment) * -1 / 1000 AS totalCurtailmentGWh, FIRST(site_name) AS site_name, FIRST(lat) AS lat, FIRST(long) AS long FROM wind_gen WHERE YEAR(settlementDate) = 2021 AND totalCashflow > 0 GROUP BY general_unit ORDER BY totalCost DESC) TO './analysis/aggregate_curtailments/2021.csv';
COPY (SELECT general_unit, sum(totalCashflow) AS totalCost, sum(curtailment) * -1 / 1000 AS totalCurtailmentGWh, FIRST(site_name) AS site_name, FIRST(lat) AS lat, FIRST(long) AS long FROM wind_gen WHERE YEAR(settlementDate) = 2022 AND totalCashflow > 0 GROUP BY general_unit ORDER BY totalCost DESC) TO './analysis/aggregate_curtailments/2022.csv';
COPY (SELECT general_unit, sum(totalCashflow) AS totalCost, sum(curtailment) * -1 / 1000 AS totalCurtailmentGWh, FIRST(site_name) AS site_name, FIRST(lat) AS lat, FIRST(long) AS long FROM wind_gen WHERE YEAR(settlementDate) = 2023 AND totalCashflow > 0 GROUP BY general_unit ORDER BY totalCost DESC) TO './analysis/aggregate_curtailments/2023.csv';
COPY (SELECT general_unit, sum(totalCashflow) AS totalCost, sum(curtailment) * -1 / 1000 AS totalCurtailmentGWh, FIRST(site_name) AS site_name, FIRST(lat) AS lat, FIRST(long) AS long FROM wind_gen WHERE YEAR(settlementDate) = 2024 AND totalCashflow > 0 GROUP BY general_unit ORDER BY totalCost DESC) TO './analysis/aggregate_curtailments/2024.csv';
COPY (SELECT general_unit, sum(totalCashflow) AS totalCost, sum(curtailment) * -1 / 1000 AS totalCurtailmentGWh, FIRST(site_name) AS site_name, FIRST(lat) AS lat, FIRST(long) AS long FROM wind_gen WHERE YEAR(settlementDate) = 2025 AND totalCashflow > 0 GROUP BY general_unit ORDER BY totalCost DESC) TO './analysis/aggregate_curtailments/2025.csv';

-- Coverage for SO spending
SELECT YEAR(settlementDate) AS "year", sum(total) AS totalCost FROM replacement_cost GROUP BY YEAR(settlementDate);

-- Total curtailment and direct cost per year:
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


-- All first pair offer prices saved
COPY (
    SELECT gen.bm_unit AS bm_unit, relevant_bo.levelTo AS levelTo, gen.extra > 0 AS accepted, offer, gen.settlementDate AS settlementDate, gen.settlementPeriod AS settlementPeriod
    FROM (SELECT * FROM bo WHERE pairId = 1 AND offer < 999.0) AS relevant_bo
    JOIN gen
    ON gen.settlementDate = relevant_bo.settlementDate AND gen.settlementPeriod = relevant_bo.settlementPeriod AND gen.bm_unit = relevant_bo.bm_unit
) TO "./analysis/all_offers.csv";


-- Top up winners:
CREATE TABLE generator_metadata AS SELECT * FROM read_csv('../extra_generators_metadata.csv');

UPDATE extra_gen SET general_unit = 'T_DIDCB' WHERE general_unit LIKE 'T_DIDCB%';

COPY (SELECT general_unit, FIRST(site_name) AS site_name, fuelType, sum(totalCashflow) AS totalCost, sum(extra) / 1000 AS totalExtraGWh, FIRST(bmUnitName) AS site_name, FIRST(long) AS long, FIRST(lat) AS lat FROM extra_gen JOIN generator_metadata ON extra_gen.bm_unit = generator_metadata.bm_unit WHERE YEAR(settlementDate) = 2021 AND totalCashflow > 0 GROUP BY general_unit, fuelType ORDER BY totalCost DESC LIMIT 20) TO './analysis/aggregate_extras/2021.csv';
COPY (SELECT general_unit, FIRST(site_name) AS site_name, fuelType, sum(totalCashflow) AS totalCost, sum(extra) / 1000 AS totalExtraGWh, FIRST(bmUnitName) AS site_name, FIRST(long) AS long, FIRST(lat) AS lat FROM extra_gen JOIN generator_metadata ON extra_gen.bm_unit = generator_metadata.bm_unit WHERE YEAR(settlementDate) = 2022 AND totalCashflow > 0 GROUP BY general_unit, fuelType ORDER BY totalCost DESC LIMIT 20) TO './analysis/aggregate_extras/2022.csv';
COPY (SELECT general_unit, FIRST(site_name) AS site_name, fuelType, sum(totalCashflow) AS totalCost, sum(extra) / 1000 AS totalExtraGWh, FIRST(bmUnitName) AS site_name, FIRST(long) AS long, FIRST(lat) AS lat FROM extra_gen JOIN generator_metadata ON extra_gen.bm_unit = generator_metadata.bm_unit WHERE YEAR(settlementDate) = 2023 AND totalCashflow > 0 GROUP BY general_unit, fuelType ORDER BY totalCost DESC LIMIT 20) TO './analysis/aggregate_extras/2023.csv';
COPY (SELECT general_unit, FIRST(site_name) AS site_name, fuelType, sum(totalCashflow) AS totalCost, sum(extra) / 1000 AS totalExtraGWh, FIRST(bmUnitName) AS site_name, FIRST(long) AS long, FIRST(lat) AS lat FROM extra_gen JOIN generator_metadata ON extra_gen.bm_unit = generator_metadata.bm_unit WHERE YEAR(settlementDate) = 2024 AND totalCashflow > 0 GROUP BY general_unit, fuelType ORDER BY totalCost DESC LIMIT 20) TO './analysis/aggregate_extras/2024.csv';
COPY (SELECT general_unit, FIRST(site_name) AS site_name, fuelType, sum(totalCashflow) AS totalCost, sum(extra) / 1000 AS totalExtraGWh, FIRST(bmUnitName) AS site_name, FIRST(long) AS long, FIRST(lat) AS lat FROM extra_gen JOIN generator_metadata ON extra_gen.bm_unit = generator_metadata.bm_unit WHERE YEAR(settlementDate) = 2025 AND totalCashflow > 0 GROUP BY general_unit, fuelType ORDER BY totalCost DESC LIMIT 20) TO './analysis/aggregate_extras/2025.csv';

-- Top losers, i.e. who are those that placed bids cheaper than the existing ones, but didn't get them accepted?
CREATE TABLE first_offers AS (
    SELECT gen.bm_unit AS bm_unit, relevant_bo.levelTo AS levelTo, gen.extra > 0 AS accepted, offer, gen.settlementDate AS settlementDate, gen.settlementPeriod AS settlementPeriod
    FROM (SELECT * FROM bo WHERE pairId = 1 AND offer < 999.0) AS relevant_bo
    JOIN gen
    ON gen.settlementDate = relevant_bo.settlementDate AND gen.settlementPeriod = relevant_bo.settlementPeriod AND gen.bm_unit = relevant_bo.bm_unit
)


COPY (SELECT settlementDate, settlementPeriod, offs.bm_unit, levelTo, offer, site_name, lat, long FROM (
    SELECT settlementDate, settlementPeriod, bm_unit, levelTo, offer FROM first_offers WHERE bm_unit IN ('2__LRWED001', 'E_ROARB-1', 'E_BARNB-1') AND NOT accepted AND YEAR(settlementDate) = '2025'
) AS offs JOIN generator_metadata ON generator_metadata.bm_unit = offs.bm_unit) TO './analysis/skipped_batteries_2025.csv';



CREATE TABLE acceptances_2025 AS (
    SELECT units.bm_unit AS bm_unit, levelTo, offer, settlementDate, settlementPeriod, b.site_name, bmUnitName FROM (
        SELECT a.bm_unit AS bm_unit, levelTo, offer, settlementDate, settlementPeriod, site_name FROM (
            SELECT bm_unit, levelTo, offer, settlementDate, settlementPeriod FROM first_offers WHERE YEAR(settlementDate) = '2025' AND accepted
        ) AS a LEFT JOIN generator_metadata ON a.bm_unit = generator_metadata.bm_unit
    ) AS b JOIN units ON units.bm_unit = b.bm_unit
);
UPDATE acceptances_2025 SET site_name = bmUnitName WHERE site_name IS NULL;

COPY (SELECT * FROM acceptances_2025) TO './analysis/first_acceptances_2025.csv';

-- All bids:
COPY (
    SELECT gen.bm_unit AS bm_unit, relevant_bo.levelTo AS levelTo, gen.curtailment < 0 AS accepted, bid, gen.settlementDate AS settlementDate, gen.settlementPeriod AS settlementPeriod
    FROM (SELECT * FROM bo WHERE pairId = -1 AND bid > -999.0) AS relevant_bo
    JOIN gen
    ON gen.settlementDate = relevant_bo.settlementDate AND gen.settlementPeriod = relevant_bo.settlementPeriod AND gen.bm_unit = relevant_bo.bm_unit
) TO "./analysis/all_bids.csv";

-- Beatrice bids:
COPY (
    SELECT bm_unit, levelTo, accepted, bid, beatrice.settlementDate AS settlementDate, beatrice.settlementPeriod AS settlementPeriod, systemSellPrice AS systemPrice, systemPrice - bid AS diff FROM (
        SELECT gen.bm_unit AS bm_unit, relevant_bo.levelTo AS levelTo, gen.curtailment < 0 AS accepted, bid, gen.settlementDate AS settlementDate, gen.settlementPeriod AS settlementPeriod
        FROM (SELECT * FROM bo WHERE pairId = -1 AND bid > -999.0) AS relevant_bo
        JOIN gen
        ON gen.settlementDate = relevant_bo.settlementDate AND gen.settlementPeriod = relevant_bo.settlementPeriod AND gen.bm_unit = relevant_bo.bm_unit
        WHERE gen.bm_unit LIKE 'T_BEATO%'
    ) AS beatrice JOIN system ON beatrice.settlementDate = system.settlementDate AND beatrice.settlementPeriod = system.settlementPeriod
) TO "./analysis/beatrice_bids.csv";

-- Odd bids
CREATE TABLE first_bids AS (
    SELECT gen.bm_unit AS bm_unit, relevant_bo.levelTo AS levelTo, gen.curtailment < 0 AS accepted, bid, gen.settlementDate AS settlementDate, gen.settlementPeriod AS settlementPeriod
    FROM (SELECT * FROM bo WHERE pairId = -1 AND bid > -999.0) AS relevant_bo
    JOIN gen
    ON gen.settlementDate = relevant_bo.settlementDate AND gen.settlementPeriod = relevant_bo.settlementPeriod AND gen.bm_unit = relevant_bo.bm_unit
);


-- I'm not certain that this reflects the issues I'm looking for. Also, it'll probably need location data
-- to make it comparable and useful
SELECT bm_unit, COUNT(*) FILTER(WHERE otherBid < bid AND otherBid > bid - 0.5) AS undercutBids
FROM (
    SELECT f1.settlementDate, f1.settlementPeriod, f1.bm_unit, f1.bid AS bid, f2.bid AS otherBid
    FROM (
        SELECT *
        FROM first_bids
        -- I want only windfarm for this, so negative prices
        WHERE bid < 0
        AND YEAR(settlementDate) = '2022'
        AND accepted
        -- AND MONTH(settlementDate) = '10'
    ) AS f1 CROSS JOIN first_bids AS f2
    WHERE f1.settlementDate = f2.settlementDate AND f1.settlementPeriod = f2.settlementPeriod
) GROUP BY bm_unit
ORDER BY undercutBids DESC;


-- Looking at long spans of consistent pricing, like we saw with Beatrice:
SELECT
    bm_unit,
    COUNT(interval_group) AS pricingLength,
    MIN(settlementDate) AS "start",
    MAX(settlementDate) AS "end",
    AVG(bid) AS avgBidPrice,
    COUNT(accepted) AS acceptedCount
FROM (
    SELECT
        bm_unit,
        settlementDate,
        settlementPeriod,
        bid,
        accepted,
        SUM(CASE WHEN fixedPrice THEN 0 ELSE 1 END) OVER (PARTITION BY bm_unit ORDER BY settlementDate, settlementPeriod) AS interval_group
    FROM (
        SELECT
            bm_unit,
            settlementDate,
            settlementPeriod,
            bid,
            LAG(bid, 1) OVER (ORDER BY bm_unit, settlementDate, settlementPeriod) AS laggedBid,
            ABS(laggedBid - bid) < 0.5 AS fixedPrice,
            accepted,
        FROM first_bids WHERE bid < 0 ORDER BY bm_unit, settlementDate, settlementPeriod
    ) ORDER BY bm_unit, settlementDate, settlementPeriod
) GROUP BY bm_unit, interval_group
ORDER BY acceptedCount DESC;


-- Dispatch times:
CREATE TABLE acc AS SELECT * FROM read_csv('./acceptance/*.csv', filename=True, union_by_name=True);
ALTER TABLE acc ADD COLUMN bm_unit VARCHAR;
UPDATE acc SET bm_unit = SUBSTRING(filename, 14, LENGTH(filename) - 17);


-- Dispatch times
COPY (
    SELECT YEAR(settlementDate) AS "year", MONTH(settlementDate) AS "month", fuelType, dispatchTime, count(dispatch_times.bm_unit) AS "count" FROM
    (SELECT
        bm_unit,
        settlementDate,
        MAX(timeTo) - MIN(timeFrom) AS dispatchTime,
    FROM (
        SELECT
            bm_unit,
            settlementDate,
            timeTo,
            timeFrom,
            -- Create a group identifier: increment when there's a gap
            SUM(CASE WHEN overlap THEN 0 ELSE 1 END) OVER (PARTITION BY bm_unit, settlementDate ORDER BY timeFrom) AS interval_group
        FROM (
            SELECT
                bm_unit,
                LAG(bm_unit, 1) OVER (ORDER BY bm_unit, settlementDate, timeFrom) AS lagged_bm_unit,
                settlementDate,
                timeFrom,
                timeTo,
                LAG(timeTo, 1) OVER (ORDER BY bm_unit, settlementDate, timeFrom) AS laggedTimeTo,
                laggedTimeTo = timeFrom AND bm_unit = lagged_bm_unit AS overlap
            FROM acc WHERE levelFrom != 0 OR levelTo != 0 ORDER BY bm_unit, settlementDate, timeFrom
        )
    ) GROUP BY bm_unit, settlementDate, interval_group) AS dispatch_times JOIN units ON dispatch_times.bm_unit = units.bm_unit
    WHERE fuelType IN ('BESS', 'CCGT') AND YEAR(settlementDate) > 2020 AND YEAR(settlementDate) < 2026 GROUP BY YEAR(settlementDate), MONTH(settlementDate), fuelType, dispatchTime
    ORDER BY "year", "month", fuelType, dispatchTime
) TO './analysis/dispatch_times_aggregate.csv';
