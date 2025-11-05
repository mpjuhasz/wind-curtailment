SELECT SUM(curtailment), SUM(physical_level), SUM(extra_generation), fuelType FROM unit_generation JOIN bm_units ON bm_units.elexonBmUnit == unit_generation.bm_unit  WHERE time = '2025-10-02 20:00:00' GROUP BY fuelType;

-- Save the curtailment and extra generation figures per fuel type 
COPY (SELECT SUM(curtailment) AS total_curtailment, SUM(physical_level) AS total_pn, SUM(extra_generation) AS total_extra, fuelType, time FROM unit_generation JOIN bm_units ON bm_units.elexonBmUnit == unit_generation.bm_unit  GROUP BY time, fuelType) TO '../output.csv' (HEADER, DELIMITER ',');

-- Top 20 extra generators 
SELECT SUM(extra_generation), FIRST(fuelType), bm_unit FROM  unit_generation JOIN bm_units ON bm_units.elexonBmUnit == unit_generation.bm_unit GROUP BY bm_unit ORDER BY SUM(extra_generation) ASC LIMIT 20;