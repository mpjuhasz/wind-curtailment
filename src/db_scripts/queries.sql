-- Save the curtailment and extra generation figures per fuel type
COPY (SELECT SUM(curtailment) AS total_curtailment, SUM(physical_level) AS total_pn, SUM(extra) AS total_extra, fuelType, time FROM unit_generation JOIN bm_units ON bm_units.elexonBmUnit == unit_generation.bm_unit  GROUP BY time, fuelType) TO '../analysis/october_by_fuel_and_time.csv' (HEADER, DELIMITER ',');

-- Export the 40 extra generators into `processed/analysis/extra_generators.csv`
COPY (SELECT SUM(extra) AS extra, FIRST(fuelType) AS fuel_type, bm_unit FROM  unit_generation JOIN bm_units ON bm_units.elexonBmUnit == unit_generation.bm_unit GROUP BY bm_unit ORDER BY SUM(extra) DESC LIMIT 40) TO '../analysis/extra_generators.csv' (HEADER, DELIMITER ',')
