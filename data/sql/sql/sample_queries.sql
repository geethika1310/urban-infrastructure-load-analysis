-- Total electricity usage by zone
SELECT b.zone, SUM(e.kWh) AS total_kWh
FROM electricity_usage e
JOIN buildings b ON e.building_id = b.building_id
GROUP BY b.zone;

-- Average water usage per occupant
SELECT b.zone, ROUND(SUM(w.liters) / SUM(b.occupancy), 2) AS liters_per_person
FROM water_usage w
JOIN buildings b ON w.building_id = b.building_id
GROUP BY b.zone;
