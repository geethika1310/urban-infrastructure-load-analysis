CREATE TABLE buildings (
    building_id INT PRIMARY KEY,
    zone VARCHAR(50),
    type VARCHAR(50),
    occupancy INT
);

CREATE TABLE electricity_usage (
    usage_id INT PRIMARY KEY AUTO_INCREMENT,
    building_id INT,
    usage_date DATE,
    kWh DECIMAL(10,2),
    FOREIGN KEY (building_id) REFERENCES buildings(building_id)
);

CREATE TABLE water_usage (
    usage_id INT PRIMARY KEY AUTO_INCREMENT,
    building_id INT,
    usage_date DATE,
    liters DECIMAL(10,2),
    FOREIGN KEY (building_id) REFERENCES buildings(building_id)
);

CREATE TABLE zone_info (
    zone VARCHAR(50) PRIMARY KEY,
    avg_temp DECIMAL(5,2),
    population_density INT
);
