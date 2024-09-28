-- Average observed temperature for the last 7 days on a single station ID
SELECT AVG(temperature) AS average_temperature
FROM observations
WHERE station_id = '032HE'; /* Here to change station ID */


-- Maximum wind speed change between two consecutive observations on a single station ID
WITH wind_speed_changes AS (
    SELECT timestamp, wind_speed,
           LAG(wind_speed) OVER (ORDER BY timestamp) AS previous_wind_speed
    FROM observations
    WHERE station_id = '032HE' /* Here to change station ID */
)
SELECT MAX(ABS(wind_speed - previous_wind_speed)) AS max_wind_speed_change
FROM wind_speed_changes
WHERE previous_wind_speed IS NOT NULL;

-- More useful querys -- 

-- View all records from a table
SELECT * FROM stations;
SELECT * FROM observations;

-- To calculate the average observed temperature for the entire table (all stations) over the last 7 days
SELECT AVG(temperature) AS average_temperature
FROM observations;

-- To calculate the maximum wind speed change between two consecutive observations for the entire table (all stations) over the last 7 days
WITH wind_speed_changes AS (
    SELECT timestamp, wind_speed,
           LAG(wind_speed) OVER (ORDER BY timestamp) AS previous_wind_speed
    FROM observations
)
SELECT MAX(ABS(wind_speed - previous_wind_speed)) AS max_wind_speed_change
FROM wind_speed_changes
WHERE previous_wind_speed IS NOT NULL;



