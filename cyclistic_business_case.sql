/* This is the SQL script used to import, compile and analyse the Cyclistic Case 
Study for the Capstone Project of the Google Data Analytics Professional Certificate.
It uses the data from Divvy Bikes (https://www.divvybikes.com/system-data).
BigQuery was used to for the following purposes:
1. Download the individual CSV documents
2. Import each into separate tables, regularising data types
3. Combine all the data into one table
4. Inspect data for anomalies
5. Identify and exclude data with anomalies
6. Create queries for data visualisations
The code below starts at step 3.
*/

----Step 3: Combine all the data into one table----

CREATE TABLE `rapid-burner-358919.Cyclistic.Total_rides` AS
(SELECT *
FROM 
  `rapid-burner-358919.Cyclistic.2021_01`
UNION ALL

SELECT *
FROM
  `rapid-burner-358919.Cyclistic.2021_02`
UNION ALL

SELECT *
FROM
  `rapid-burner-358919.Cyclistic.2021_03`
UNION ALL

SELECT *
FROM
  `rapid-burner-358919.Cyclistic.2021_04`
UNION ALL

SELECT *
FROM
  `rapid-burner-358919.Cyclistic.2021_05`
UNION ALL

SELECT *
FROM
  `rapid-burner-358919.Cyclistic.2021_06`
UNION ALL

SELECT *
FROM
  `rapid-burner-358919.Cyclistic.2021_07`
UNION ALL
SELECT *
FROM
  `rapid-burner-358919.Cyclistic.2021_08`
UNION ALL

SELECT *
FROM
  `rapid-burner-358919.Cyclistic.2021_09`
UNION ALL

SELECT *
FROM
  `rapid-burner-358919.Cyclistic.2021_10`
UNION ALL

SELECT *
FROM
  `rapid-burner-358919.Cyclistic.2021_11`
UNION ALL

SELECT *
FROM
  `rapid-burner-358919.Cyclistic.2021_12`
)
ORDER BY
  started_at;

----add ride_length column----

SELECT *,
  DATETIME_DIFF(ended_at, started_at, MINUTE) AS ride_length,
FROM
  `rapid-burner-358919.Cyclistic.Total_rides`
ORDER BY
  started_at;

----Step 4: Inspect data for anomalies----

SELECT DISTINCT member_casual
FROM 
  `rapid-burner-358919.Cyclistic.Total_rides`;

SELECT 
  MIN (end_lng),
  MAX (end_lng),
  MIN (end_lat),
  MAX (end_lat),
  MIN (start_lng),
  MAX(start_lng),
  MIN (start_lat),
  MAX(start_lat),
  MIN(ride_length),
  MAX(ride_length)
FROM
  `rapid-burner-358919.Cyclistic.Total_rides`;

SELECT 
  end_station_id,
  end_station_name,
  COUNT(1)
FROM 
  `rapid-burner-358919.Cyclistic.Total_rides`
GROUP BY
  end_station_id,
  end_station_name;

SELECT 
  start_station_id,
  start_station_name,
  COUNT(1)
FROM 
  `rapid-burner-358919.Cyclistic.Total_rides`
GROUP BY
  start_station_id,
  start_station_name;

SELECT 
  rideable_type,
  COUNT(1)
FROM
  `rapid-burner-358919.Cyclistic.Total_rides`
GROUP BY
  rideable_type;

--check for duplicates in ride_id--

SELECT 
  ride_id,
  COUNT(1)
FROM 
  `rapid-burner-358919.Cyclistic.Total_rides`
GROUP BY
  ride_id
HAVING
  COUNT(1) > 1;

SELECT *
FROM
  `rapid-burner-358919.Cyclistic.Total_rides`
WHERE 
  started_at IS NULL AND ended_at IS NULL;

SELECT *
FROM
  `rapid-burner-358919.Cyclistic.Total_rides`
WHERE 
  started_at IS NULL or ended_at IS NULL;

SELECT *
FROM
  `rapid-burner-358919.Cyclistic.Total_rides`
WHERE
  rideable_type IS NULL;

SELECT *
FROM
  `rapid-burner-358919.Cyclistic.Total_rides`
WHERE
  member_casual IS NULL;

----Step 5: Identify and exclude data with anomalies (overwrite the table in these steps using the query setting)----

--exclude cases where ride length is less than or equal to 0 and started at is greater than or equal to ended at--

SELECT *
FROM 
  `rapid-burner-358919.Cyclistic.Total_rides`
WHERE
  ride_length > 0 AND started_at < ended_at
ORDER BY
  started_at;

--exclude cases where start station name and end station name are the base or the warehouse--

SELECT *
FROM
  `rapid-burner-358919.Cyclistic.Total_rides`
WHERE
  start_station_name NOT LIKE '%Base%Warehouse%' AND end_station_name NOT LIKE '%Base%Warehouse%'
ORDER BY
  started_at;

--exclude duplicates rows--

SELECT DISTINCT *
FROM 
  `rapid-burner-358919.Cyclistic.Total_rides`
ORDER BY
  started_at;
  
----Step 6: Create queries for data visualisations-----

--types of membership--

SELECT 
  member_casual,
  COUNT (member_casual) as trips
FROM
  `rapid-burner-358919.Cyclistic.Total_rides`
GROUP BY
  member_casual;

--types of bikes--

SELECT 
  rideable_type,
  member_casual, 
  COUNT (1) as trips
FROM
  `rapid-burner-358919.Cyclistic.Total_rides`
GROUP BY 
  rideable_type,
  member_casual;

--top 30 trips for members--

SELECT 
  start_station_name,
  end_station_name,
  COUNT (1) as trips
FROM
  `rapid-burner-358919.Cyclistic.Total_rides`
WHERE 
  member_casual = "member"
GROUP BY
  start_station_name,
  end_station_name
ORDER BY
  trips DESC
LIMIT 30;

--top 30 trips for casuals--

SELECT 
  start_station_name,
  end_station_name,
  COUNT (1) as trips
FROM
  `rapid-burner-358919.Cyclistic.Total_rides`
WHERE 
  member_casual = "casual"
GROUP BY
  start_station_name,
  end_station_name
ORDER BY
  trips DESC
LIMIT 30;

--average ride duration--

SELECT 
  member_casual,
  AVG (ride_length) AS average_duration
FROM
  `rapid-burner-358919.Cyclistic.Total_rides`
GROUP BY
  member_casual;

--number of trips between 0 and 1 hs--

SELECT
  member_casual,
  COUNT (1) AS trips
FROM
  `rapid-burner-358919.Cyclistic.Total_rides`
WHERE
  ride_length > 0 and ride_length < 60 
GROUP BY
  member_casual;

--number of trips between 1 and 2 hs--

SELECT
  member_casual,
  COUNT (1) AS trips
FROM
  `rapid-burner-358919.Cyclistic.Total_rides`
WHERE
  ride_length >= 60 and ride_length <= 120 
GROUP BY
  member_casual;

--top performing days of the week, divided by membership type and type of bike--

SELECT 
  member_casual,
  rideable_type,
  CAST(started_at AS STRING FORMAT "day") AS started_day,
  COUNT (1) AS trips
FROM
  `rapid-burner-358919.Cyclistic.Total_rides`
GROUP BY
  started_day,
  member_casual,
  rideable_type
ORDER BY
  trips DESC;

--top performing months, divided by membership type and type of bike--

SELECT 
  member_casual,
  rideable_type,
  CAST(started_at AS STRING FORMAT "month") as started_month,
  COUNT (1) AS trips
FROM
  `rapid-burner-358919.Cyclistic.Total_rides`
GROUP BY
  started_month,
  member_casual,
  rideable_type
ORDER BY
  trips DESC;

--top performing hours, divided by membership type and type of bike--

SELECT 
  member_casual,
  rideable_type,
  EXTRACT(HOUR FROM started_at) as started_hour,
  COUNT (1) AS trips
FROM
  `rapid-burner-358919.Cyclistic.Total_rides`
GROUP BY
  started_hour,
  member_casual,
  rideable_type
ORDER BY
  trips DESC
