SELECT * FROM cleaned_environment;

-- Task 1: Find the average temperature recorded for each device.

select device_id,avg(temperature) 
from cleaned_environment
group by device_id;

-- Task 2: Retrieve the top 5 devices with the highest average carbon monoxide levels.

select device_id,avg(carbon_monoxide)
from cleaned_environment
group by device_id
order by device_id desc
limit 5;

-- Task 3: Calculate the average temperature recorded in the cleaned_environment table

select avg(temperature) as avg_temperature
from cleaned_environment;

-- Task 4: Find the timestamp and temperature of the highest recorded temperature for each device.
/*  This task requires identifying the highest recorded temperature for each device and retrieving 
the corresponding timestamp and temperature values.*/

select distinct device_id,timestamp,temperature from cleaned_environment
where temperature in (
select max(temperature)
from cleaned_environment
group by device_id);


-- Task 5: Identify devices where the temperature has increased from the minimum recorded temperature to the maximum recorded temperature
/*  The goal is to Identify devices where the temperature has increased from the minimum 
recorded temperature to the maximum recorded temperature */

SELECT device_id
FROM cleaned_environment
GROUP BY device_id
HAVING (MAX(temperature) - MIN(temperature)) > 0;

-- Task 6: Calculate the exponential moving average of temperature for each device limit to 10 devices.
/* Calculate the exponential moving average (EMA) of the temperature for each device.
 Retrieve the device ID, timestamp, temperature, and the EMA temperature for the first 10 devices from the 'cleaned_environment' table. 
 The EMA temperature is calculated by partitioning the data based on the device ID, ordering it by the timestamp, 
 and considering all preceding rows up to the current row */
 
 -- select device_id,timestamp,temperature, EMA
  
SELECT device_id,
timestamp,
temperature,
AVG(temperature) OVER (PARTITION BY device_id ORDER BY timestamp
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS 'ema'
FROM cleaned_environment
LIMIT 10;
 

-- Task 7 : Find the timestamps and devices where carbon monoxide level exceeds the average carbon monoxide level of all devices.
/* The objective is to identify the timestamps and devices where the carbon monoxide level
 exceeds the average carbon monoxide level across all devices. */

select timestamp,device_id from cleaned_environment where carbon_monoxide > (select avg(carbon_monoxide) from cleaned_environment);

-- Task 8: Retrieve the devices with the highest average temperature recorded.
-- The objective is to identify the devices that have recorded the highest average temperature among all the devices in the dataset.

select device_id, AVG(temperature) as avg_temp 
from cleaned_environment 
group by device_id 
order by avg_temp DESC;


-- Task 9: Calculate the average temperature for each hour of the day across all devices.
-- The goal is to calculate the average temperature for each hour of the day, considering data from all devices.

select hour_of_day,( select avg(temperature) as average_temperature from cleaned_environment) as average_temperature
from ( select distinct hour(timestamp) as hour_of_day from cleaned_environment) new;

-- Task 10: Which device(s) in the cleaned environment dataset have recorded only a single distinct temperature value?
-- The objective is to identify device(s) in the cleaned environment dataset have recorded only a single distinct temperature value.

select device_id 
from cleaned_environment
group by device_id
having count(distinct temperature) = 1;

-- Task 11 : Find the devices with the highest humidity levels.
-- The objective is to identify the devices that have recorded the highest humidity levels.


select device_id,max(humidity) from cleaned_environment
group by device_id;

-- task 12: Calculate the average temperature for each device, excluding outliers (temperatures beyond 3 standard deviations).
/* This task requires calculating the average temperature for each device while excluding outliers, 
which are temperatures beyond 3 standard deviations from the mean.*/


SELECT device_id,AVG(temperature)
FROM cleaned_environment
WHERE temperature BETWEEN (SELECT (AVG(temperature)- 3 * STD(temperature)) FROM cleaned_environment) AND
(SELECT (AVG(temperature)+ 3 * STD(temperature)) FROM cleaned_environment)
GROUP BY device_id;

-- task13: Retrieve the devices that have experienced a sudden change in humidity (greater than 50% difference) within a 30-minute window.
-- The goal is to identify devices that have undergone a sudden change in humidity,
-- where the difference is greater than 50%, within a 30-minute time window.


SELECT table1.device_id, table1.timestamp, table1.humidity
FROM
(SELECT device_id, timestamp,
humidity,
LAG(humidity,1) OVER (
PARTITION BY device_id
ORDER BY timestamp),
(humidity - (LAG(humidity,1) OVER (
PARTITION BY device_id
ORDER BY timestamp))) diff,
ABS((humidity - (LAG(humidity,1) OVER (
PARTITION BY device_id
ORDER BY timestamp)))*100) c1
FROM `cleaned_environment`) table1
WHERE table1.c1 > 50;

-- task 14: Find the average temperature for each device during weekdays and weekends separately.
-- This task involves calculating the average temperature for each device separately for weekdays and weekends.


select device_id,avg(temperature) from cleaned_environment
group by device_id;

with day_cte as (
select *,
case timestamp
when dayname(timestamp) in ('Saturday','Sunday') then 'Weekend'
else 'Weekday'
end as day_type
from cleaned_environment)
-- select * from day_cte;
select device_id,day_type,avg(temperature) as average_temperature
from day_cte
group by device_id,day_type;

-- (or) 

SELECT device_id,
CASE
WHEN DAYOFWEEK(timestamp) IN (1, 7) THEN 'Weekend'
ELSE 'Weekday'
END AS day_type,
AVG(temperature) AS avg_temperature
FROM cleaned_environment
GROUP BY device_id, day_type;

-- task 15: Calculate the cumulative sum of temperature for each device, ordered by timestamp limit to 10.
-- The objective is to calculate the cumulative sum of temperature for each device, considering the records ordered by timestamp limit to 10

SELECT device_id, timestamp, temperature,
SUM(temperature) OVER (PARTITION BY device_id ORDER BY timestamp) AS cumulative_temperature
FROM cleaned_environment
LIMIT 10;
