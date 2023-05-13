CREATE OR REPLACE TABLE `uberdataengineering.uber_data_engineering.table_analytics` AS (

SELECT 
f.VendorID,
d.tpep_pickup_datetime,
d.tpep_dropoff_datetime,
p.passenger_count,
t.trip_distance,
r.rate_code_name,
pick.pickup_latitude,
pick.pickup_longitude,
drop.dropoff_latitude,
drop.dropoff_longitude,
pay.payment_type_name,
f.fare_amount,
f.extra,
f.mta_tax,
f.tip_amount,
f.tolls_amount,
f.improvement_surcharge,
f.total_amount
FROM 

`uberdataengineering.uber_data_engineering.fact_table` f
JOIN `uberdataengineering.uber_data_engineering.datetime_table`d  ON f.datetime_id=d.datetime_id

JOIN uberdataengineering.uber_data_engineering.passenger_count_table p ON p.passenger_count_id=f.passenger_count_id  

JOIN uberdataengineering.uber_data_engineering.trip_distance_table t ON t. trip_distance_id=f.trip_distance_id  
JOIN uberdataengineering.uber_data_engineering.rate_code_table r ON r.rate_code_id=f.rate_code_id  
JOIN uberdataengineering.uber_data_engineering.pickup_location_table pick ON pick.pickup_location_id=f.pickup_location_id
JOIN uberdataengineering.uber_data_engineering.dropoff_location_table drop ON drop.dropoff_location_id=f.dropoff_location_id
JOIN uberdataengineering.uber_data_engineering.payment_type_table pay ON pay.payment_type_id=f.payment_type_id)
;