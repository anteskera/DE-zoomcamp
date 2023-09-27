-- 179 MB
select * from `glossy-ally-396206.de_zoomcamp.rides` limit 10;

create or replace table `glossy-ally-396206.de_zoomcamp.rides_partitioned` 
partition by DATE(tpep_pickup_datetime) as
select * from `glossy-ally-396206.de_zoomcamp.rides` ;

select distinct DATE(tpep_pickup_datetime) from `glossy-ally-396206.de_zoomcamp.rides_partitioned`;

-- 31 MB processed data
select tpep_pickup_datetime, tpep_dropoff_datetime, VendorID from `glossy-ally-396206.de_zoomcamp.rides`
where DATE(tpep_pickup_datetime) between '2021-01-20' and '2021-01-24';

-- partitioning reduces to 5 MB
select tpep_pickup_datetime, tpep_dropoff_datetime, VendorID from `glossy-ally-396206.de_zoomcamp.rides_partitioned`
where DATE(tpep_pickup_datetime) between '2021-01-20' and '2021-01-24';


SELECT table_name, partition_id, total_rows
FROM `de_zoomcamp.INFORMATION_SCHEMA.PARTITIONS`
ORDER BY total_rows DESC;

CREATE OR REPLACE TABLE `de_zoomcamp.rides_clustered`
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `de_zoomcamp.rides`;

