select * from uber_sales_analysis
---------------------------
select
column_name,
Data_type
from INFORMATION_SCHEMA.COLUMNS
where table_name = 'ncr_ride_bookings'
----------------------------
select
column_name,
Data_type
from INFORMATION_SCHEMA.COLUMNS
where table_name = 'uber_sales_analysis'

alter table uber_sales_analysis
alter column date DATE

alter table uber_sales_analysis
alter column time TIME(0)
----------------------------------------------
--1.Date Range Analysis
select 
datediff(Month,min(date),max(date)) as Month_range
from uber_sales_analysis

select 
datediff(DAY,min(date),max(date)) as Day_range
from uber_sales_analysis

select
min(date) as minimum_date,
max(date) as maximum_date
from uber_sales_analysis
-----------------------------
--Deleting Duplicates

with duplicate_details as
(
select *,
ROW_NUMBER() over(partition by booking_id order by date) as rw_num
from uber_sales_analysis
)

delete from duplicate_details
where rw_num >1

--------------------------------------------
/*COLUMNS
date
time
booking_id
booking_status
customer_id
vehicle_type
pickup_location
drop_location
avg_vtat
avg_ctat
trip_status
canelled_ride_by
cancelled_reasons
booking_value
ride_distance
driver_ratings
customer_rating
payment_method
*/
--------------------------------------------
--Key Metrics

select 'Total Bookings' as Metrics, count(DISTINCT booking_id) as Metric_Value from uber_sales_analysis
union all
select 'Total Customer', count(DISTINCT customer_id) from uber_sales_analysis
union all
select 'Total Booking Value', sum(booking_value) from uber_sales_analysis
union all
select 'Average Ride Distance', AVG(ride_distance) from uber_sales_analysis where trip_status = 'Completed'
union all
select 'Average Customer Rating', AVG(customer_rating) from uber_sales_analysis where trip_status = 'Completed'
--------------------------------------------
--1.Total Bookings by Booking Status

select *,
sum(total_bookings) over() as overall_bookings,
CONCAT(ROUND(CAST(total_bookings*100.0 as float)/sum(total_bookings) over(),2),'%') as StatusRatioPercent
from
(select
booking_status,
count(DISTINCT booking_id) as total_bookings
from uber_sales_analysis
group by booking_status) a
order by total_bookings desc
------------------------------
-- 2.Total Booking Value by Booking Status

select *,
sum(booking_value) over() as overall_revenue,
CONCAT(ROUND(CAST(booking_value*100.0 as float)/sum(booking_value) over(),2),'%') as StatusRatioPercent
from
(select
booking_status,
sum(booking_value) as booking_value
from uber_sales_analysis
group by booking_status) a
order by booking_value desc
------------------------------
--3.Total Bookings by Vehicle Type

select *,
sum(total_bookings) over() as overall_bookings,
CONCAT(ROUND(CAST(total_bookings*100.0 as float)/sum(total_bookings) over(),2),'%') as StatusRatioPercent
from
(select
vehicle_type,
count(DISTINCT booking_id) as total_bookings
from uber_sales_analysis
group by vehicle_type) a
order by total_bookings desc
-----------------------------
--4.Total Revenue by Vehicle Type

select *,
sum(booking_value) over() as overall_revenue,
CONCAT(ROUND(CAST(booking_value*100.0 as float)/sum(booking_value) over(),2),'%') as StatusRatioPercent
from
(select
vehicle_type,
sum(booking_value) as booking_value
from uber_sales_analysis
group by vehicle_type) a
order by booking_value desc
---------------------------
--5.Total Booking by Pickup Location

select *,
sum(total_bookings) over() as overall_bookings,
CONCAT(ROUND(CAST(total_bookings*100.0 as float)/sum(total_bookings) over(),2),'%') as StatusRatioPercent
from
(select
pickup_location,
count(DISTINCT booking_id) as total_bookings
from uber_sales_analysis
group by pickup_location) a
order by total_bookings desc
------------------------------
--6.Total Revenue by Pickup Location

select *,
sum(booking_value) over() as overall_revenue,
CONCAT(ROUND(CAST(booking_value*100.0 as float)/sum(booking_value) over(),2),'%') as StatusRatioPercent
from
(select
pickup_location,
sum(booking_value) as booking_value
from uber_sales_analysis
group by pickup_location) a
order by booking_value desc
---------------------------
--7.Total Bookings by Caneclled Ride By

select *,
sum(total_bookings) over() as overall_bookings,
CONCAT(ROUND(CAST(total_bookings*100.0 as float)/sum(total_bookings) over(),2),'%') as StatusRatioPercent
from
(select
canelled_ride_by,
cancelled_reasons,
count(DISTINCT booking_id) as total_bookings
from uber_sales_analysis
where trip_status <> 'Completed'
group by 
	canelled_ride_by,
	cancelled_reasons) a
order by total_bookings desc
----------------------------
--8.Total Bookings by Payment Method

select *,
sum(booking_value) over() as overall_revenue,
CONCAT(ROUND(CAST(booking_value*100.0 as float)/sum(booking_value) over(),2),'%') as StatusRatioPercent
from
(select
payment_method,
sum(booking_value) as booking_value
from uber_sales_analysis
group by payment_method) a
order by booking_value desc
---------------------------
--Estimated revenue loss due to cancellations

select 
booking_status,
sum(booking_value) as booking_value
from uber_sales_analysis
where trip_status = 'Cancelled'
group by booking_status
order by booking_value Desc
---------------------------

select *
from uber_sales_analysis
where booking_status = 'Incomplete'
--------------------------------------
--Estimated Revenue Loss due to cancellations

CREATE INDEX idx_tripstatus_vehicle_route
ON uber_sales_analysis (trip_status, vehicle_type, pickup_location, drop_location)


with completed_avg as
(
select
vehicle_type,
pickup_location,
drop_location,
AVG(booking_value) as Avg_booking_value
from uber_sales_analysis
where trip_status = 'Completed'
group by
	vehicle_type,
	pickup_location,
	drop_location
),
----
cancelled_count as
(
select
vehicle_type,
pickup_location,
drop_location,
count(*) as Cancelled_bookings
from uber_sales_analysis
where trip_status = 'Cancelled'
group by
	vehicle_type,
	pickup_location,
	drop_location
),
final_table as
(
select 
ca.*,
COALESCE(c.Cancelled_bookings,0) as Cancelled_bookings,
COALESCE(c.Cancelled_bookings,0)*ca.Avg_booking_value as Estimated_Rev_loss
from completed_avg ca
left join cancelled_count c
on ca.vehicle_type = c.vehicle_type and ca.pickup_location = c.pickup_location and ca.drop_location = c.drop_location)

select 
vehicle_type,
sum(Estimated_Rev_loss) as Estimated_revenue_loss
from final_table
group by vehicle_type
order by Estimated_revenue_loss Desc
--------------------------------------
--Pickup and drop location-wise cancellation rates

select top 25 *,
sum(cancelled_count) over() as total_cancelled,
concat(round(cast(cancelled_count*100.0 as float)/sum(cancelled_count) over(),2),'%') as Cancellation_rates
from
(
select
pickup_location,
drop_location,
 SUM(CASE 
                WHEN trip_status = 'Cancelled' 
                THEN 1 
                ELSE 0 
            END) AS cancelled_count
from uber_sales_analysis
where trip_status = 'Cancelled'
group by
	pickup_location,
	drop_location
) a
order by cancelled_count Desc
------------------------------
--Trend Analysis
----------------
--Quarter Wise
--------------
--Revenue
select *,
sum(revenue) over() as overall_revenue,
CONCAT(ROUND(CAST(revenue*100.0 as float)/sum(revenue) over(),2),'%') as contribution
from
(select
DATEPART(QUARTER,date) as Quarter,
sum(booking_value) as revenue
from uber_sales_analysis
group by DATEPART(QUARTER,date)) a
order by revenue desc
---------------------
--Booking Volume
select *,
sum(booking_volume) over() as Overall_booking_Volume,
CONCAT(ROUND(CAST(booking_volume*100.0 as float)/sum(booking_volume) over(),2),'%') as Contribution
from
(select
DATEPART(QUARTER,date) as Quarter,
count(DISTINCT booking_id) as booking_volume
from uber_sales_analysis
group by DATEPART(QUARTER,date)) a
order by booking_volume desc
----------------------------
--Cancellation
select *,
sum(booking_volume) over() as Overall_booking_Volume,
CONCAT(ROUND(CAST(booking_volume*100.0 as float)/sum(booking_volume) over(),2),'%') as Contribution
from
(select
DATEPART(QUARTER,date) as Quarter,
count(DISTINCT booking_id) as booking_volume
from uber_sales_analysis
where trip_status = 'Cancelled'
group by DATEPART(QUARTER,date)) a
order by booking_volume desc
----------------------------------
--Month Wise
--Revenue
select *,
sum(revenue) over() as overall_revenue,
CONCAT(ROUND(CAST(revenue*100.0 as float)/sum(revenue) over(),2),'%') as contribution
from
(select
MONTH(date) as Month,
DATENAME(MONTH,date) as month_name,
sum(booking_value) as revenue
from uber_sales_analysis
group by MONTH(date),DATENAME(MONTH,date)) a
order by MOnth
---------------
--Booking Volume
select *,
sum(booking_volume) over() as Overall_booking_Volume,
CONCAT(ROUND(CAST(booking_volume*100.0 as float)/sum(booking_volume) over(),2),'%') as Contribution
from
(select
MONTH(date) as Month,
DATENAME(MONTH,date) as month_name,
count(DISTINCT booking_id) as booking_volume
from uber_sales_analysis
group by MONTH(date),DATENAME(MONTH,date)) a
order by Month
---------------
--Cancellations
select *,
sum(booking_volume) over() as Overall_booking_Volume,
CONCAT(ROUND(CAST(booking_volume*100.0 as float)/sum(booking_volume) over(),2),'%') as Contribution
from
(select
MONTH(date) as Month,
DATENAME(MONTH,date) as month_name,
count(DISTINCT booking_id) as booking_volume
from uber_sales_analysis
where trip_status = 'Cancelled'
group by MONTH(date),DATENAME(MONTH,date)) a
order by Month
------------------
--Day Wise
----------
--Revenue

select *,
sum(revenue) over() as overall_revenue,
CONCAT(ROUND(CAST(revenue*100.0 as float)/sum(revenue) over(),2),'%') as contribution
from
(select
DATEPART(WEEKDAY,date) as Days,
DATENAME(WEEKDAY,date) as Day_name,
sum(booking_value) as revenue
from uber_sales_analysis
group by DATEPART(WEEKDAY,date),DATENAME(WEEKDAY,date)) a
order BY revenue dESC
----------------------
--Booking Volume

select *,
sum(booking_volume) over() as Overall_booking_Volume,
CONCAT(ROUND(CAST(booking_volume*100.0 as float)/sum(booking_volume) over(),2),'%') as Contribution
from
(select
DATEPART(WEEKDAY,date) as Days,
DATENAME(WEEKDAY,date) as Day_name,
count(DISTINCT booking_id) as booking_volume
from uber_sales_analysis
group by DATEPART(WEEKDAY,date),DATENAME(WEEKDAY,date)) a
order by booking_volume Desc
----------------------------
--Cancellations
select *,
sum(booking_volume) over() as Overall_booking_Volume,
CONCAT(ROUND(CAST(booking_volume*100.0 as float)/sum(booking_volume) over(),2),'%') as Contribution
from
(select
DATEPART(WEEKDAY,date) as Days,
DATENAME(WEEKDAY,date) as Day_name,
count(DISTINCT booking_id) as booking_volume
from uber_sales_analysis
where trip_status = 'Cancelled'
group by DATEPART(WEEKDAY,date),DATENAME(WEEKDAY,date)) a
order by booking_volume Desc
-----------------------------------------
--Time Segmentation

select
min(time) as minimum_time,
max(time) as maximum_time
from uber_sales_analysis
-----------------------
SELECT 
    time,
    CASE 
        WHEN  time BETWEEN '00:00:00' AND '05:59:59' THEN 'Night'
        WHEN time BETWEEN '06:00:00' AND '11:59:59' THEN 'Morning'
        WHEN time BETWEEN '12:00:00' AND '17:59:59' THEN 'Afternoon'
        ELSE 'Evening'
    END AS time_segment
FROM uber_sales_analysis;

select * from uber_sales_analysis

alter table uber_sales_analysis
add time_segment varchar(20)

update uber_sales_analysis
set time_segment =
CASE 
        WHEN  time BETWEEN '00:00:00' AND '05:59:59' THEN 'Night'
        WHEN time BETWEEN '06:00:00' AND '11:59:59' THEN 'Morning'
        WHEN time BETWEEN '12:00:00' AND '17:59:59' THEN 'Afternoon'
        ELSE 'Evening'
        end

------------------
--Time Wise
-----------
--Revenue

select *,
sum(revenue) over() as overall_revenue,
CONCAT(ROUND(CAST(revenue*100.0 as float)/sum(revenue) over(),2),'%') as contribution
from
(select
time_segment,
sum(booking_value) as revenue
from uber_sales_analysis
group by time_segment) a
order BY revenue dESC
---------------------
--Booking Volume

select *,
sum(booking_volume) over() as Overall_booking_Volume,
CONCAT(ROUND(CAST(booking_volume*100.0 as float)/sum(booking_volume) over(),2),'%') as Contribution
from
(select
time_segment,
count(DISTINCT booking_id) as booking_volume
from uber_sales_analysis
group by time_segment) a
order by booking_volume Desc
-----------------------------
--Cancellation Rate
select *,
sum(booking_volume) over() as Overall_booking_Volume,
CONCAT(ROUND(CAST(booking_volume*100.0 as float)/sum(booking_volume) over(),2),'%') as Contribution
from
(select
time_segment,
count(DISTINCT booking_id) as booking_volume
from uber_sales_analysis
where trip_status = 'Cancelled'
group by time_segment) a
order by booking_volume Desc
---------------------------------
--Location
--Location Cluster of No Driver Frequency

with no_driver_found_segments as
(
select *,
case 
    when total_bookings between 35 and 50 then 'Low Frequency'
    when total_bookings between 51 and 65 then 'Mid Frequency'
    when total_bookings > 65 then 'High Frequency'
    end as No_driver_freq
from
(
select
pickup_location,
count(booking_id) as total_bookings
from uber_sales_analysis
where booking_status = 'No Driver Found'
group by pickup_location
) a)
select 
No_driver_freq,
count(DISTINCT pickup_location) as Total_Cities
from no_driver_found_segments
group by No_driver_freq
order by Total_Cities Desc
---------------------------
--Location Cluster of Incomplete

with no_driver_found_segments as
(
select *,
case 
    when total_bookings between 30 and 45 then 'Low Frequency'
    when total_bookings between 46 and 60 then 'Mid Frequency'
    when total_bookings > 60 then 'High Frequency'
    end as Incomplete_freq
from
(
select
pickup_location,
count(booking_id) as total_bookings
from uber_sales_analysis
where booking_status = 'Incomplete'
group by pickup_location
) a)
select 
Incomplete_freq,
count(DISTINCT pickup_location) as Total_Cities
from no_driver_found_segments
group by Incomplete_freq
order by Total_Cities Desc

--Ratings
select * from uber_sales_analysis where trip_status = 'Cancelled'

select * from uber_sales_analysis where trip_status = 'Completed'

select
booking_status,
AVG(driver_ratings) as Avg_Driver_Rating,
AVG(customer_rating) as Avg_Customer_Rating
from uber_sales_analysis
group by booking_status
------------------------------------

select * from uber_sales_analysis


select
distinct vehicle_type
from uber_sales_analysis