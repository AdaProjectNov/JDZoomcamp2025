{{
    config(
        materialized='table'
    )
}}

-- 读取 FHV 数据，并确保 pickup 和 dropoff 位置已知
with fhv_tripdata as (
    select *
    from {{ref('stg_fhv_tripdata_dbt')}}
    where pickup_locationid is not null and dropoff_locationid is not null
), 

-- 读取地理区域维度表，去掉 'Unknown' 区域
dim_zones as (
    select * 
    from {{ref('dim_zones')}}
    where borough != 'Unknown'
)

-- 选择并映射字段，同时关联 pickup 和 dropoff 区域
select 
    fhv_tripdata.tripid, 
    fhv_tripdata.dispatching_base_num, 
    fhv_tripdata.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    fhv_tripdata.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    fhv_tripdata.pickup_datetime, 
    fhv_tripdata.dropoff_datetime,
    fhv_tripdata.SR_Flag,
    fhv_tripdata.affiliated_base_number
from fhv_tripdata

-- 关联 pickup 地区信息
inner join dim_zones as pickup_zone
on fhv_tripdata.pickup_locationid = pickup_zone.locationid

-- 关联 dropoff 地区信息
inner join dim_zones as dropoff_zone
on fhv_tripdata.dropoff_locationid = dropoff_zone.locationid
