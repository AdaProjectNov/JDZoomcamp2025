{{ config(
    materialized='table'
) }}

WITH trips AS (
    -- 计算 trip_duration
    SELECT 
        pickup_zone,
        dropoff_zone,
        pickup_locationid,
        dropoff_locationid,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(MONTH FROM pickup_datetime) AS month,
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) AS trip_duration
    FROM {{ ref('fhv_fact_trips') }}  
),

p90_trip_duration AS (
    -- 计算 P90 行程时长，按 年、月、上车地点、下车地点 分组
    SELECT
        year,
        month,
        pickup_zone,
        dropoff_zone,
        pickup_locationid,
        dropoff_locationid,
        PERCENTILE_CONT(trip_duration, 0.90) OVER (
            PARTITION BY year, month, pickup_locationid, dropoff_locationid
        ) AS p90_duration
    FROM trips
),

filtered_data AS (
    -- 筛选 2019 年 11 月，起点是 Newark Airport、SoHo、Yorkville East 的数据
    SELECT DISTINCT
        pickup_zone,
        dropoff_zone,
        p90_duration
    FROM p90_trip_duration
    WHERE year = 2019 AND month = 11
    AND pickup_zone IN ('Newark Airport', 'SoHo', 'Yorkville East')
),

ranked_data AS (
    -- 给每个 pickup_zone 内部的 dropoff_zone 按 P90 行程时间排序
    SELECT 
        pickup_zone,
        dropoff_zone,
        p90_duration,
        RANK() OVER (PARTITION BY pickup_zone ORDER BY p90_duration DESC) AS rank_num
    FROM filtered_data
)

-- 选择 P90 行程时间排名第 2 的 dropoff_zone
SELECT pickup_zone, dropoff_zone, p90_duration
FROM ranked_data
WHERE rank_num = 2
