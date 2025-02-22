{{ config(
    materialized='table'  
) }}

WITH combined_trips AS (
    SELECT * FROM {{ ref('stg_yellow_tripdata_dbt') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_green_tripdata_dbt') }}
)

SELECT DISTINCT  -- 去除重复行
    -- 提取年份 (e.g., 2019, 2020)
    EXTRACT(YEAR FROM pickup_datetime) AS year,

    -- 提取季度 (1, 2, 3, 4)
    EXTRACT(QUARTER FROM pickup_datetime) AS quarter,

    -- 生成 year_quarter (e.g., 2019/Q1, 2020-Q2)
    CONCAT(
        CAST(EXTRACT(YEAR FROM pickup_datetime) AS STRING), 
        '/Q', 
        CAST(EXTRACT(QUARTER FROM pickup_datetime) AS STRING)
    ) AS year_quarter,

    -- 提取月份 (1 - 12)
    EXTRACT(MONTH FROM pickup_datetime) AS month

FROM combined_trips
WHERE pickup_datetime IS NOT NULL  -- 去除空数据





