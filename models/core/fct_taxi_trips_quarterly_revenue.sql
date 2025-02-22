{{
    config(
        materialized='table'
    )
}}

WITH quarterly_revenue AS (
    SELECT 
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(QUARTER FROM pickup_datetime) AS quarter,
        service_type,
        SUM(total_amount) AS quarterly_revenue
    FROM {{ ref('fact_trips') }}  -- 直接从 fact_trips 获取数据
    GROUP BY year, quarter, service_type
),

revenue_comparison AS (
    SELECT 
        q1.year AS current_year,
        q1.quarter,
        q1.service_type,
        q1.quarterly_revenue AS current_revenue,
        q2.quarterly_revenue AS previous_revenue,
        -- 计算 YoY 增长率: (今年收入 - 去年收入) / 去年收入 * 100
        SAFE_DIVIDE(q1.quarterly_revenue - q2.quarterly_revenue, q2.quarterly_revenue) * 100 AS yoy_growth
    FROM quarterly_revenue q1
    LEFT JOIN quarterly_revenue q2
    ON q1.quarter = q2.quarter  -- 按季度匹配
    AND q1.service_type = q2.service_type  -- 按出租车类型匹配
    AND q1.year = q2.year + 1  -- 今年 vs 去年
)

SELECT * FROM revenue_comparison
ORDER BY current_year, quarter, service_type

