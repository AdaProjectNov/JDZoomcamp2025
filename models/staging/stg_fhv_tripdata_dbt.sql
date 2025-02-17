{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
  select *
  from {{ source('staging','fhv_tripdata_dbt') }}
  where extract(year from pickup_datetime) = 2019  -- 只保留2019年的记录
)
select
    -- Identifiers
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    
    -- Timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- Trip Info
    dispatching_base_num,
    {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }} as dropoff_locationid,

    -- SR_Flag: 保持 string 类型，空值用 NULL 代替
    NULLIF(SR_Flag, '') AS SR_Flag,

    -- Affiliated Base Number
    coalesce(Affiliated_base_number, '') as affiliated_base_number
from tripdata
