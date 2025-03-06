{{ config(
    materialized='incremental', 
    on_schema_change='fail'
)}}


WITH pollution AS (
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY City_index, dt) AS rn
    FROM {{ source('source','airpollution') }}
    ORDER BY City_index
)
select 
    City_index AS city_id,
    dt,
    {{ get_pollution_quality('Sulfur_Dioxide_SO2', 'Nitrogen_Dioxide_NO2', 'PM10', 'PM2_5', 'Ozone_O3', 'Carbon_Monoxide_CO') }} AS pollution_quality,
    Sulfur_Dioxide_SO2 AS sulfur_dioxide_so2,
    Nitrogen_Dioxide_NO2 AS nitrogen_dioxide_no2,
    PM10 AS pm10,
    PM2_5 AS pm2_5,
    Ozone_O3 AS ozone_o3,
    Carbon_Monoxide_CO AS carbon_monoxide_co,
    NH3 AS nh3,
    Nitric_oxide_NO AS nitric_oxide_no
from 
    pollution
{% if is_incremental() %}
AND dt > (SELECT MAX(dt) FROM {{ this }})
{% endif %}

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=false) %}

  limit 300

{% endif %}