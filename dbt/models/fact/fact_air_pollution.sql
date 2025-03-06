{{ config(materialized='table') }}

WITH cities as (
    SELECT  
        id,
        city,
        latitude,
        longitude 
    FROM 
        {{ ref('staging_cities') }}
),

pollution AS (
    SELECT 
        *
    FROM 
        {{ ref('staging_air_pollution') }}
)

SELECT 
    city_id,
    city, 
    dt,
    pollution_quality,
    sulfur_dioxide_so2,
    nitrogen_dioxide_no2,
    pm10,
    pm2_5,
    ozone_o3,
    carbon_monoxide_co,
    nh3,
    nitric_oxide_no,
    latitude,
    longitude
FROM 
    pollution AS p
INNER JOIN 
    cities AS c
ON 
    p.city_id = c.id
ORDER BY 
    city, dt

