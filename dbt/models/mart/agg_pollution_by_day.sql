{{ config(materialized='table') }}

WITH pollution AS (
    SELECT * FROM {{ ref('fact_air_pollution') }}
)

SELECT 
    DATE(dt) as date,
    city,
    CASE 
        WHEN COUNTIF(pollution_quality = 'Very_Poor') > 0 THEN 'Very_Poor'
        WHEN COUNTIF(pollution_quality = 'Poor') > 0 THEN 'Poor'
        WHEN COUNTIF(pollution_quality = 'Moderate') > 0 THEN 'Moderate'
        WHEN COUNTIF(pollution_quality = 'Fair') > 0 THEN 'Fair'
        WHEN COUNTIF(pollution_quality = 'Good') > 0 THEN 'Good'
        ELSE 'Unknown'
    END AS pollution_quality,
    COUNT(DISTINCT DATE(dt)) as count_days,
    ROUND(AVG(sulfur_dioxide_so2),2) as avg_SO2,
    ROUND(AVG(nitrogen_dioxide_no2),2) as avg_NO2,
    ROUND(AVG(pm10),2) as avg_PM10,
    ROUND(AVG(pm2_5),2) as avg_PM2_5,
    ROUND(AVG(ozone_o3),2) as avg_O3,
    ROUND(AVG(carbon_monoxide_co),2) as avg_CO,
    ROUND(AVG(nh3),2) as avg_NH3,
    ROUND(AVG(nitric_oxide_no),2) as avg_NO
FROM 
  pollution
WHERE 
  pollution_quality IN ('Very_Poor', 'Poor', 'Moderate', 'Fair', 'Good')
GROUP BY 
  1, 2
ORDER BY 
  date ASC
