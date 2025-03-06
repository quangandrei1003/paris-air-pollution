WITH cities AS (
    SELECT *,
    row_number() over(partition by City_index) AS rn
    FROM {{ source('source','cities') }}
    ORDER BY City_index
)
SELECT 
    City_index AS id,
    City AS city,
    Latitude as latitude,
    Longitude AS longitude
FROM 
    cities