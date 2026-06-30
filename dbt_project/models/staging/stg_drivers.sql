WITH drivers_data AS (  
    SELECT 
        driver_id,
        COUNT(CASE WHEN status='completed' THEN 1 END) AS completed_rides,
        COUNT(CASE WHEN status='cancelled' then 1 END) AS cancelled_rides,
        COUNT(*) AS total_rides
    FROM {{ source('raw','rides')}}
    GROUP BY driver_id
)
SELECT 
    d.driver_id,
    d.driver_name,
    d.vehicle_type,
    d.vehicle_no,
    d.avg_rating,
    d.is_active,
    d.created_at,
    d.updated_at,
    COALESCE(ds.completed_rides,0) AS completed_rides,
    COALESCE(ds.cancelled_rides,0) AS cancelled_rides,
    COALESCE(ds.total_rides,0) AS total_rides,
    ROUND(
        COALESCE(ds.cancelled_rides,0)*100 / NULLIF(ds.total_rides,0),2
    ) AS  cancellation_rate
FROM {{source('raw','drivers')}} d
LEFT JOIN drivers_data ds ON d.driver_id=ds.driver_id