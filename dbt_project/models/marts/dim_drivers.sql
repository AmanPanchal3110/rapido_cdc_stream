WITH driver_data AS(
    SELECT 
        driver_id,
        COUNT(CASE WHEN status='completed' THEN 1 END) AS completed_rides,
        COUNT(CASE WHEN status='cancelled' THEN 1 END) AS cancelled_rides,
        COUNT(*) AS total_rides
    FROM {{ref('stg_rides')}}
    GROUP BY driver_id
)
SELECT 
    d.driver_id,
    d.driver_name,
    d.vehicle_type,
    d.vehicle_no,
    d.is_active,
    d.avg_rating,
    d.driver_rating_category,
    COALESCE(ds.completed_rides,0) AS completed_rides,
    COALESCE(ds.cancelled_rides,0) AS cancelled_rides,
    COALESCE(ds.total_rides,0) AS total_rides,
    ROUND(
        COALESCE(ds.cancelled_rides,0)*100 / NULLIF(ds.total_rides,0),2
    ) AS  cancellation_rate,
    CASE 
        WHEN COALESCE(ds.total_rides,0)>=600 THEN 'HIGHLY ACTIVE'
        WHEN COALESCE(ds.total_rides,0)>=250 THEN 'ACTIVE'
        WHEN COALESCE(ds.total_rides,0)>=50 THEN 'MODERATE ACTIVE'
        ELSE 'NEW DRIVERS'
    END AS driver_activity,
    d.created_at
FROM {{ref('stg_drivers')}} AS d
LEFT JOIN driver_data AS ds on d.driver_id=ds.driver_id