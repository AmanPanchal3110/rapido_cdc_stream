SELECT 
    d.driver_id,
    d.driver_name,
    d.vehicle_type,
    d.vehicle_no,
    d.is_active,
    d.avg_rating,
    CASE 
        WHEN d.avg_rating>=4.0 THEN 'EXCELLENT'
        WHEN d.avg_rating>=3.5 THEN 'GOOD'
        WHEN d.avg_rating>=3.0 THEN 'AVERAGE'
        ELSE 'NEED IMPORVEMENT'
    END AS driver_rating_category,
    d.created_at,
    d.updated_at
FROM {{source('raw','drivers')}} AS d