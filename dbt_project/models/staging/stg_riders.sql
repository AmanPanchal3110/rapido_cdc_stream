SELECT 
    r.rider_id,
    r.rider_name,
    r.phone,
    r.email,
    r.city,
    r.avg_rating,
    CASE
        WHEN r.avg_rating>=4.0 THEN 'EXCELLENT'
        WHEN r.avg_rating>=3.5 THEN 'GOOD'
        WHEN r.avg_rating>=3.0 THEN 'AVERAGE'
        ELSE 'NEED IMPORVEMENT'
    END AS rider_rating_category,
    r.created_at,
    r.updated_at
FROM {{source('raw','riders')}} AS r