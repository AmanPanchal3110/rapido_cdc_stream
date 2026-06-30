SELECT 
    ride_id,
    rider_id,
    driver_id,
    status,
    pickup,
    drop_loc,
    vehicle_type,
    fare,
    distance_km,
    rider_rating,
    driver_rating,
    created_at,
    updated_at,
    DATE(created_at) AS ride_date,
    EXTRACT(HOURS FROM created_at) AS ride_hour,
    EXTRACT(MONTH FROM created_at) AS ride_month,
    EXTRACT(YEAR FROM created_at) AS ride_year
FROM {{source('raw','rides')}}
    