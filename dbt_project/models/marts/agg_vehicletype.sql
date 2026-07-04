SELECT 
    ride_date,
    SUM(CASE WHEN vehicle_type = 'auto' THEN fare ELSE 0 END) AS total_revenue_auto,
    SUM(CASE WHEN vehicle_type = 'car' THEN fare ELSE 0 END) AS total_revenue_car,
    SUM(CASE WHEN vehicle_type = 'bike' THEN fare ELSE 0 END) AS total_revenue_bike,

    COUNT(CASE WHEN vehicle_type = 'auto' THEN 1 END) AS total_rides_auto,
    COUNT(CASE WHEN vehicle_type = 'car' THEN 1 END) AS total_rides_car,
    COUNT(CASE WHEN vehicle_type = 'bike' THEN 1 END) AS total_rides_bike,

    ROUND(SUM(CASE WHEN vehicle_type = 'auto' THEN fare END) / 
          NULLIF(COUNT(CASE WHEN vehicle_type = 'auto' THEN 1 END), 0), 2) AS avg_fare_auto,
    ROUND(SUM(CASE WHEN vehicle_type = 'car' THEN fare END) / 
          NULLIF(COUNT(CASE WHEN vehicle_type = 'car' THEN 1 END), 0), 2) AS avg_fare_car,
    ROUND(SUM(CASE WHEN vehicle_type = 'bike' THEN fare END) / 
          NULLIF(COUNT(CASE WHEN vehicle_type = 'bike' THEN 1 END), 0), 2) AS avg_fare_bike

FROM {{ ref('fact_rides') }}
WHERE status = 'completed'
GROUP BY ride_date
ORDER BY ride_date