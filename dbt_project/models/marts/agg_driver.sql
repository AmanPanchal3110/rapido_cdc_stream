SELECT
    d.driver_id,
    d.driver_name,
    d.vehicle_type,
    d.avg_rating,
    d.driver_activity,
    d.driver_rating_category,
    d.completed_rides,
    d.cancelled_rides,
    d.cancellation_rate,
    SUM(f.fare) AS total_revenue,
    AVG(f.fare) AS avg_fare_per_ride,
    AVG(f.distance_km) AS avg_distance
FROM {{ref('dim_drivers')}} AS d
LEFT JOIN {{ref('fact_rides')}} AS f
ON d.driver_id=f.driver_id AND f.status='completed'
GROUP BY
    d.driver_id, d.driver_name, d.vehicle_type,
    d.avg_rating, d.driver_activity, d.driver_rating_category,
    d.completed_rides, d.cancelled_rides, d.cancellation_rate