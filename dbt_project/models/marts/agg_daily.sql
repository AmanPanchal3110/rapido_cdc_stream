SELECT
    ride_date,
    COUNT(*) AS total_rides,
    COUNT(CASE WHEN status='completed' THEN 1 END) AS completed_rides,
    COUNT(CASE WHEN status='cancelled' THEN 1 END) AS cancelled_rides,
    SUM(fare) AS total_revenue,
    AVG(fare) AS avg_fare,
    AVG(distance_km) AS avg_distance,
    ROUND(
        COUNT(CASE WHEN status='cancelled' THEN 1 END)*100.0 / NULLIF(COUNT(*),0),2
    ) AS cancellation_rate
FROM {{ref('fact_rides')}}
GROUP BY ride_date
ORDER BY ride_date 