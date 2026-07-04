SELECT pickup,
        COUNT(*) AS total_rides,
        SUM(fare) AS total_revenue,
        ROUND(SUM(fare) / NULLIF(COUNT(*), 0), 2) AS avg_fare 
FROM {{ref('fact_rides')}}
GROUP BY pickup
ORDER BY total_revenue DESC