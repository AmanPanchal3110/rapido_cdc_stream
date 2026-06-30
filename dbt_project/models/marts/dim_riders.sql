WITH rider_data AS (
    SELECT 
        rider_id,
        COUNT(CASE WHEN status='completed' THEN 1 END) AS completed_rides,
        COUNT(CASE WHEN status='cancelled' THEN 1 END) AS cancelled_rides,
        COUNT(*) AS total_rides,
        SUM(CASE WHEN status='completed' THEN fare END) AS total_spents
    FROM {{ref('stg_rides')}}
    GROUP BY rider_id
)
SELECT 
    r.rider_id,
    r.rider_name,
    r.phone,
    r.email,
    r.city,
    r.avg_rating,
    r.rider_rating_category,
    COALESCE(rs.completed_rides,0) AS completed_rides,
    COALESCE(rs.cancelled_rides,0) AS cancelled_rides,
    COALESCE(rs.total_rides,0) AS total_rides,
    COALESCE(rs.total_spents,0) AS total_spent,
    ROUND(
        COALESCE(rs.cancelled_rides,0)*100 / NULLIF(rs.total_rides,0),2
    ) AS cancellation_rate,
    CASE 
        WHEN COALESCE(rs.total_rides,0)>=600 THEN 'HIGHLY ACTIVE'
        WHEN COALESCE(rs.total_rides,0)>=250 THEN 'ACTIVE'
        WHEN COALESCE(rs.total_rides,0)>=50 THEN 'MODERATE ACTIVE'
        ELSE 'NEW RIDER'
    END AS rider_activity
FROM {{ref('stg_riders')}} AS r
LEFT JOIN rider_data AS rs ON r.rider_id=rs.rider_id
