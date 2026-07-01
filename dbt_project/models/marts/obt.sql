SELECT
    f.ride_id,
    f.status,
    f.fare,
    f.distance_km,
    f.rider_rating,
    f.driver_rating,
    f.vehicle_type,
    f.pickup,
    f.drop_loc,
    f.ride_date,
    f.ride_hour,
    f.ride_month,
    f.ride_year,

    d.driver_name,
    d.driver_activity,
    d.driver_rating_category,
    d.cancellation_rate AS driver_cancellation_rate,

    r.rider_name,
    r.city,
    r.rider_activity,
    r.rider_rating_category,
    r.total_spent AS rider_total_spent

FROM {{ ref('fact_rides') }}   f
LEFT JOIN {{ ref('dim_drivers') }} d ON f.driver_id = d.driver_id
LEFT JOIN {{ ref('dim_riders') }}  r ON f.rider_id  = r.rider_id