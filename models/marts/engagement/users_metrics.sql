WITH user_life_events AS (
    SELECT
        user_id,
        ROUND(AVG(nb_event),2) as monthly_transac
    FROM (
        SELECT
            user_id,
            month, 
            sum(nb_event) as nb_event
        FROM {{ ref('user_event_monthly') }}
        WHERE event_type = "transaction"
        group by 1,2)
    GROUP BY 1
)

SELECT
    us.user_id
    , ul.is_active
    , ul.active_ratio
    , ue.monthly_transac
    , CASE 
        WHEN ue.monthly_transac>10 THEN 1
        ELSE 0 
    END as high_monthly_transac
    , CASE 
        WHEN ul.active_ratio>=0.89 THEN 1
        ELSE 0 
    END as high_active_ratio
FROM {{ ref('stg_users') }} us
LEFT JOIN {{ ref('users_life_summary') }} ul on us.user_id = ul.user_id
LEFT JOIN user_life_events ue on us.user_id = ue.user_id