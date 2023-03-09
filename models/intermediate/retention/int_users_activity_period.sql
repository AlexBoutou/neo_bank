with user_cross_month AS (
    SELECT
        user_id,
        month_year
    FROM {{ ref('stg_users') }}
    CROSS JOIN {{ ref('int_events_month') }}
    order by 1,2)
SELECT
    um.user_id,
    um.month_year,
    CASE 
        WHEN ut.user_id is not null THEN 1
        ELSE 0
    END as is_active_month
FROM user_cross_month um
LEFT JOIN {{ ref("int_users_with_transactions_month") }} ut on um.user_id = ut.user_id and um.month_year = ut.month_year
ORDER BY 1,2