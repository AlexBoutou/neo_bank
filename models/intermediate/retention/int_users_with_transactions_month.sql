SELECT
    user_id,
    FORMAT_DATETIME("%Y, %m", timestamp) as month_year
FROM {{ ref('int_user_event_with_sold') }}
WHERE event_type="transaction"
GROUP BY 1,2
ORDER BY 1,2
