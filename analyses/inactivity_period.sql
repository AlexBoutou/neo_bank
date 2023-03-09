SELECT
    COUNT(DISTINCT user_id),
    AVG(nb_month)
FROM {{ ref('users_life_split') }}
WHERE is_active_month=0 AND is_last_period=0