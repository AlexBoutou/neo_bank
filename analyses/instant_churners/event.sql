SELECT
    user_id,
    MAX(sold) as max_sold
FROM {{ ref('int_user_event_with_sold') }} us
RIGHT JOIN {{ ref('users_churned_after_time') }} ic using(user_id)
GROUP BY 1
ORDER BY 2 desc
;
SELECT
    user_id,
    MAX(sold) as max_sold
FROM {{ ref('int_user_event_with_sold') }} us
RIGHT JOIN {{ ref('users_life_time_users') }} ic using(user_id)
GROUP BY 1
ORDER BY 2 desc