WITH complementary_users AS (
    SELECT
        user_id
    FROM {{ ref('int_instant_churners') }}

    UNION ALL

    SELECT
        user_id
    FROM {{ ref('int_active_users') }}
)
SELECT
    us.user_id
FROM {{ ref('stg_users') }} us 
LEFT JOIN complementary_users cu using(user_id)
WHERE cu.user_id is null