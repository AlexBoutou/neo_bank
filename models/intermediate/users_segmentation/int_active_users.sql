SELECT
    user_id
FROM {{ ref('users_life_summary') }}
WHERE is_active = 1