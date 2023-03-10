SELECT
    user_id
FROM {{ ref('int_lifetime_user') }}
WHERE is_active = 1