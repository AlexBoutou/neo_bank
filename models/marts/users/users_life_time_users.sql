SELECT
    user_id
FROM {{ ref('int_lifetime_user') }}
