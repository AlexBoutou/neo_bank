SELECT
    *
FROM {{ ref('users_life_summary') }}
WHERE nb_active_period=0