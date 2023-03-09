SELECT
    *
FROM {{ ref('int_user_event_with_sold') }}
WHERE user_id ="user_14889"

SELECT
    *
FROM {{ ref('users_parameters_model') }}
WHERE user_id ="user_14889"
