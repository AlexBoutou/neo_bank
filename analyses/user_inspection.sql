SELECT
    *
FROM {{ ref('int_user_event_with_sold') }}
WHERE user_id ="user_13758"
;
SELECT
    *
FROM {{ ref('users_parameters_model') }}
WHERE user_id ="user_13758"
