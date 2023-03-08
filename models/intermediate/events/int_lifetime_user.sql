SELECT
    us.user_id
    , us.created_date
    , las.last_transaction_timestamp
    , DATE_DIFF(las.last_transaction_timestamp, us.created_date, DAY) as lifetime_days
FROM {{ ref('stg_users') }} us
LEFT JOIN {{ ref('int_users_last_transaction') }} las using(user_id)