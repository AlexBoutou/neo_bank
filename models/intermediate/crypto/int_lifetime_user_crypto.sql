SELECT
    us.user_id as user_id
    , us.created_date as account_creation_timestamp
    , las.last_transaction_timestamp as last_transaction_timestamp
    , DATE_DIFF(las.last_transaction_timestamp, us.created_date, DAY) as lifetime_days
    , las.last_transaction_sold as final_sold
    , CASE 
        WHEN EXTRACT(MONTH FROM las.last_transaction_timestamp) = 5 AND EXTRACT(YEAR FROM las.last_transaction_timestamp) = 2019 THEN 1
        ELSE 0
    END AS is_active
FROM {{ ref('stg_users') }} us
LEFT JOIN {{ ref('int_users_last_transaction_crypto') }} las using(user_id)