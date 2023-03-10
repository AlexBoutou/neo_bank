SELECT
    user_id
FROM {{ ref('int_lifetime_user') }}
WHERE last_transaction_timestamp is null