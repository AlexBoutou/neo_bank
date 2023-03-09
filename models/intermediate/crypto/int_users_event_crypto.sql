SELECT 
    user_id
    , created_date as timestamp
    , "create_account" as event_type
    , "" as event_detail
    , 0 as amount
    , "completed" as event_status
FROM {{ ref('stg_users') }}

UNION ALL

SELECT 
    user_id
    , created_date as timestamp
    , lower(channel) as event_type
    , lower(reason) as event_detail
    , 0 as amount
    , lower(status) as event_status
FROM {{ ref('stg_notifications') }}
    
UNION ALL

SELECT 
    user_id
    , created_date as timestamp
    , "transaction" as event_type
    , transaction_type as event_detail
    , amount_usd as amount
    , lower(transaction_state) as event_status
FROM {{ ref('int_transactions_crypto') }}
ORDER BY user_id,timestamp