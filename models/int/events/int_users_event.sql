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
    , "atm" as event_detail
    , - amount_usd as amount
    , lower(transaction_state) as event_status
FROM {{ ref('int_transactions_atm') }}

UNION ALL

SELECT 
    user_id
    , created_date as timestamp
    , "transaction" as event_type
    , "card_payment" as event_detail
    , - amount_usd as amount
    , lower(transaction_state) as event_status
FROM {{ ref('int_transactions_card_payment') }}

UNION ALL

SELECT 
    user_id
    , created_date as timestamp
    , "transaction" as event_type
    , "cashback" as event_detail
    , + amount_usd as amount
    , lower(transaction_state) as event_status
FROM {{ ref('int_transactions_cashback') }}

UNION ALL

SELECT 
    user_id
    , created_date as timestamp
    , "transaction" as event_type
    , "fee" as event_detail
    , - amount_usd as amount
    , "completed" as event_status
FROM {{ ref('int_transactions_fee') }}

UNION ALL

SELECT 
    user_id
    , created_date as timestamp
    , "transaction" as event_type
    , "refunds" as event_detail
    , + amount_usd as amount
    , lower(transaction_state) as event_status
FROM {{ ref('int_transactions_refunds') }}

UNION ALL

SELECT 
    user_id
    , created_date as timestamp
    , "transaction" as event_type
    , "tax" as event_detail
    , - amount_usd as amount
    , lower(transaction_state) as event_status
FROM {{ ref('int_transactions_tax') }}

UNION ALL

SELECT 
    user_id
    , created_date as timestamp
    , "transaction" as event_type
    , "topup" as event_detail
    , + amount_usd as amount
    , lower(transaction_state) as event_status
FROM {{ ref('int_transactions_topup') }}

UNION ALL

SELECT 
    user_id
    , created_date as timestamp
    , "transaction" as event_type
    , "exchange" as event_detail
    , - amount_usd as amount
    , lower(transaction_state) as event_status
FROM {{ ref('int_transactions_exchange') }}

UNION ALL

SELECT 
    user_id
    , created_date as timestamp
    , "transaction" as event_type
    , "exchange" as event_detail
    , amount_usd as amount
    , lower(transaction_state) as event_status
FROM {{ ref('int_transactions_exchange') }}

UNION ALL

SELECT 
    user_id
    , created_date as timestamp
    , "transaction" as event_type
    , "transfer" as event_detail
    , CASE 
        WHEN direction = "INBOUND" THEN amount_usd 
        ELSE - amount_usd
        END as amount
    , lower(transaction_state) as event_status
FROM {{ ref('int_transactions_transfer') }}
ORDER BY user_id,timestamp



