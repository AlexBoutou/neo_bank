SELECT 
    user_id
    , created_date as timestamp
    , "create_account" as event_type
    , "" as event_detail
    , 0 as amount
FROM {{ ref('stg_users') }}

UNION ALL

SELECT 
    user_id
    , created_date as timestamp
    , channel as event_type
    , reason as event_detail
    , 0 as amount
FROM {{ ref('stg_notifications') }}
    
UNION ALL

SELECT 
    user_id
    , created_date as timestamp
    , "transaction" as event_type
    , "atm" as event_detail
    , - amount_usd as amount
FROM {{ ref('dim_transactions_atm') }}
WHERE transaction_state = "COMPLETED"

UNION ALL

SELECT 
    user_id
    , created_date as timestamp
    , "transaction" as event_type
    , "card_payment" as event_detail
    , - amount_usd as amount
FROM {{ ref('dim_transactions_card_payment') }}
WHERE transaction_state = "COMPLETED"

UNION ALL

SELECT 
    user_id
    , created_date as timestamp
    , "transaction" as event_type
    , "cashback" as event_detail
    , + amount_usd as amount
FROM {{ ref('dim_transactions_cashback') }}
WHERE transaction_state = "COMPLETED"

UNION ALL

SELECT 
    user_id
    , created_date as timestamp
    , "transaction" as event_type
    , "fee" as event_detail
    , - amount_usd as amount
FROM {{ ref('dim_transactions_fee') }}

UNION ALL

SELECT 
    user_id
    , created_date as timestamp
    , "transaction" as event_type
    , "refunds" as event_detail
    , + amount_usd as amount
FROM {{ ref('dim_transactions_refunds') }}
WHERE transaction_state = "COMPLETED"

UNION ALL

SELECT 
    user_id
    , created_date as timestamp
    , "transaction" as event_type
    , "tax" as event_detail
    , - amount_usd as amount
FROM {{ ref('dim_transactions_tax') }}
WHERE transaction_state = "COMPLETED"

UNION ALL

SELECT 
    user_id
    , created_date as timestamp
    , "transaction" as event_type
    , "topup" as event_detail
    , + amount_usd as amount
FROM {{ ref('dim_transactions_topup') }}
WHERE transaction_state = "COMPLETED"

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
FROM {{ ref('dim_transactions_transfer') }}
WHERE transaction_state = "COMPLETED"
ORDER BY user_id,timestamp



