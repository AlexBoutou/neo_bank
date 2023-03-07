SELECT  
    transaction_id
    , transaction_currency
    , amount_usd
    , transaction_state
    , user_id
    , created_date
FROM {{ref("stg_transactions")}}
WHERE transaction_type = "TOPUP"