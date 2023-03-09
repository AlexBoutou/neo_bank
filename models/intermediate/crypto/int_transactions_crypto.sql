SELECT  
    t.transaction_id
    , t.transaction_currency
    , t.amount_usd
    , t.transaction_state
    , t.transaction_type
    , t.direction
    , t.user_id
    , t.created_date
FROM {{ref("stg_transactions")}} t
INNER JOIN {{ref("stg_currencies")}} c ON t.transaction_currency = c.currency_code

WHERE transaction_type = "EXCHANGE" AND c.is_crypto = 1