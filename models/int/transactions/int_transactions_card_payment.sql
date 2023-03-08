SELECT  
    transaction_id
    , transaction_currency
    , amount_usd
    , transaction_state
    , ea_cardholderpresence 
    , merchant_mcc
    , ea_merchant_city
    , ea_merchant_country
    , user_id
    , created_date
FROM {{ref("stg_transactions")}}
WHERE transaction_type = "CARD_PAYMENT"