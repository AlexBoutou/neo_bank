SELECT  
    transaction_id
    , transactions_type as transaction_type
    , transactions_currency as transactions_currency
    , amount_usd as amount_usd
    , transactions_state as transaction_state
    , ea_cardholderpresence 
    , CAST(ea_merchant_mcc AS INT64) as merchant_mcc
    , ea_merchant_city
    , ea_merchant_country
    , direction
    , user_id
    , created_date
FROM raw_data.transactions