SELECT  
    transaction_id
    , transaction_currency
    , amount_usd
    , transaction_state
    , user_id
    , created_date
    ,CASE
        WHEN transaction_type = "CARD_REFUND" THEN 1
        ELSE 0
    END as is_card
FROM {{ref("stg_transactions")}}
WHERE transaction_type = "REFUND" OR transaction_type = "CARD_REFUND"