SELECT 
    *
FROM (
    SELECT DISTINCT 
        user_id
        , event_type as last_transaction_event_type
        , event_detail as last_transaction_event_detail
        , event_status as last_transaction_event_status
        , timestamp as last_transaction_timestamp
        , amount as last_transaction_amount
        , sold as last_transaction_sold
        , ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY timestamp DESC) as rn
    FROM {{ ref('int_user_event_with_sold') }}
    WHERE event_type = "transaction"
)
WHERE rn = 1
ORDER BY user_id