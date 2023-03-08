SELECT 
    *
FROM (
    SELECT DISTINCT 
        user_id
        , event_type
        , event_detail
        , event_status
        , timestamp
        , amount
        , sold
        , ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY timestamp DESC) as rn
    FROM {{ ref('int_user_event_with_sold') }}
    WHERE event_type = "transaction"
)
WHERE rn = 1
ORDER BY user_id