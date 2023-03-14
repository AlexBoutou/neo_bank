SELECT 
    u.user_id
    , u.created_date as timestamp
    , "create_account" as event_type
    , "" as event_detail
    , 0 as amount
    , "completed" as event_status
    , "" as token
FROM 
    {{ ref('stg_users') }} u
INNER JOIN
    {{ ref('int_transactions_crypto') }} t
ON
    u.user_id = t.user_id

UNION ALL

SELECT 
    n.user_id
    , n.created_date as timestamp
    , lower(n.channel) as event_type
    , lower(n.reason) as event_detail
    , 0 as amount
    , lower(n.status) as event_status
    , "" as token
FROM 
    {{ ref('stg_notifications') }} n
INNER JOIN
    {{ ref('int_transactions_crypto') }} t
ON
    n.user_id = t.user_id
    
UNION ALL

SELECT 
    t.user_id
    , t.created_date as timestamp
    , "transaction" as event_type
    , t.transaction_type as event_detail
    , t.amount_usd as amount
    , lower(t.transaction_state) as event_status
    , t.transaction_currency as token
FROM 
    {{ ref('int_transactions_crypto') }} t
ORDER BY user_id,timestamp
