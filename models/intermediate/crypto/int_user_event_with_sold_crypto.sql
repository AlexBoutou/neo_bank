SELECT
    user_id
    , timestamp
    , event_type
    , event_detail
    , amount
    , event_status
    , ROUND(SUM(amount) OVER(PARTITION BY user_id ORDER BY timestamp RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),2) as sold
FROM {{ ref('int_users_event_crypto') }}
ORDER BY user_id, timestamp