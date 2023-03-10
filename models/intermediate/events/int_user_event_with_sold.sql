SELECT
    user_id
    , timestamp
    , event_type
    , event_detail
    , amount
    , event_status
    , ROUND(SUM(IF(event_status = 'completed',amount,0)) OVER(PARTITION BY user_id ORDER BY timestamp RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),2) as sold
FROM {{ ref('int_users_event') }}
ORDER BY user_id, timestamp