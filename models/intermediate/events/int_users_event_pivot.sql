select 
    user_id,
    --notifications
    sum(case when event_type = "sms" then 1 else 0 end) as sms,
    sum(case when event_type = "email" then 1 else 0 end) as email,
    sum(case when event_type = "push" then 1 else 0 end) as push,
    --transactions
    round(avg(case when event_detail = "topup" then amount else 0 end),2) as avg_amount_topup,
    sum(case when event_detail = "topup" then 1 else 0 end) as count_topup,
    round(avg(case when event_detail = "transfer" then amount else 0 end),2) as avg_amount_transfer,
    sum(case when event_detail = "transfer" then 1 else 0 end) as count_transfer,
    round(avg(case when event_detail = "tax" then amount else 0 end),2) as avg_amount_tax,
    sum(case when event_detail = "tax" then 1 else 0 end) as count_tax,
    round(avg(case when event_detail = "refunds" then amount else 0 end),2) as avg_amount_refunds,
    sum(case when event_detail = "refunds" then 1 else 0 end) as count_refunds,
    round(avg(case when event_detail = "fee" then amount else 0 end),2) as avg_amount_fee,
    sum(case when event_detail = "fee" then 1 else 0 end) as count_fee,
    round(avg(case when event_detail = "exchange" then amount else 0 end),2) as avg_amount_exchange,
    sum(case when event_detail = "exchange" then 1 else 0 end) as count_exchange,
    round(avg(case when event_detail = "cashback" then amount else 0 end),2) as avg_amount_cashback,
    sum(case when event_detail = "cashback" then 1 else 0 end) as count_cashback,
    round(avg(case when event_detail = "card_payment" then amount else 0 end),2) as avg_amount_card_payment,
    sum(case when event_detail = "card_payment" then 1 else 0 end) as count_card_payment,
    round(avg(case when event_detail = "atm" then amount else 0 end),2) as avg_amount_card_atm,
    sum(case when event_detail = "card_atm" then 1 else 0 end) as count_atm,            
FROM  {{ ref("int_users_event") }}
GROUP BY user_id