select 
    user_id,
    --notifications
    sum(case when event_type = "sms" then 1 else 0 end) as sms,
    sum(case when event_type = "email" then 1 else 0 end) as email,
    sum(case when event_type = "push" then 1 else 0 end) as push,
    --transactions
    round(sum(case when event_detail = "topup" then amount else 0 end),2) as sum_amount_topup,
    sum(case when event_detail = "topup" then 1 else 0 end) as count_topup,
    round(sum(case when event_detail = "transfer" then amount else 0 end),2) as sum_amount_transfer,
    sum(case when event_detail = "transfer" then 1 else 0 end) as count_transfer,
    round(sum(case when event_detail = "tax" then amount else 0 end),2) as sum_amount_tax,
    sum(case when event_detail = "tax" then 1 else 0 end) as count_tax,
    round(sum(case when event_detail = "refunds" then amount else 0 end),2) as sum_amount_refunds,
    sum(case when event_detail = "refunds" then 1 else 0 end) as count_refunds,
    round(sum(case when event_detail = "fee" then amount else 0 end),2) as sum_amount_fee,
    sum(case when event_detail = "fee" then 1 else 0 end) as count_fee,
    round(sum(case when event_detail = "exchange" then amount else 0 end),2) as sum_amount_exchange,
    sum(case when event_detail = "exchange" then 1 else 0 end) as count_exchange,
    round(sum(case when event_detail = "cashback" then amount else 0 end),2) as sum_amount_cashback,
    sum(case when event_detail = "cashback" then 1 else 0 end) as count_cashback,
    round(sum(case when event_detail = "card_payment" then amount else 0 end),2) as sum_amount_card_payment,
    sum(case when event_detail = "card_payment" then 1 else 0 end) as count_card_payment,
    round(sum(case when event_detail = "atm" then amount else 0 end),2) as sum_amount_card_atm,
    sum(case when event_detail = "card_atm" then 1 else 0 end) as count_atm,            
FROM  {{ ref("int_users_event") }}
GROUP BY user_id