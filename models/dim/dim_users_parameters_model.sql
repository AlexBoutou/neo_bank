with
    pivot_int_events_models as (
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
    )
select
    --stg_users
    us.user_id,
    us.birth_year,
    us.country_code,
    --stg_country
    co.country_name,
    --stg_users
    us.city,
    us.created_date,
    us.user_settings_crypto_unlocked,
    us.plan,
    us.attributes_notifications_marketing_push,
    us.attributes_notifications_marketing_email,
    us.num_contacts,
    -- stg_devices
    de.device_category,
    -- pivot_int_events_models
    ev.sms,
    ev.email,
    ev.push,
    ev.avg_amount_topup,
    ev.count_topup,
    ev.avg_amount_transfer,
    ev.count_transfer,
    ev.avg_amount_tax,
    ev.count_tax,
    ev.avg_amount_refunds,
    ev.count_refunds,
    ev.avg_amount_fee,
    ev.count_fee,
    ev.avg_amount_exchange,
    ev.count_exchange,
    ev.avg_amount_cashback,
    ev.count_cashback,
    ev.avg_amount_card_payment,
    ev.count_card_payment,
    ev.avg_amount_card_atm,
    ev.count_atm,
    -- int_lifetime_user
    li.account_creation_timestamp,
    li.last_transaction_timestamp,
    li.lifetime_days,
    li.final_sold
from {{ ref("stg_users") }} us
left join {{ ref("stg_devices") }} de on us.user_id=de.user_id
left join pivot_int_events_models ev on us.user_id=ev.user_id
left join {{ ref("stg_country") }} co on  us.country_code=co.country_code
left join {{ ref("int_lifetime_user") }} li on  us.user_id=li.user_id