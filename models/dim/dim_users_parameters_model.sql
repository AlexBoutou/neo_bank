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
left join {{ ref("int_users_event_pivot") }} ev on us.user_id=ev.user_id
left join {{ ref("stg_country") }} co on  us.country_code=co.country_code
left join {{ ref("int_lifetime_user") }} li on  us.user_id=li.user_id