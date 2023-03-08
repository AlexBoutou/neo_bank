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
    -- int_users_event_pivot
    ev.sms,
    ev.email,
    ev.push,
    round(SAFE_DIVIDE(ev.sum_amount_topup,li.lifetime_days),2) as avgday_amount_topup,
    ev.count_topup,
    round(SAFE_DIVIDE(ev.sum_amount_transfer,li.lifetime_days),2) as avgday_amount_transfer,
    ev.count_transfer,
    round(SAFE_DIVIDE(ev.sum_amount_tax,li.lifetime_days),2) as avgday_amount_tax,
    ev.count_tax,
    round(SAFE_DIVIDE(ev.sum_amount_refunds,li.lifetime_days),2) as avgday_amount_refunds,
    ev.count_refunds,
    round(SAFE_DIVIDE(ev.sum_amount_fee,li.lifetime_days),2) as avgday_amount_fee,
    ev.count_fee,
    round(SAFE_DIVIDE(ev.sum_amount_exchange,li.lifetime_days),2) as avgday_amount_exchange,
    ev.count_exchange,
    round(SAFE_DIVIDE(ev.sum_amount_cashback,li.lifetime_days),2) as avgday_amount_cashback,
    ev.count_cashback,
    round(SAFE_DIVIDE(ev.sum_amount_card_payment,li.lifetime_days),2) as avgday_amount_card_payment,
    ev.count_card_payment,
    round(SAFE_DIVIDE(ev.sum_amount_card_atm,li.lifetime_days),2) as avgday_amount_atm,
    ev.count_atm,
    -- int_lifetime_user
    li.account_creation_timestamp,
    li.last_transaction_timestamp,
    li.lifetime_days,
    li.final_sold,
    li.is_active
from {{ ref("stg_users") }} us
left join {{ ref("stg_devices") }} de on us.user_id=de.user_id
left join {{ ref("int_users_event_pivot") }} ev on us.user_id=ev.user_id
left join {{ ref("stg_country") }} co on  us.country_code=co.country_code
left join {{ ref("int_lifetime_user") }} li on  us.user_id=li.user_id