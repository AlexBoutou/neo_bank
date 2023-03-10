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
    IFNULL(round(SAFE_DIVIDE(ev.sum_amount_topup,li.lifetime_days),3),0) as avgday_amount_topup,
    IFNULL(round(SAFE_DIVIDE(ev.count_topup,li.lifetime_days),3),0) as avgday_count_topup,
    IFNULL(round(SAFE_DIVIDE(ev.sum_amount_transfer,li.lifetime_days),3),0) as avgday_amount_transfer,
    IFNULL(round(SAFE_DIVIDE(ev.count_transfer,li.lifetime_days),3),0) as avgday_count_transfer,
    IFNULL(round(SAFE_DIVIDE(ev.sum_amount_tax,li.lifetime_days),3),0) as avgday_amount_tax,
    IFNULL(round(SAFE_DIVIDE(ev.count_tax,li.lifetime_days),3),0) as avgday_count_tax,
    IFNULL(round(SAFE_DIVIDE(ev.sum_amount_refunds,li.lifetime_days),3),0) as avgday_amount_refunds,
    IFNULL(round(SAFE_DIVIDE(ev.count_refunds,li.lifetime_days),3),0) as avgday_count_refunds,
    IFNULL(round(SAFE_DIVIDE(ev.sum_amount_fee,li.lifetime_days),3),0) as avgday_amount_fee,
    IFNULL(round(SAFE_DIVIDE(ev.count_fee,li.lifetime_days),3),0) as avgday_count_fee,
    IFNULL(round(SAFE_DIVIDE(ev.sum_amount_exchange,li.lifetime_days),3),0) as avgday_amount_exchange,
    IFNULL(round(SAFE_DIVIDE(ev.count_exchange,li.lifetime_days),3),0) as avgday_count_exchange,
    IFNULL(round(SAFE_DIVIDE(ev.sum_amount_cashback,li.lifetime_days),3),0) as avgday_amount_cashback,
    IFNULL(round(SAFE_DIVIDE(ev.count_cashback,li.lifetime_days),3),0) as avgday_count_cashback,
    IFNULL(round(SAFE_DIVIDE(ev.sum_amount_card_payment,li.lifetime_days),3),0) as avgday_amount_card_payment,
    IFNULL(round(SAFE_DIVIDE(ev.count_card_payment,li.lifetime_days),3),0) as avgday_count_card_payment,
    IFNULL(round(SAFE_DIVIDE(ev.sum_amount_card_atm,li.lifetime_days),3),0) as avgday_amount_atm,
    IFNULL(round(SAFE_DIVIDE(ev.count_atm,li.lifetime_days),3),0) as avgday_count_atm,    
    -- int_lifetime_user
    li.account_creation_timestamp,
    li.last_transaction_timestamp,
    li.lifetime_days,
    li.final_sold,
    li.is_active

FROM {{ ref("stg_users") }} us
LEFT JOIN {{ ref("int_users_event_pivot_crypto") }} ev 
    ON us.user_id = ev.user_id
LEFT JOIN {{ ref("stg_devices") }} de 
    ON ev.user_id = de.user_id
LEFT JOIN {{ ref("stg_country") }} co 
    ON us.country_code = co.country_code
LEFT JOIN {{ ref("int_lifetime_user_crypto") }} li 
    ON ev.user_id = li.user_id
WHERE ev.user_id IS NOT NULL