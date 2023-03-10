select
    --stg_users
    us.user_id,
    us.birth_year,
    --stg_country
    co.country_name,
    --stg_users
    us.city,
    us.created_date,
    us.user_settings_crypto_unlocked,
    -- users_parameters_model_crypto
    CASE
        WHEN cr.user_id = us.user_id THEN 1
        ELSE 0
    END AS crypto_user,
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
    li.is_active,
    CASE
        WHEN us.user_id=ch.user_id THEN 1
        ELSE 0
    END as users_churned_after_time,
    CASE
        WHEN us.user_id=it.user_id THEN 1
        ELSE 0
    END as users_instant_churners


from {{ ref("stg_users") }} us
left join {{ ref("stg_devices") }} de on us.user_id=de.user_id
left join {{ ref("int_users_event_pivot") }} ev on us.user_id=ev.user_id
left join {{ ref("stg_country") }} co on  us.country_code=co.country_code
left join {{ ref("int_lifetime_user") }} li on  us.user_id=li.user_id
left join {{ ref("int_churners") }} ch on  us.user_id=ch.user_id
left join {{ ref("int_instant_churners") }} it on  us.user_id=it.user_id
left join {{ ref ("users_parameters_model_crypto")}} cr on us.user_id=cr.user_id