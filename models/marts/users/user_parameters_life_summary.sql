SELECT
-- users_parameters_model    
    pa.user_id,
    pa.birth_year,
    pa.age_category,
    pa.country_name,
    pa.city,
    pa.created_date,
    pa.user_settings_crypto_unlocked,
    pa.crypto_user,
    pa.plan,
    pa.attributes_notifications_marketing_push,
    pa.attributes_notifications_marketing_email,
    pa.num_contacts,
    pa.device_category,
    pa.sms,
    pa.email,
    pa.push,
-- int_users_event_pivot + int_lifetime_user 
    IFNULL(round(SAFE_DIVIDE(ev.sum_amount_topup,li.lifetime_month),2),0) as avgmonth_amount_topup,
    IFNULL(round(SAFE_DIVIDE(ev.count_topup,li.lifetime_month),2),0) as avgmonth_count_topup,
    IFNULL(round(SAFE_DIVIDE(ev.sum_amount_transfer,li.lifetime_month),2),0) as avgmonth_amount_transfer,
    IFNULL(round(SAFE_DIVIDE(ev.count_transfer,li.lifetime_month),2),0) as avgmonth_count_transfer,
    IFNULL(round(SAFE_DIVIDE(ev.sum_amount_tax,li.lifetime_month),2),0) as avgmonth_amount_tax,
    IFNULL(round(SAFE_DIVIDE(ev.count_tax,li.lifetime_month),2),0) as avgmonth_count_tax,
    IFNULL(round(SAFE_DIVIDE(ev.sum_amount_refunds,li.lifetime_month),2),0) as avgmonth_amount_refunds,
    IFNULL(round(SAFE_DIVIDE(ev.count_refunds,li.lifetime_month),2),0) as avgmonth_count_refunds,
    IFNULL(round(SAFE_DIVIDE(ev.sum_amount_fee,li.lifetime_month),2),0) as avgmonth_amount_fee,
    IFNULL(round(SAFE_DIVIDE(ev.count_fee,li.lifetime_month),2),0) as avgmonth_count_fee,
    IFNULL(round(SAFE_DIVIDE(ev.sum_amount_exchange,li.lifetime_month),2),0) as avgmonth_amount_exchange,
    IFNULL(round(SAFE_DIVIDE(ev.count_exchange,li.lifetime_month),2),0) as avgmonth_count_exchange,
    IFNULL(round(SAFE_DIVIDE(ev.sum_amount_cashback,li.lifetime_month),2),0) as avgmonth_amount_cashback,
    IFNULL(round(SAFE_DIVIDE(ev.count_cashback,li.lifetime_month),2),0) as avgmonth_count_cashback,
    IFNULL(round(SAFE_DIVIDE(ev.sum_amount_card_payment,li.lifetime_month),2),0) as avgmonth_amount_card_payment,
    IFNULL(round(SAFE_DIVIDE(ev.count_card_payment,li.lifetime_month),2),0) as avgmonth_count_card_payment,
    IFNULL(round(SAFE_DIVIDE(ev.sum_amount_card_atm,li.lifetime_month),2),0) as avgmonth_amount_atm,
    IFNULL(round(SAFE_DIVIDE(ev.count_atm,li.lifetime_month),2),0) as avgmonth_count_atm, 
-- users_parameters_model    
    pa.final_sold,
-- int_lifetime_user
    li.lifetime_month,
    li.nb_active_period,
    li.nb_inactive_period,
    li.nb_inactive_month,
    li.nb_active_month,
    li.inactive_ratio,
    li.active_ratio,
    li.is_active,
    CASE 
        WHEN pa.user_id=it.user_id then 1
        ELSE 0
    END as instant_churner
FROM {{ ref ("users_parameters_model")}} pa
LEFT JOIN {{ ref("int_users_event_pivot") }} ev on pa.user_id=ev.user_id
LEFT JOIN {{ ref ("users_life_summary")}} li on pa.user_id=li.user_id
LEFT JOIN {{ ref ("int_instant_churners")}} it on pa.user_id=it.user_id