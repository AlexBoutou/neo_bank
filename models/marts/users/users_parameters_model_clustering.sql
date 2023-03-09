select
    CASE
        WHEN birth_year <= 2001 and birth_year >= 1990 then "18-29"
        WHEN birth_year <= 1988 and birth_year >= 1970 then "30-49"
        ELSE ">50"
    END as age_category
    user_settings_crypto_unlocked,
    attributes_notifications_marketing_push,
    attributes_notifications_marketing_email,
    device_category,
    sms,
    email,
    push,
    avgday_amount_topup,
    avgday_count_topup,
    avgday_amount_transfer,
    avgday_count_transfer,
    avgday_amount_card_payment,
    avgday_count_card_payment,
    avgday_amount_atm,
    avgday_count_atm, 
    --IFNULL(round(SAFE_DIVIDE(ev.sum_amount_tax,li.lifetime_days),3),0) as avgday_amount_tax,
    --IFNULL(round(SAFE_DIVIDE(ev.count_tax,li.lifetime_days),3),0) as avgday_count_tax,
    --IFNULL(round(SAFE_DIVIDE(ev.sum_amount_refunds,li.lifetime_days),3),0) as avgday_amount_refunds,
    --IFNULL(round(SAFE_DIVIDE(ev.count_refunds,li.lifetime_days),3),0) as avgday_count_refunds,
    --IFNULL(round(SAFE_DIVIDE(ev.sum_amount_fee,li.lifetime_days),3),0) as avgday_amount_fee,
    --IFNULL(round(SAFE_DIVIDE(ev.count_fee,li.lifetime_days),3),0) as avgday_count_fee,
    --IFNULL(round(SAFE_DIVIDE(ev.sum_amount_exchange,li.lifetime_days),3),0) as avgday_amount_exchange,
    --IFNULL(round(SAFE_DIVIDE(ev.count_exchange,li.lifetime_days),3),0) as avgday_count_exchange,
    --IFNULL(round(SAFE_DIVIDE(ev.sum_amount_cashback,li.lifetime_days),3),0) as avgday_amount_cashback,
    --IFNULL(round(SAFE_DIVIDE(ev.count_cashback,li.lifetime_days),3),0) as avgday_count_cashback,
    -- int_lifetime_user
    --li.account_creation_timestamp,
    --li.last_transaction_timestamp,
    li.lifetime_days,
    li.final_sold,
    li.is_active

from {{ ref("users_parameters_model_clustering") }}*
