select
    CASE
        WHEN birth_year <= 2001 and birth_year >= 1990 then "18-29"
        WHEN birth_year <= 1988 and birth_year >= 1970 then "30-49"
        ELSE ">50"
    END as age_category,
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
    lifetime_days,
    final_sold,
    is_active,
    users_churned_after_time,
    users_instant_churners
from {{ ref("users_parameters_model") }}

--crypto
