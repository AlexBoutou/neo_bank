SELECT  
    user_id
    , birth_year
    , country as country_code
    , city
    , created_date
    , user_settings_crypto_unlocked
    , plan
    , COALESCE(attributes_notifications_marketing_push,0) AS attributes_notifications_marketing_push
    , COALESCE(attributes_notifications_marketing_email,0) AS attributes_notifications_marketing_email
    , num_contacts
--    , num_referrals
--    , num_successful_referrals
FROM raw_data.users