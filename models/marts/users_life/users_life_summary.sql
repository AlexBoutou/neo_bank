WITH preprocess AS (
    SELECT
        user_id,
        sum(nb_month) as lifetime_month,
        SUM(
            CASE 
                WHEN is_active_month=1 THEN 1
                ELSE 0
            END) AS nb_active_period,
        SUM(
            CASE 
                WHEN is_active_month=1 THEN 0
                ELSE 0
            END) AS nb_inactive_period,
        SUM(
            CASE 
                WHEN is_active_month=0 THEN nb_month
                ELSE 0
            END) AS nb_inactive_month,
        SUM(
            CASE 
                WHEN is_active_month=1 THEN nb_month
                ELSE 0
            END) AS nb_active_month,
    FROM {{ ref('int_users_life_split') }}
    group by 1)
SELECT 
    li.user_id
    , DATE_DIFF(DATE(lf.last_transaction_timestamp),DATE(lf.account_creation_timestamp), MONTH) as lifetime_month
    , li.nb_active_period
    , li.nb_inactive_period
    ,li.nb_inactive_month
    ,li.nb_active_month
    , ROUND(li.nb_inactive_month/li.lifetime_month,2) as inactive_ratio
    , ROUND(li.nb_active_month/li.lifetime_month,2) as active_ratio
    , us.is_active
FROM preprocess li
LEFT JOIN {{ ref('int_lifetime_user') }} us using(user_id)
LEFT JOIN {{ ref('int_lifetime_user') }} lf on li.user_id = lf.user_id