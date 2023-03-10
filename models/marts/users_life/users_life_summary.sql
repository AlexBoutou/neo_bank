WITH preprocess AS (
    SELECT
        user_id,
        MAX(1+COALESCE(DATE_DIFF(DATE(lf.last_transaction_timestamp),DATE(lf.account_creation_timestamp), MONTH),0)) as lifetime_month,
        SUM(
            CASE 
                WHEN is_active_month=1 THEN 1
                ELSE 0
            END) AS nb_active_period,
        SUM(
            CASE 
                WHEN is_active_month=0 THEN 1
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
    LEFT JOIN {{ ref('int_lifetime_user') }} lf using(user_id)
    WHERE is_last_period !=1 OR is_active_month=1
    group by 1)
SELECT 
    li.user_id
    , li.lifetime_month
    , li.nb_active_period
    , li.nb_inactive_period
    ,li.nb_inactive_month
    ,li.nb_active_month
    , ROUND(SAFE_DIVIDE(li.nb_inactive_month,li.lifetime_month),2) as inactive_ratio
    , ROUND(SAFE_DIVIDE(li.nb_active_month,li.lifetime_month),2) as active_ratio
    , us.is_active
FROM preprocess li
LEFT JOIN {{ ref('int_lifetime_user') }} us using(user_id)
ORDER BY 1