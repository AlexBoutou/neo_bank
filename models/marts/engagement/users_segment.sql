SELECT
    user_id
    , CASE 
        WHEN high_active_ratio = 1 AND high_monthly_transac=1 THEN "Dynamic Reccurent"
        WHEN high_active_ratio = 1 AND high_monthly_transac=0 THEN "Passive Reccurent"
        WHEN high_active_ratio = 0 AND high_monthly_transac=1 THEN "Dynamic Ponctual"
        WHEN high_active_ratio = 0 AND high_monthly_transac=0 THEN "Passive Ponctual"
    END as sgt
FROM {{ ref('users_metrics') }}
