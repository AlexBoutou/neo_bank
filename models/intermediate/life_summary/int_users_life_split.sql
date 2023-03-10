WITH periods_calculation AS (
    SELECT
        user_id,
        month_year,
        is_active_month,
        ROW_NUMBER() OVER(PARTITION BY user_id, is_active_month ORDER BY month_year)-DATE_DIFF(PARSE_DATETIME("%Y, %m", month_year),PARSE_DATETIME("%Y, %m", created_date), MONTH) as nperiod,
    FROM {{ ref('int_users_activity_period') }}
    ORDER BY 1,2)
SELECT 
    user_id,
    is_active_month,
    min(month_year) as starting_month,
    max(month_year) as ending_month,
    1+DATE_DIFF(PARSE_DATETIME("%Y, %m",max(month_year)),PARSE_DATETIME("%Y, %m",min(month_year)),MONTH) as nb_month,
    CASE 
        WHEN max(month_year) = "2019, 05" THEN 1
        ELSE 0
    END AS is_last_period,
    ROW_NUMBER() OVER(PARTITION BY user_id, is_active_month ORDER BY min(month_year)) AS nth_period
FROM periods_calculation
GROUP BY user_id,is_active_month,nperiod
ORDER BY 1,3