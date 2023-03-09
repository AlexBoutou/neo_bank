with creation_filtering AS (
    SELECT
        ac.user_id
        , ac.month_year
        , ac.is_active_month
        ,   FORMAT_TIMESTAMP( "%Y, %m",created_date) as created_date
        , DATE_DIFF(PARSE_DATETIME("%Y, %m", month_year),EXTRACT(DATETIME FROM us.created_date),MONTH) as diff_with_creation_month
    FROM {{ ref('int_users_activity_period') }} ac
    LEFT JOIN {{ ref('stg_users') }} us using (user_id)
    ORDER BY 1,2),
periods_calculation AS (
    SELECT
        user_id,
        month_year,
        is_active_month,
        ROW_NUMBER() OVER(PARTITION BY user_id, is_active_month ORDER BY month_year)-DATE_DIFF(PARSE_DATETIME("%Y, %m", month_year),PARSE_DATETIME("%Y, %m", created_date), MONTH) as nperiod,
    FROM creation_filtering
    WHERE diff_with_creation_month >=0
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
    END AS is_last_period
FROM periods_calculation
GROUP BY user_id,is_active_month,nperiod
ORDER BY 1,3