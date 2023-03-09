WITH nb_month_users AS (
    SELECT
        act.user_id,
        act.month_year,
        1+DATE_DIFF(PARSE_DATE("%Y, %m", act.month_year), PARSE_DATE("%Y, %m", ini.month_year), MONTH) as mn,
        ini.month_year
    FROM {{ ref('int_users_with_transactions_month') }} act
    LEFT JOIN {{ ref('int_acquisition_cohort_users') }} ini using(user_id)
    order by act.user_id,act.month_year
)
SELECT 
ini.month_year,
cs.cohort_size,
SUM(CASE 
    WHEN us.mn = 1 THEN 1
    ELSE 0 END) as one,
SUM(CASE 
    WHEN us.mn = 2 THEN 1
    ELSE 0 END) as two,
SUM(CASE 
    WHEN us.mn = 3 THEN 1
    ELSE 0 END) as three,
SUM(CASE 
    WHEN us.mn = 4 THEN 1
    ELSE 0 END) as four,
SUM(CASE 
    WHEN us.mn = 5 THEN 1
    ELSE 0 END) as five,
SUM(CASE 
    WHEN us.mn = 6 THEN 1
    ELSE 0 END) as six,
SUM(CASE 
    WHEN us.mn = 7 THEN 1
    ELSE 0 END) as seven,
SUM(CASE 
    WHEN us.mn = 8 THEN 1
    ELSE 0 END) as eight,
SUM(CASE 
    WHEN us.mn = 9 THEN 1
    ELSE 0 END) as nine,
SUM(CASE 
    WHEN us.mn = 10 THEN 1
    ELSE 0 END) as ten,
SUM(CASE 
    WHEN us.mn = 11 THEN 1
    ELSE 0 END) as eleven,
SUM(CASE 
    WHEN us.mn = 12 THEN 1
    ELSE 0 END) as twelve
FROM {{ ref('int_acquisition_cohort_users') }} ini 
LEFT JOIN {{ ref('int_acquisition_cohort_size') }} cs on ini.month_year = cs.month_year
LEFT JOIN nb_month_users us on ini.user_id = us.user_id
GROUP BY 1,2
order by month_year
