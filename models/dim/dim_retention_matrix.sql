WITH initial_month AS (
    SELECT
        user_id,
        FORMAT_DATETIME("%Y, %m", timestamp) as month_year,
    FROM {{ ref('dim_user_event_with_sold') }}
    WHERE event_type = "create_account"
),
cohort_size AS (
    SELECT
        month_year,
        count(user_id) as cohort_size
    FROM initial_month
    GROUP BY 1
),
active_month_users AS (
    SELECT
        user_id,
        FORMAT_DATETIME("%Y, %m", timestamp) as month_year
    FROM {{ ref('dim_user_event_with_sold') }}
    WHERE event_type="transaction"
    GROUP BY 1,2
),
nb_month_users AS (
    SELECT
        act.user_id,
        act.month_year,
        1+DATE_DIFF(PARSE_DATE("%Y, %m", act.month_year), PARSE_DATE("%Y, %m", ini.month_year), MONTH) as mn,
        ini.month_year
    FROM active_month_users act
    LEFT JOIN initial_month ini using(user_id)
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
FROM initial_month ini 
LEFT JOIN nb_month_users us on ini.user_id = us.user_id
LEFT JOIN cohort_size cs on ini.month_year = cs.month_year
GROUP BY 1,2
order by month_year
