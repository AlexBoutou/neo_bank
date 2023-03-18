WITH nb_month_users AS (
    SELECT
        act.user_id,
        act.month_year,
        1+DATE_DIFF(PARSE_DATE("%Y, %m", act.month_year), PARSE_DATE("%Y, %m", ini.month_year), MONTH) as mn,
        ini.month_year,
        ug.sgt
    FROM {{ ref('int_users_with_transactions_month') }} act
    LEFT JOIN {{ ref('int_acquisition_cohort_users') }} ini using(user_id)
    LEFT JOIN {{ ref('users_segment') }} ug on act.user_id = ug.user_id
    order by act.user_id,act.month_year
),
keeping_three_month AS (
    SELECT
        user_id
        , sgt
        , CASE 
            WHEN MN =1 THEN "First month"
            WHEN MN <=3 THEN "Three months"
            WHEN MN <=5 THEN "Five months"
            WHEN MN <=7 THEN "Seven months"
        END as mn
    FROM nb_month_users
),
remove_dup AS (
    SELECT
        user_id,
        sgt,
        mn
    FROM keeping_three_month
    where mn is not null
    group by 1,2,3 
)
SELECT 
us.sgt,
sg.sgt_size,
SUM(CASE 
    WHEN us.mn = "First month" THEN 1
    ELSE 0 END) as first_month,
SUM(CASE 
    WHEN us.mn = "Three months" THEN 1
    ELSE 0 END) as three_months,
SUM(CASE 
    WHEN us.mn = "Five months" THEN 1
    ELSE 0 END) as five_months,
SUM(CASE 
    WHEN us.mn = "Seven months" THEN 1
    ELSE 0 END) as seven_months,
FROM {{ ref('int_acquisition_cohort_users') }} ini 
LEFT JOIN remove_dup us on ini.user_id = us.user_id
LEFT JOIN {{ ref('sgt_size') }} sg on us.sgt = sg.sgt
WHERE us.sgt is not null
GROUP BY 1,2
