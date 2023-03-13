WITH user_cross_month AS (
    SELECT
        user_id,
        month_year
    FROM
        {{ ref('stg_users') }}
        CROSS JOIN {{ ref('int_event_months') }}
    ORDER BY
        1,
        2
)
SELECT
    user_id,
    month_year,
    is_active_month,
    FORMAT_DATETIME("%Y, %m",created_date) as created_date
FROM
    (
        SELECT
            um.user_id,
            um.month_year,
            CASE
                WHEN ut.user_id IS NOT NULL THEN 1
                ELSE 0
            END AS is_active_month,
            date_diff(parse_datetime("%Y, %m", um.month_year), EXTRACT(datetime FROM us.created_date), MONTH) AS diff_with_creation_month,
            us.created_date as created_date
        FROM user_cross_month um 
        LEFT JOIN {{ ref("int_users_with_transactions_month") }} ut ON um.user_id = ut.user_id AND um.month_year = ut.month_year
        LEFT JOIN {{ ref('stg_users') }} us ON um.user_id = us.user_id
        ORDER BY 1,2
    )
WHERE
    diff_with_creation_month >= 0
