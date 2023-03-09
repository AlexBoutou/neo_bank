SELECT
    DISTINCT FORMAT_DATETIME("%Y, %m", timestamp) as month_year
FROM {{ ref('int_users_event') }}
ORDER BY 1