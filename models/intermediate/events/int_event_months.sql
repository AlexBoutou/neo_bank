SELECT
    DISTINCT FORMAT_DATETIME("%Y, %m", created_date) as month_year
FROM {{ ref('stg_transactions') }}
ORDER BY 1