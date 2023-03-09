SELECT
    month_year,
    count(user_id) as cohort_size
FROM {{ ref('int_acquisition_cohort_users') }}
GROUP BY 1
ORDER BY 1
