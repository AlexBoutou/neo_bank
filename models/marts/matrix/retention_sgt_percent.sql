SELECT
    sgt
    , sgt_size
    , ROUND(first_month/sgt_size,2) as first_month
    , ROUND(three_months/sgt_size,2) as three_months
    , ROUND(five_months/sgt_size,2) as five_months
    , ROUND(seven_months/sgt_size,2) as seven_months
FROM {{ ref('retention_sgt') }}
order by 1