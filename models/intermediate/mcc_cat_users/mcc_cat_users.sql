with mcc_join AS (
    SELECT
        user_id,
        category,
        count(*) as nb
    FROM {{ ref('stg_transactions') }} tr
    LEFT JOIN {{ ref('stg_mcc_category') }} mc on tr.merchant_mcc between mc.lower_boundary and mc.upper_boundary
    WHERE transaction_state ="COMPLETED" and merchant_mcc is not null
    group by 1,2)
SELECT 
    user_id,
    max(first_category) as first_category,
    max(second_category) as second_category,
    max(third_category) as third_category
FROM (
    SELECT 
        user_id,
        FIRST_VALUE(category) OVER(PARTITION BY user_id ORDER BY nb) as first_category,
        NTH_VALUE(category,2) OVER(PARTITION BY user_id ORDER BY nb) second_category,
        NTH_VALUE(category,3) OVER(PARTITION BY user_id ORDER BY nb) third_category
    FROM mcc_join)
GROUP BY user_id
