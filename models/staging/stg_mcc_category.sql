SELECT 
    mcc_category.mcc_category as category
    , CAST(lower_boundary AS INT64) as lower_boundary
    , CAST(upper_boundary AS INT64) as upper_boundary
FROM raw_data.mcc_category