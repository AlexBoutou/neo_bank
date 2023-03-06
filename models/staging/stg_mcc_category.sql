SELECT 
    mcc_category.mcc_category as category
    , lower_boundary
    , upper_boundary
FROM raw_data.mcc_category