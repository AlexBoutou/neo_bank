SELECT
    month_year
    , cohort_size
    , ROUND(one/cohort_size,2) as one
    , ROUND(two/cohort_size,2) as two
    , ROUND(three/cohort_size,2) as three
    , ROUND(four/cohort_size,2) as four
    , ROUND(five/cohort_size,2) as five
    , ROUND(six/cohort_size,2) as six
    , ROUND(seven/cohort_size,2) as seven
    , ROUND(eight/cohort_size,2) as eight
    , ROUND(nine/cohort_size,2) as nine
    , ROUND(ten/cohort_size,2) as ten
    , ROUND(eleven/cohort_size,2) as eleven
    , ROUND(twelve/cohort_size,2) as twelve
FROM {{ ref('dim_retention_matrix') }}