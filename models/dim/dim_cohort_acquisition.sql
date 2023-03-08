WITH cohort_table AS
(SELECT
CASE
    WHEN EXTRACT ( MONTH FROM timestamp) = 1 THEN "janvier"
    WHEN EXTRACT ( MONTH FROM timestamp) = 2 THEN "février"
    WHEN EXTRACT ( MONTH FROM timestamp) = 3 THEN "mars"
    WHEN EXTRACT ( MONTH FROM timestamp) = 4 THEN "avril"
    WHEN EXTRACT ( MONTH FROM timestamp) = 5 THEN "mai"
    WHEN EXTRACT ( MONTH FROM timestamp) = 6 THEN "juin"
    WHEN EXTRACT ( MONTH FROM timestamp) = 7 THEN "juillet"
    WHEN EXTRACT ( MONTH FROM timestamp) = 8 THEN "aout"
    WHEN EXTRACT ( MONTH FROM timestamp) = 9 THEN "septembre"
    WHEN EXTRACT ( MONTH FROM timestamp) = 10 THEN "octobre"
    WHEN EXTRACT ( MONTH FROM timestamp) = 11 THEN "novembre"
    WHEN EXTRACT ( MONTH FROM timestamp) = 12 THEN "décembre"
END AS cohort
    , user_id
    , timestamp
    , event_type
    , event_detail
    , amount
    , sold
FROM {{ref("dim_user_event_with_sold")}}
WHERE event_type = "transaction")
SELECT
user_id
,timestamp
,COUNT(event_type)
FROM cohort_table
GROUP BY 1,2
