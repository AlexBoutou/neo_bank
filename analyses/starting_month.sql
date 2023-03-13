SELECT 
    EXTRACT(MONTH from created_date) as m,
    EXTRACT(YEAR from created_date) as y,
    count(*)
FROM {{ ref('stg_users') }}
group by 1,2
order by 2,1