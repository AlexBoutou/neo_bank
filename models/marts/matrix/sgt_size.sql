SELECT
    sgt,
    count(*) as sgt_size
FROM {{ ref('users_segment') }}
group by 1