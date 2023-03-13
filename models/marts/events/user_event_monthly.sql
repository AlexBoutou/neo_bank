SELECT
    ev.user_id,
    DATE(EXTRACT(YEAR from ev.timestamp),EXTRACT(MONTH from ev.timestamp),1) as month,
    1+DATE_DIFF(DATE(ev.timestamp), DATE(us.created_date), MONTH) as month_nb,
    ev.event_type,
    ev.event_detail,
    count(*) as nb_event,
    ROUND(sum(ev.amount),2) as amount_sum
FROM {{ ref('int_user_event_with_sold') }} ev
LEFT JOIN {{ ref('stg_users') }} us using (user_id)
group by 1,2,3,4,5
order by 1,2,3