SELECT
    ev.user_id,
    DATE(EXTRACT(YEAR from ev.timestamp),EXTRACT(MONTH from ev.timestamp),1) as month,
    ev.event_type,
    ev.event_detail,
    count(*) as nb_event,
    ROUND(sum(ev.amount),2) as amount_sum
FROM {{ ref('int_user_event_with_sold') }} ev
group by 1,2,3,4
order by 1,2,3