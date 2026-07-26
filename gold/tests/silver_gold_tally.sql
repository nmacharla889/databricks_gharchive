select
    (select sum(push_events) from {{ ref('push_events') }}) as mart_total,
    (select count(*) from {{ source('silver', 'push_events') }}) as silver_total
where (select sum(push_events) from {{ ref('push_events') }})
   != (select count(*) from {{ source('silver', 'push_events') }})