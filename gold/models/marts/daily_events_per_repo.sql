with bronze_events as (

    select
        repo_id,
        repo_name,
        event_date
    from {{ source('bronze_dev', 'events') }}
),

daily_counts as (

    select
        repo_id,
        max(repo_name) as repo_name,
        event_date,
        count(*) as event_count
    from bronze_events
    group by repo_id, event_date

)

select *
from daily_counts
order by event_date, event_count desc