with push_events as (

    select
        repo_id,
        repo_name,
        event_date,
        push_size
    from {{ source('silver_dev', 'push_events') }}

),

daily_pushes as (

    select
        repo_id,
        max(repo_name) as repo_name,
        event_date,
        count(*) as push_events,
        sum(push_size) as commits_pushed
    from push_events
    group by repo_id, event_date

)

select *
from daily_pushes
order by event_date, commits_pushed desc