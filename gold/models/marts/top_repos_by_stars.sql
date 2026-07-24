with watch_events as (

    select
        repo_id,
        repo_name
    from {{ source('silver_dev', 'watch_events') }}

),

star_counts as (

    select
        repo_id,
        max(repo_name) as repo_name,
        count(*) as total_star_events
    from watch_events
    group by repo_id

)

select *
from star_counts
order by total_star_events desc