{{ config(materialized='table') }}

with pr_events as (

    select
        repo_id,
        repo_name,
        event_date,
        pr_merged
    from {{ source('silver_dev', 'pull_request_events') }}

),

issue_events as (

    select
        repo_id,
        repo_name,
        event_date
    from {{ source('silver_dev', 'issues_events') }}

),

comment_events as (

    select
        repo_id,
        repo_name,
        event_date
    from {{ source('silver_dev', 'issue_comment_events') }}

),

combined as (

    select repo_id, repo_name, event_date, 'pr' as source_type, pr_merged
    from pr_events

    union all

    select repo_id, repo_name, event_date, 'issue' as source_type, cast(null as boolean) as pr_merged
    from issue_events

    union all

    select repo_id, repo_name, event_date, 'comment' as source_type, cast(null as boolean) as pr_merged
    from comment_events

),

daily_activity as (

    select
        repo_id,
        max(repo_name) as repo_name,
        event_date,
        count(case when source_type = 'pr' then 1 end) as pr_events,
        count(case when source_type = 'pr' and pr_merged = true then 1 end) as prs_merged,
        count(case when source_type = 'issue' then 1 end) as issue_events,
        count(case when source_type = 'comment' then 1 end) as comment_events
    from combined
    group by repo_id, event_date

)

select *
from daily_activity
order by event_date, repo_id