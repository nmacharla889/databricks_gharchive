select *
from {{ ref('pr_issue_activity') }}
where prs_merged > pr_events