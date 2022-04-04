{{
  config(
    materialized = 'incremental'
    , incremental_strategy = 'merge'
    , partition_by = {
      "field": "report_date"
      , "data_type": "date"
      , "granularity": "day"
    }
  )
}}

select
  date(created_at) as report_date
  , languages.language_name
  , count(distinct repo_id) as active_repos_number
from {{ ref('github_events') }} events
left join {{ ref('github_repo_languages') }} languages on events.repo_name = languages.repo_name
where 
  languages.language_name is not null
{% if is_incremental() %}
  and date(events.created_at) >= coalesce((select max(date(created_at)) from {{ this }}), '1900-01-01')
{% endif %}

group by 1, 2