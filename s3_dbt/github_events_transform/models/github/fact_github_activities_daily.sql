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
  , count(distinct repo_id) as active_repos_number
  , count(case when type = 'CreateEvent' then 1 else null end) as create_events_number
from {{ ref('github_events') }}
where true
{% if is_incremental() %}
  and date(created_at) >= coalesce((select max(date(created_at)) from {{ this }}), '1900-01-01')
{% endif %}

group by 1
