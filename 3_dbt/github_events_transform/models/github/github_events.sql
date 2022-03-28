{{
  config(
    materialized = 'incremental'
    , incremental_strategy = 'merge'
    , partition_by = {
      "field": "created_at"
      , "data_type": "timestamp"
      , "granularity": "day"
    }
  )
}}
select
  id
  , created_at
  , type
  , repo.name as repo_name
  , repo.id as repo_id
  , org.login as org_login
  , org.id as org_id
  , actor.login as actor_login
  , actor.id as actor_id
  
from {{ source('src_github', 'events') }} events

where 
  events.type in ('PushEvent', 'CreateEvent')
{% if is_incremental() %}
  and date(events.created_at) >= coalesce((select max(date(created_at)) from {{ this }}), '1900-01-01')
{% endif %}
