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

with stg_events as (
    select
      id
      , created_at
      , type
      , repo
      , org
      , actor
    from {{ source('src_github', 'create_events') }}
    where true
    {% if is_incremental() %}
      and date(created_at) >= coalesce((select max(date(created_at)) from {{ this }}), '1900-01-01')
    {% endif %}

    union all 

    select
      id
      , created_at
      , type
      , repo
      , org
      , actor
    from {{ source('src_github', 'push_events') }}
    where true
    {% if is_incremental() %}
      and date(created_at) >= coalesce((select max(date(created_at)) from {{ this }}), '1900-01-01')
    {% endif %}
)

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
from stg_events