select
  repo_name
  , languages.name as language_name
from {{ source('github_repos', 'languages') }} as repo_languages
, unnest(language) as languages