# Github Archive Event Pipeline

## Problem description
With this project, I aim to pull in data from [GH Archive](https://www.gharchive.org/), transform and analyze them to get some information about the activity of public GitHub repositories over time. Some possible analytics question:
- Is there any pattern change in the activities before and after the Covid Pandemic? My assumption is that due to the long stay-at-home period, people may start learning new programming skills and will be more active on public repos.
- If there is such a change, what languages became more active?

## Solution
To answer those questions, this is the setup I used:
- Terraform to manage & config Google Cloud BigQuery datasets, and Cloud Storage buckets
- Google Cloud Storage as data lake, and BigQuery as the data warehouse
- Airflow to orchestrate data pipelining tasks
- dbt for transformation task
- Google Data Studio for visualization


## Caveats
- Due to time constraint, the current set up can only be run locally. 
- The amount of event data is too large, so loading three years' worth of data (2019 - 2022) will take too much time. For demonstration purpose, I only have a slice of 2019 data.
- There are too many schema changes between raw JSON files of different dates that make the final BigQuery upload task fails, and I have not had the time to investigate it yet.


## Instruction
- First, supply values for these variables in your environment:
    ```shell
    export GCS_BUCKET=your-bucket
    export GCP_PROJECT_ID=your-project-123
    ```
- Copy your Google credentials to `~/.google/credentials/google_credentials.json`
- Go to `1_terraform`, run `terraform apply`. Supply your own GCP Project ID. Once done, the following resources will be created:
    - BigQuery dataset: `src_github`
    - Cloud Storage bucket: `your-project-id_github_archive_data`
- Go to `2_airflow`, run `docker compose up` to start building & running the Airflow image. The image uses the environment variables mentioned above.
- When the initialization is done, go to Airflow UI and enable the DAG `github_event_ingestion` to start processing the data. The pipeline will create an `events` table inside the `src_github` dataset. You can also change the `start_date` and `end_date` params in the dag file to download data of the period you want. However, the full workflow currently only work with 2019 period.
- Next, Go to `3_dbt/github_event_transform`. Set up the dbt profile `github_events_transform` like in the sample `profiles.yml` file (specifying the path to credentials and your Google Cloud project.)
- Make sure you already have dbt version 1.0.0 and above. Run `dbt build`. This will create the following tables in your data warehouse
    - `fact_github_activities_daily`
    - `fact_language_activities_daily`
    - `github_events`
    - `github_repo_languages` 
- The two fact tables will be used to create visualizations.


## Demonstration dashboard
https://datastudio.google.com/reporting/0e1e3e18-d5be-4aea-a627-ba77de0b8cb3