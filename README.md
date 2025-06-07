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
- The current setup can only be run locally (setting up & run Cloud Composer is quite costly)
- The amount of event data is too large, so loading three years' worth of data (2019 - 2022) will take too much time. For demonstration purpose, I only have a slice of 2019-2020 data.

## Instruction
### Set up local environment
First, supply values for these variables in your environment:
```shell
export GCS_BUCKET=your-bucket
export GCP_PROJECT_ID=your-project-123
```
Copy your Google credentials to `~/.google/credentials/google_credentials.json`. 

These environment variables and Google credentials will be used throughout the project.

### Terraform

Go to `s1_terraform` and run
- `terraform init` to initiate Terraform in the current folder
- `terraform apply` to create and track the defined resources
You will need to supply your own GCP Project ID. Once done, the following resources will be created:
    - BigQuery dataset: `src_github`
    - Cloud Storage bucket: `your-project-id_github_archive_data`

### Airflow
Go to `s2_airflow`, run `docker compose up` to start building & running the Airflow image. When the initialization is done, go to Airflow UI and enable the DAG `github_event_ingestion_v2` to start processing the data. You can also change the `start_date` and `end_date` params in the dag file to download data of the period you want.

The pipeline will do the following things:
- Download GitHub events from https://data.gharchive.org, and extract the downloaded `.gz` files into `.json` files.
- Scan through the JSON files, and pick out only `CreateEvent` records. 
- Package those records into a compressed `.gz` file, and upload to GCS
- Create a `create_events` table using those files.

### dbt

- Next, Go to `s3_dbt/github_event_transform`. Set up the dbt profile `github_events_transform` like in the sample `profiles.yml` file (specifying the path to credentials and your Google Cloud project.)
- Make sure you already have dbt version 1.0.0 and above. Run `dbt build`. This will create the following tables in your data warehouse
    - `fact_github_activities_daily`
    - `fact_language_activities_daily`
    - `github_events`
    - `github_repo_languages` 

The two fact tables will be used to create visualizations.
