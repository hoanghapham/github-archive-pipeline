# Github Event Transform

dbt project to transform GitHub event data. To run this project:
- Required: dbt version 1.0 and above
- Set up the profiles in your `profiles.yml` file. Reference: `sample_profiles/profiles.yml`
- Run command: 
    ```shell
    dbt build --select models/github
    ```