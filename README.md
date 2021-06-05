# Data-Pipelines
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

Steps required for the ETL are shown below:
![DAG](example-dag.png "DAG")

## Installation
Run `install.sh`:
```
./install.sh
```

Modify the following variables in `airflow.cfg`:
```
task_runner = StandardTaskRunner
secret_key = N9baWahK5oABT+Jn1lR2Qg==
```

### Configure credentials
Go to [http://localhost:3000/admin/connection/](http://localhost:3000/admin/connection/)
and click Create.

#### S3
- **Conn Id:** `aws`
- **Conn Type:** `Amazon Web Services`
- **Login:** `<ACCESS_KEY>`
- **Password:** `<SECRET_ACCESS>`

#### Redshift
- **Conn Id:** `redshift`
- **Conn Type:** Postgres`
- **Host:** `<Endpoint>`
- **Schema:** `dev`
- **Login:** `awsuser`
- **Password:** `<Password>`
- **Port:** `5439`


## Start Airflow
Run `start.sh`
```
./start.sh
```

## Stop Airflow
Run `stop.sh`
```
./stop.sh
```

# Schema

- Fact Table: songplays
    - records in log data associated with song plays i.e. records with page NextSong
        - songplay_id
        - start_time
        - user_id
        - level
        - song_id
        - artist_id
        - session_id
        - location
        - user_agent

- Dimension Tables:
    - users - users in the app
        - user_id
        - first_name
        - last_name
        - gender
        - level
    - songs - songs in music database
        - song_id
        - title
        - artist_id
        - year
        - duration
    - artists - artists in music database
        - artist_id
        - name
        - location
        - latitude
        - longitude
    - time - timestamps of records in songplays broken down into specific units
        - start_time
        - hour
        - day
        - week
        - month
        - year
        - weekday
