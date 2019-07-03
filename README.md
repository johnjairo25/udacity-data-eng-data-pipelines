# Data Pipelines Project with Airflow

A data pipeline demo project using Apache Airflow.


## Starting Apache Airflow with Docker

This project provides a `docker-compose.yml` file which allows you to run
Apache Airflow with Docker. The Docker Compose file sets up the folders 
for the _dags_ and the _plugins_. It also starts an instance of Postgres
that stores the metadata.

```bash
docker-compose up -d
```


