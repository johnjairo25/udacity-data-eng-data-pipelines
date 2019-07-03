import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

dag = DAG(
    'create_tables',
    start_date=datetime.datetime.utcnow()
)

start_task = DummyOperator(task_id='begin_execution', dag=dag)

create_artist_task = PostgresOperator(
    task_id='create_artist_table',
    dag=dag,
    sql="""
    CREATE TABLE public.artists (
        artistid varchar(256) NOT NULL,
        name varchar(256),
        location varchar(256),
        lattitude numeric(18,0),
        longitude numeric(18,0)
    );
    """,
    postgres_conn_id="redshift"
)

create_songplays_task = PostgresOperator(
    task_id='create_songplays_table',
    dag=dag,
    sql="""
    CREATE TABLE public.songplays (
        playid varchar(32) NOT NULL,
        start_time timestamp NOT NULL,
        userid int4 NOT NULL,
        "level" varchar(256),
        songid varchar(256),
        artistid varchar(256),
        sessionid int4,
        location varchar(256),
        user_agent varchar(256),
        CONSTRAINT songplays_pkey PRIMARY KEY (playid)
    );
    """,
    postgres_conn_id="redshift"
)

create_songs_task = PostgresOperator(
    task_id='create_songs_table',
    dag=dag,
    sql="""
    CREATE TABLE public.songs (
        songid varchar(256) NOT NULL,
        title varchar(256),
        artistid varchar(256),
        "year" int4,
        duration numeric(18,0),
        CONSTRAINT songs_pkey PRIMARY KEY (songid)
    );
    """,
    postgres_conn_id="redshift"
)

create_staging_events_task = PostgresOperator(
    task_id='create_staging_events_table',
    dag=dag,
    sql="""
    CREATE TABLE public.staging_events (
        artist varchar(256),
        auth varchar(256),
        firstname varchar(256),
        gender varchar(256),
        iteminsession int4,
        lastname varchar(256),
        length numeric(18,0),
        "level" varchar(256),
        location varchar(256),
        "method" varchar(256),
        page varchar(256),
        registration numeric(18,0),
        sessionid int4,
        song varchar(256),
        status int4,
        ts int8,
        useragent varchar(256),
        userid int4
    );
    """,
    postgres_conn_id="redshift"
)

create_staging_songs_task = PostgresOperator(
    task_id='create_staging_songs_table',
    dag=dag,
    sql="""
    CREATE TABLE public.staging_songs (
        num_songs int4,
        artist_id varchar(256),
        artist_name varchar(256),
        artist_latitude numeric(18,0),
        artist_longitude numeric(18,0),
        artist_location varchar(256),
        song_id varchar(256),
        title varchar(256),
        duration numeric(18,0),
        "year" int4
    );
    """,
    postgres_conn_id="redshift"
)

create_time_task = PostgresOperator(
    task_id='create_time_table',
    dag=dag,
    sql="""
    CREATE TABLE public."time" (
        start_time timestamp NOT NULL,
        "hour" int4,
        "day" int4,
        week int4,
        "month" varchar(256),
        "year" int4,
        weekday varchar(256),
        CONSTRAINT time_pkey PRIMARY KEY (start_time)
    );
    """,
    postgres_conn_id="redshift"
)

create_users_task = PostgresOperator(
    task_id='create_users_table',
    dag=dag,
    sql="""
    CREATE TABLE public.users (
        userid int4 NOT NULL,
        first_name varchar(256),
        last_name varchar(256),
        gender varchar(256),
        "level" varchar(256),
        CONSTRAINT users_pkey PRIMARY KEY (userid)
    );
    """,
    postgres_conn_id="redshift"
)

end_task = DummyOperator(task_id='end_execution', dag=dag)


start_task >> create_artist_task
create_artist_task >> create_songplays_task
create_songplays_task >> create_songs_task
create_songs_task >> create_staging_events_task
create_staging_events_task >> create_staging_songs_task
create_staging_songs_task >> create_time_task
create_time_task >> create_users_task
create_users_task >> end_task

