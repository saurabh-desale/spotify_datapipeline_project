//create db
create or replace database spotify_db2;

//create schema
create or replace schema spotify_schema2;

//create storage integration
create or replace storage integration spotify_init
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE 
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::058264376964:role/snowflake_developer'
  STORAGE_ALLOWED_LOCATIONS = ('s3://spotify-api-data-project/transformed_data/')
   COMMENT = 'Creating connection to S3';

 
//describe init
desc storage integration spotify_init;

//create file format
create or replace file format spotify_db2.spotify_schema2.csv_spotify2
    type = csv
    FIELD_OPTIONALLY_ENCLOSED_BY='"'
    skip_header = 1;

//create one stage    
CREATE OR REPLACE stage spotify_db2.spotify_schema2.spotify_stg
    URL = 's3://spotify-api-data-project/transformed_data/'
    STORAGE_INTEGRATION = spotify_init
    FILE_FORMAT = spotify_db2.spotify_schema2.csv_spotify2;    

//list down stage
LIST @spotify_db2.spotify_schema2.spotify_stg;    

//create tables
CREATE  or replace table spotify_db2.spotify_schema2.album_tbl (
    album_id STRING,
    name STRING,
    release_date STRING,
    total_tracks INTEGER,
    url STRING
);

CREATE  or replace  TABLE spotify_db2.spotify_schema2.songs_tbl (
    song_id STRING,
    song_name STRING,
    duration_ms INT,
    url STRING,
    popularity INT,
    song_added TIMESTAMP_NTZ,
    album_id STRING,
    artist_id STRING
);

create or replace  TABLE spotify_db2.spotify_schema2.artist_tbl (
    artist_id STRING,
    artist_name STRING,
    external_url STRING
);

//load data in tbl from stage
copy into spotify_db2.spotify_schema2.album_tbl from
@spotify_db2.spotify_schema2.spotify_stg/album_data;

copy into spotify_db2.spotify_schema2.songs_tbl from
@spotify_db2.spotify_schema2.spotify_stg/songs_data;

copy into spotify_db2.spotify_schema2.artist_tbl from
@spotify_db2.spotify_schema2.spotify_stg/artist_data;

//fetch data from tbls
select * from spotify_db2.spotify_schema2.album_tbl;
select * from spotify_db2.spotify_schema2.songs_tbl;
select * from spotify_db2.spotify_schema2.artist_tbl;

// Create schema to keep things organized
CREATE OR REPLACE SCHEMA spotify_db2.pipes;

// Define pipe
CREATE OR REPLACE pipe spotify_db2.pipes.spotify_pipe
auto_ingest = TRUE
AS
copy into spotify_db2.spotify_schema2.album_tbl from
@spotify_db2.spotify_schema2.spotify_stg/album_data; 

CREATE OR REPLACE pipe spotify_db2.pipes.spotify_songs_pipe
auto_ingest = TRUE
AS
copy into spotify_db2.spotify_schema2.songs_tbl from
@spotify_db2.spotify_schema2.spotify_stg/songs_data;

CREATE OR REPLACE pipe spotify_db2.pipes.spotify_artist_pipe
auto_ingest = TRUE
AS
copy into spotify_db2.spotify_schema2.artist_tbl from
@spotify_db2.spotify_schema2.spotify_stg/artist_data;

// Describe pipe get the notification_channel and copy it for S3 event 
DESC pipe spotify_db2.pipes.spotify_pipe;


show pipes;
SHOW PIPES like '%spotify%'    
    