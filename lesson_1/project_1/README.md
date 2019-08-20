# DATA MODELLING WITH POSTGRES

This project covers the basics of data modelling. It introduces building an ETL pipeline to process data and store it into an organized format using python. The overall requirements of the project was to read JSON files extract the relevant data necessary to query based on songs and artists. The intention was to create data models using the data from the JSON files to support the queries required for analysis using postgres.

The following setup will help Sparkify understand the different attributes attached to the songs played by the user. For analytic purposes, for example, we can figure out what song has been played the most and which user is more active with location of the user and the medium the song gets played in. We can also find out when the song was played. More complex problems like which song has been played the most or which song is likely to be played more on Wednesday can also be figured out with the help of tools and this initial setup of data.

There are two different files in the project giving two diffrent sets of data.
1. `Song Files` under the directory song_data. The song files store information about artists and songs. The JSON data can be represented in the following table where the columns are the keys and values are in each row. Only the relevant information to the tables are shown below.

Data and View for `songs` table:

| song_id | title | artist_id | year | duration |
| ------- | ----- | --------- | ---- | -------- |
| SOUPIRU12A6D4FA1E1 | Der Kleine Dompfaff | ARJIE2Y1187B994AB7 | 0 | 152.92036 |

Data and View for `artists` table:

| artist_id | artist_name | artist_location | artist_latitude | artist_longitude |
| --------- | ----------- | --------------- | --------------- | ---------------- |
| ARJIE2Y1187B994AB7 | Line Renaud | some location | 0.14651 | 15.36 |

2. `Log Files` under the directory log_data. The log files store information about the user and time of play of the song. The JSON data can be represented in the following table where the columns are the keys and values are in each row. Only the relevant information to the tables are shown below.

Data and View for `time` table:

| start_time | hour | day | week | month | year | weekday |
| ---------- | ---- | --- | ---- | ----- | ---- | ------- |
| 2018-12-19 09:26:03.478039 | 14 | 12 | 24 | 6 | 1992 | 5 |

**This table used to breakdown the timestamp and store it for reference. The timestamp is captured from the log file and is processed to `datetime` format for easier read on when the song was played. It was initially in the `ms` format.** 

Data and View for `users` table:

| user_id | first_name | last_name | gender | level |
| ------- | ---------- | --------- | ------ | ----- |
| ARJIE2Y1187B994AB7 | Line | Renaud | male | free |

The `songplays` table is constructed using the joined data of `songs` and `artists` table combined with the `timestamp` and `user` data from the log file.
Format of `songplays` table:

| songplay_id | start_time | user_id |  level | song_id | artist_id | session_id | location | user_agent |
| ----------- | ---------- | ------- | ------ | ------- | --------- | ---------- | -------- | ---------- |
| ARJIE2Y1187B994AB7 | 2018-12-19 09:26:03.478039 | ARJIE2Y1187B994AB7 | free | ARJIE2Y1187B994AB7 | ARJIE2Y1187B994AB7 | 457 | some location | Mozilla.... 

**The `songplays` table helps with less duplication of data in the tables. For example, if we had to find which song was played in a certain day and also we have to find the artists of the song. we had to store the `datetime` value in both the `artists` and `songs` tables. Further on update we need to do two updates for one. It is essential to reduce duplication of code for faster queries and integrity of the data.**

## FILES AND FOLDERS IN THIS PROJECT
1. `data` - contains the JSON files which we read to extract data from
2. `etl.ipynb` - is the tutorial for creating an ETL pipeline
3. `test.ipynb` - is used to test if our data is loaded properly in postgres
4. `create_tables.py` - creates the database and the tables, wipes the db first before creating  
5. `etl.py` - reads and processes JSON data from data folder and stores them in the tables designed  
6. `sql_queries.py` - contains all the queries for populating the database and querying it

## RUNNING THE PROJECT
1. Run `create_tables.py` using the command `python create_tables.py`
2. Run `etl.py` using the command `python etl.py`