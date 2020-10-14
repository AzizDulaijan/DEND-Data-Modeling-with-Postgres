the read me file has:
how to run the python scripts 
explanation of the files in the repository 
comments are used effectively 
each function has a docstring 


## Files
### sql_queries.py 
this file contains the a list of SQL queries to create tables, insert into tables, and drop tables. 
#### create tables : 
create songplay table:
```sql
CREATE TABLE IF NOT EXISTS songplays 
(songplay_id VARCHAR, start_time NUMERIC, user_id VARCHAR, level VARCHAR, song_id VARCHAR, artist_id VARCHAR, session_id VARCHAR,  location VARCHAR, user_agent VARCHAR);
```
create the users table:
```sql
CREATE TABLE IF NOT EXISTS users 
(user_id VARCHAR, first_name VARCHAR, last_name VARCHAR, gender VARCHAR, level VARCHAR)
```
create the songs table:
```sql
CREATE TABLE IF NOT EXISTS songs 
(song_id VARCHAR, title VARCHAR, artist_id VARCHAR, year INT, duration NUMERIC)
```
create the artists table:
```sql
CREATE TABLE IF NOT EXISTS artists 
(artist_id VARCHAR, name VARCHAR, location VARCHAR, latitude NUMERIC, longitude NUMERIC)
```
create the time table:
```sql
CREATE TABLE IF NOT EXISTS time 
(start_time DATE, hour INT, day INT, week INT, month INT, year INT ,weekday int)
```
#### insert data queries:
insert into songplay table:
```sql
INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id,  location, user_agent) 
VALUES (%s, %s, %s, %s, %s, %s, %s,  %s, %s)
```
insert into users table:
```sql
INSERT INTO users (user_id, first_name, last_name, gender, level)
VALUES (%s, %s, %s, %s, %s)
```
insert into songs table:
```sql
INSERT INTO songs (song_id, title, artist_id, year, duration)
VALUES (%s, %s, %s, %s, %s)
```
insert into artists table:
```sql
INSERT INTO artists (artist_id, name, location, latitude, longitude)
VALUES (%s, %s, %s, %s, %s)
```
insert into time table:
```sql
INSERT INTO time (start_time, hour, day, week, month, year ,weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s)
```
Here is the  SELECT query to to find the `song_id` and `artist_id` where `title` from song table, `name` from artist table, and `duration` from song table match input. 
```sql
SELECT s.song_id, a.artist_id
FROM songs AS s
JOIN artists As a
ON s.artist_id = a.artist_id
WHERE  s.title = %s AND a.name = %s AND s.duration = %s
GROUP BY s.song_id, a.artist_id 
```

### create_table.py
This code creates a database , then runs queries from `sql_queries.py` file to create the database tables.

### etl.py 
this is the ETL pipeline code, it does Extract information from files, Transform file data to fit data base tables, and Load data into database. 
####  Processing song_data:
extracting the data from a JSON file , is done using pandas library. it's done by inserting data into a `DataFrame` which is a 2D data structure that helps with handling the data and transforming it.   
```python
df = pd.read_json(filepath, lines=True)
```
Getting information form the `DataFrame` for the song table: 
```python
    song_df = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    song_data = song_df.values
    song_data = song_data[0]
```
Then using the insert query from `sql_queries.py` to insert data into database:
```python
    try:
        cur.execute(song_table_insert, song_data)
    except psycopg2.Error as e:
        print("Error: Inserting Rows")
        print (e)
```
Getting information form the `DataFrame` for the artist table: 
```python
  artist_df = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
  artist_data = artist_df.values
  artist_data = artist_data[0]
```
Inserting data into Database:
```python
    try:
        cur.execute(artist_table_insert, artist_data)
    except psycopg2.Error as e:
        print("Error: Inserting Rows")
        print (e)
```
#### Processing log_data:
this also starts by opening a JSON file and loading it into a `DataFrame` structure using Pandas.
```python
  df = pd.read_json(filepath, lines=True)
```
For the time table data needs to be transformed in order to fit the table structure. first timestamp are extracted from the `DataFrame`. Then converted into a `datatime` type or  `dt` that has attributes that can help extract time infomation easily as can be seen in the line where the append is happening: 
```python
    # filter by NextSong action
    df = df[(df.page == 'NextSong')]

    # convert timestamp column to datetime
    t= df[['ts']] 
    
    #convert values from string from to int
    temp_list = list(map(int, t.values)) 
    
    #convert integers to datatime for easy access, using its attributes 
    time_ToDateTime = pd.to_datetime(temp_list, unit='ms')
    
    #creating the time_data records, using the datatime list 
    #goes through each timestamp to extract needed info,then add it to time_data. 
    time_data = []
    for i in time_ToDateTime:
        time_data.append([i, i.hour, i.day, i.week, i.month, i.year, i.dayofweek ])
    
    # insert time data records
    time_data = []
    for i in time_ToDateTime:
        time_data.append([i, i.hour, i.day, i.week, i.month, i.year, i.dayofweek ])
    column_labels = ('timestamp', 'hour', 'day', 'week of year', 'month', 'year', 'weekday')
    time_df = pd.concat([pd.DataFrame([i], columns= column_labels) for i in time_data], ignore_index=True)
```





to run the python scripts, first start a new command line luncher and type:

'''pytohn 
    run create_table.py
'''
this would create the database and its tables.
then to popilate the tables with information from the data files , run the etl python file:
    
    ''' run etl.py'''
    

