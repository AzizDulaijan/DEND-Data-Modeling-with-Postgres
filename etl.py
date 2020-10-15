import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    
    """ETL pipeline for song table, and artist table"""
    
    # open song file
    df = pd.read_json(filepath, lines=True)
    
    # insert song record
    song_df = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    song_data = song_df.values
    song_data = song_data[0]
    
    try:
        cur.execute(song_table_insert, song_data)
    except psycopg2.Error as e:
        print("Error: Inserting Rows")
        print (e)
        
    # insert artist record
    artist_df = df[['artist_id', 'artist_name', 'artist_location'
                    , 'artist_latitude', 'artist_longitude']]
    artist_data = artist_df.values
    artist_data = artist_data[0]
    
    try:
        cur.execute(artist_table_insert, artist_data)
    except psycopg2.Error as e:
        print("Error: Inserting Rows")
        print (e)

def process_log_file(cur, filepath):
    
    """ETL pipeline for time table, and songplay table"""
    
    # open log file
    df = pd.read_json(filepath, lines=True)

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
    column_labels = ('timestamp', 'hour', 'day', 'week of year', 
                     'month', 'year', 'weekday')
    time_df = pd.concat([pd.DataFrame([i], columns= column_labels) \
                         for i in time_data], ignore_index=True)

    
    for i, row in time_df.iterrows():
        try: 
            cur.execute(time_table_insert, list(row))
        except psycopg2.Error as e:
            print("Error: Inserting Rows")
            print (e)

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        try:
            cur.execute(song_select, (row.song, row.artist, row.length))
        except psycopg2.Error as e:
            print("Error: Selecting Rows")
            print (e)
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = [row.ts ,row.userId, row.level, songid , artistid, 
                         row.sessionId, row.location, row.userAgent]
        try:
            cur.execute(songplay_table_insert, songplay_data)
        except psycopg2.Error as e:
            print("Error: Inserting Rows")
            print (e)

def process_data(cur, conn, filepath, func):
    
    """takes an file directory, find and proccess each JSON file in it"""
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    
    """ this is the main function it connects to db and start the ETL"""
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()