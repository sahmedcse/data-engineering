import os
import glob
import psycopg2
import pandas as pd
import uuid
from sql_queries import *


def process_song_file(cur, filepath):
    """ 
    Processes a song file to enter data in the postgres table
    
    A song file is a JSON file where each object consists of data related to the artist and the song itself.
    The song file is convereted to a DataFrame and parsed to store relevant information. 
    The information relevant to song is stored in the songs table. E.g: song_id, title, artist_id, year, duration
    The information relevant to the artist of the song is stored in the artists table. E.g: artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    
    Parameters:
    cur (cursor): Connection cursor of the database
    filepath (string): Filepath to the JSON file
    
    Returns:
    NULL
 
    """
    # open song file
    df = pd.read_json(filepath, typ="series")

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values
    song_data = song_data.tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values
    artist_data = artist_data.tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Processes logs files to store information in the relevant tables
    
    Reads the log JSON file and formats data for easier storage after the data got converted to a DataFrame.
    Useful information regarding time of play of the song, users and songplay is stored in their respectives tables (time, users, songplay)
    The songplay data combines artists and songs in the same table
    
    Parameters:
    cur (cursor): Connection cursor of the database
    filepath (string): Filepath to the JSON file
    
    Returns:
    NULL
    
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.where(df["page"] == "NextSong").dropna(subset=["page"])

    # convert timestamp column to datetime
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")
    t = df
    
    # insert time data records
    time_data = list((t["ts"].values, t["ts"].dt.hour.values, t["ts"].dt.day.values, t["ts"].dt.weekofyear.values, t["ts"].dt.month.values, t["ts"].dt.year.values, t["ts"].dt.weekday.values))
    column_labels = list(("timestamp", "hour", "day", "weekofyear", "month", "year", "weekday"))
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)), columns = column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        if(songid != None) and (artistid != None):
            songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
            cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Processes all of the JSON files in a folder
    
    Finds all the files with the .json extension in a folder specified.
    For each of the files found in the folder delegates process of its data to the correct callback.
    After processing the data commits the transaction in the database to update and store the new state of the tables.
    Prints status of the files to be read and the status of the files that are processed.
    
    Parameters:
    cur (cursor): Connection cursor of the database
    connection (connection): Connection to the database
    filepath (string): Filepath to the folder of the JSON files
    func (function): callback to delegate the processing of the files in the folder 
    
    Returns:
    NULL
    
    """
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
    """
    The main program that starts the etl process
    
    Intializes the database connection to start processing the data from JSON files.
    The function delegates tasks to process the song file and then log file to store data in the database.
    
    Parameters:
    NULL
    
    Returns: 
    NULL
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()