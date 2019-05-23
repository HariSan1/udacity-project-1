import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file_docstring(cur, filepath):
    """
    Read and process song files and populate songs table
    Create dataframe, convert to list and insert

    Keyword arguments:
        cur:      cursor command, allows connection to conn db connection
        filepath: location + name of data files
    """
        
def process_song_file(cur, filepath):

    df = pd.read_json(filepath, lines = True)

    df_song=df[['song_id','title','artist_id','year','duration']]
    song_data = df_song.values.tolist()
    cur.execute(song_table_insert, song_data[0])
    
    df_artists=df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    artist_data = df_artists.values.tolist()
    cur.execute(artist_table_insert, artist_data[0])

def process_log_file_docstring(cur, filepath):
    """
    Read and process time log files and populate time table
    Read and process user files and populate users table
    Read, manipulate, query by song_id and artist_id
    Extract song records that match query and populate songplay table
    Convert timestamp to datetime, create dictionary, populate time table

    Keyword arguments:
        cur:      cursor command, allows connection to conn db connection
        filepath: location + name of data files
    """
      
def process_log_file(cur, filepath):

    df = df = pd.read_json(filepath, lines= True)

    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    t = df['ts'] 
    
    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('timestamp', 'hour', 'day', 'week of year', 'month', 'year', 'weekday') 
    time_dictionary = dict(zip(column_labels, time_data))
    time_df = pd.DataFrame.from_dict(time_dictionary)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))


    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_df = user_df.drop_duplicates(subset='userId', keep='first', inplace=False)

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records - for each row in dataframe, query for matches and insert
    for index, row in df.iterrows():
        
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)

def process_data_docstring(cur, conn, filepath, func):
    """
    Process all data files in dataset
        Get list of all files
        Iterate over files and process
    
    keyword arguments:
        cur:      cursor command, allows connection to conn db connection
        conn:     connection specifics for db connection
        filepath: location+name of files
        func:     to call function
    
    """    
        
def process_data(cur, conn, filepath, func):
    
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))

def main_docstring():
    """
    Call process_data function to process song data and then again to process log data
    These functions extract, load and transform (ETL) music dataset data into tables
    """
        
def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()