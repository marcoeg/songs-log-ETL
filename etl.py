import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def process_song_file(cur, filepath):
    """ETL on song data; inserting data into a song and artist table
    
    Parameters: 
        cur - database cursor
        filepath - path to song data in filesystem
    Return: 
        None

    """ 

    print('song file: {}'.format(filepath))
    # read song file into a dataframe 
    df = pd.read_json(filepath, lines=True)
    # dataframe cleanup
    df.drop_duplicates(subset='song_id', keep='first')
    df.fillna(value='None')
    df.replace('', 'None')

    df.replace('None', None)
    # songs dataframe extraction
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']]

    # insert song records
    song_data_list = song_data.values.tolist()
    for row in song_data_list:
        print('\tsong row: {}'.format(row))
        cur.execute(song_table_insert, row)

    # artists dataframe extraction and cleanup    
    artist_data = df[['artist_id', 'artist_name', 'artist_latitude', 'artist_longitude']]
    artist_data = artist_data.drop_duplicates(subset='artist_id', keep='first')

    # insert artist records
    artist_data_list = artist_data.values.tolist()
    for row in artist_data_list:
        print('\tartist row: {}'.format(row))
        cur.execute(artist_table_insert, row)

def process_log_file(cur, filepath):
    """
    Transform and load log data for time, user and songplay 
    
    Parameters: 
        cur - database cursor
        filepath - path to song log data in the filesystem
    Return: 
        None

    """

    global times_count
    global songplays_count

    # read file into a dataframe and filter by NextSong
    df = pd.read_json(filepath, lines=True)
    data = df[df.page == 'NextSong'] 
    print('log file: {} length: {}'.format(filepath, len(data)))

    # ------ process time data
    #
    # convert timestamp column to datetime (in ms)
    timestamp_series = data.ts
    datetime_series = pd.to_datetime(timestamp_series, unit='ms')

    # extract timedata details and name columns
    timedata = [timestamp_series, datetime_series.dt.hour, datetime_series.dt.day, 
                datetime_series.dt.week, 
                datetime_series.dt.month, datetime_series.dt.year, 
                datetime_series.dt.weekday]
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']

    # combine timedata and labels
    labeled_timedata = dict(zip(column_labels, timedata))

    # iterate through dataframe and insert each row in time table
    time_df = pd.DataFrame(data=labeled_timedata)
    for i, row in time_df.iterrows():
        times_count += 1
        cur.execute(time_table_insert, list(row))

    # ------ process user data
    #
    # create user dataframe and remove duplicates
    user_df = data[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_df = user_df.drop_duplicates(subset='userId', keep='first')

    # insert user records in user table
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # ----- process songs played
    #
    # get songs played based on user logs by artist and song
    for index, row in data.iterrows():
        songplays_count += 1
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist))
        results = cur.fetchone()
        if results:
            songid, artistid = results
            print("results not null: {}".format(results))
        else:
            songid, artistid = None, None
   
        # insert songplay record into songplays table
        songplay_data = ([row.ts, row.userId, songid, row.level, artistid, 
                              row.sessionId, row.location, row.userAgent])
        cur.execute(songplay_table_insert, songplay_data)

def process_data(cur, conn, filepath, func):
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
    global times_count
    global songplays_count
    times_count = 0
    songplays_count = 0

    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='./data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='./data/log_data', func=process_log_file)

    conn.close()
    # check if the same number of rows have been processed for time and songplays
    # the actual count from the DB may be different due to colliding timestamps
    print("times_count: {} songplays_count: {}".format(times_count, songplays_count))

if __name__ == "__main__":
    main()