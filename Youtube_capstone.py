import requests
import json
import mysql.connector
import streamlit as st
import pandas as pd
from datetime import timedelta
from streamlit_option_menu import option_menu
from st_aggrid import AgGrid    
_connection = None


st.set_page_config(
    page_title="Youtube Data Harvesting",
    page_icon="Home",
    layout="wide",
    initial_sidebar_state="auto",
    menu_items=None
)

class Channel:
    def __init__(self, channel_id, channel_name, channel_type, channel_views, channel_description, channel_status):
        self.channel_id = channel_id
        self.channel_name = channel_name
        self.channel_type = channel_type
        self.channel_views = channel_views
        self.channel_description = channel_description
        self.channel_status = channel_status

class Playlist:
    def __init__(self, playlist_id, channel_id, playlist_name):
        self.playlist_id = playlist_id
        self.channel_id = channel_id
        self.playlist_name = playlist_name

class Video:
    def __init__(self, video_id, playlist_id, video_name, video_description, published_date, view_count, like_count, dislike_count, favorite_count, comment_count, duration, thumbnail, caption_status):
        self.video_id = video_id
        self.playlist_id = playlist_id
        self.video_name = video_name
        self.video_description = video_description
        self.published_date = published_date
        self.view_count = view_count
        self.like_count = like_count
        self.dislike_count = dislike_count
        self.favorite_count = favorite_count
        self.comment_count = comment_count
        self.duration = duration
        self.thumbnail = thumbnail
        self.caption_status = caption_status

class Comment:
    def __init__(self, comment_id, video_id, comment_text, comment_author, comment_published_date):
        self.comment_id = comment_id
        self.video_id = video_id
        self.comment_text = comment_text
        self.comment_author = comment_author
        self.comment_published_date = comment_published_date

def connect_to_mysql():
    global _connection
    if not _connection:
        _connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password=""
        )
    
def create_database():
     mycursor=_connection.cursor(buffered=True)
     mycursor.execute("CREATE DATABASE IF NOT EXISTS Youtube_Project")
     mycursor.execute("USE Youtube_project") 

def create_Channel_table():
    
    mycursor=_connection.cursor(buffered=True)
    mycursor.execute('''CREATE TABLE IF NOT EXISTS Youtube_project.Channel (
             channel_id varchar(255) PRIMARY KEY,
             channel_name varchar(255),
             channel_type varchar(255),
             channel_views INTEGER,
             channel_description TEXT,
             channel_status varchar(255)
             )''')
   
def create_Playlist_table():
   
    mycursor=_connection.cursor(buffered=True)
    mycursor.execute('''CREATE TABLE IF NOT EXISTS Youtube_Project.Playlist (
             playlist_id varchar(255) PRIMARY KEY,
             channel_id varchar(255),
             playlist_name varchar(255),
             FOREIGN KEY (channel_id) REFERENCES Channel(channel_id)
             )''')
    
def create_video_table():
    
    mycursor=_connection.cursor(buffered=True)
    mycursor.execute('''CREATE TABLE IF NOT EXISTS Youtube_Project.Video (
             video_id varchar(255) Primary Key,
             playlist_id varchar(255),
             video_name varchar(255),
             video_description TEXT,
             published_date DATETIME,
             view_count INT,
             like_count INT,    
             dislike_count INT,
             favorite_count INT,
             comment_count INT,
             duration TIME,
             thumbnail varchar(255),
             caption_status varchar(255),
             FOREIGN KEY (playlist_id) REFERENCES Playlist(playlist_id)
             )''')
    
def create_Comment_table():
    
    mycursor=_connection.cursor(buffered=True)
    mycursor.execute('''CREATE TABLE IF NOT EXISTS Youtube_Project.Comments (
                comment_id varchar(255) Primary Key,             
                video_id varchar(255),
                comment_text TEXT,
                comment_author varchar(255),
                comment_published_date DATETIME,
                FOREIGN KEY (video_id) REFERENCES Video(video_id)
                )''')
def fetch_data_from_youtube_api(url):
    response = requests.get(url)
    return json.loads(response.text)

def insert_channel_data(channel, myD):
    sql = "INSERT INTO Youtube_Project.channel (channel_name, channel_status, channel_description, channel_views, channel_id) VALUES (%s, %s, %s, %s, %s)"
    val = (channel.channel_name, channel.channel_status, channel.channel_description, channel.channel_views, channel.channel_id)
    cursor = myD.cursor(buffered=True)
    cursor.execute(sql, val)

def insert_playlist_data(playlist, myD):
    sql = "INSERT INTO Youtube_Project.playlist (channel_id, playlist_id, playlist_name) VALUES (%s, %s, %s)"
    val = (playlist.channel_id, playlist.playlist_id, playlist.playlist_name)
    cursor = myD.cursor(buffered=True)
    cursor.execute(sql, val)

def insert_video_data(video, myD):
    sql = "INSERT INTO Youtube_Project.video (video_id, video_name, playlist_id, video_description, published_date, view_count, like_count, dislike_count, favorite_count, comment_count, duration, thumbnail, caption_status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    val = (video.video_id, video.video_name, video.playlist_id, video.video_description, video.published_date, video.view_count, video.like_count,video.dislike_count, video.favorite_count, video.comment_count, video.duration, video.thumbnail, video.caption_status)
    cursor = myD.cursor(buffered=True)
    cursor.execute(sql, val)

def insert_comment_data(comment, myD):
    sql = "INSERT INTO Youtube_Project.comments (comment_id, video_id, comment_text, comment_author, comment_published_date) VALUES (%s, %s, %s, %s, %s)"
    val = (comment.comment_id, comment.video_id, comment.comment_text, comment.comment_author, comment.comment_published_date)
    cursor = myD.cursor(buffered=True)
    cursor.execute(sql, val)

def fetch_and_insert_channel_data(User_input):
    
    url = f"https://www.googleapis.com/youtube/v3/channels?id={User_input}&part=contentDetails,snippet,id,statistics,status&order=date&key=AIzaSyBPOr-dsgx3zMIUlOz6r3XC4RJ2S3yAVkA"
    data = fetch_data_from_youtube_api(url)
    channel_info = data['items'][0]['snippet']
    channel = Channel(
        channel_id=data['items'][0]['id'],
        channel_name=channel_info['title'],
        channel_type='',
        channel_views=data['items'][0]['statistics']['viewCount'],
        channel_description=channel_info['localized']['description'],
        channel_status=data['items'][0]['status']['privacyStatus']
    )
    insert_channel_data(channel, _connection)
    
    
    

def fetch_and_insert_playlist_data(User_input):

    url = f"https://www.googleapis.com/youtube/v3/playlists?channelId={User_input}&part=snippet,id,contentDetails,player,status&order=date&maxResults=40&key=AIzaSyBPOr-dsgx3zMIUlOz6r3XC4RJ2S3yAVkA"
    data = fetch_data_from_youtube_api(url)
   
    playlist_channel_id = data['items'][0]['snippet']['channelId']
    for item in data['items']:
        playlist = Playlist(
            playlist_id=item['id'],
            channel_id=playlist_channel_id,
            playlist_name=item['snippet']['title']
        )         
        insert_playlist_data(playlist, _connection)
   
def fetch_and_insert_video_data(User_input):
    mycursor = _connection.cursor(buffered=True)
    mycursor.execute("SELECT playlist_id, channel_id, playlist_name FROM Youtube_Project.playlist WHERE channel_id=%s", (User_input,))
    playlists = mycursor.fetchall()    
    for playlist_id in playlists:        
        playList = playlist_id[0]
        next_page_token = None 
        while True:
            url = f"https://www.googleapis.com/youtube/v3/playlistItems?playlistId={playList}&part=snippet,id,contentDetails,status&order=date&maxResults=40&key=AIzaSyBPOr-dsgx3zMIUlOz6r3XC4RJ2S3yAVkA"
            if next_page_token:
                url += f"&pageToken={next_page_token}"  
            data = fetch_data_from_youtube_api(url)
            for item in data.get('items', []):  
                vidID = item['contentDetails']['videoId']
                mycursor.execute("SELECT COUNT(*) FROM Youtube_Project.video WHERE video_id=%s", (vidID,))
                count = mycursor.fetchone()[0]
                if count > 0:
                    print(f"Video with ID {vidID} already exists in the database. Skipping insertion.")
                    continue  
                video_url = f"https://www.googleapis.com/youtube/v3/videos?id={vidID}&playlistId={playList}&part=snippet,id,contentDetails,status,statistics&order=date&maxResults=40&key=AIzaSyBPOr-dsgx3zMIUlOz6r3XC4RJ2S3yAVkA"
                video_data = fetch_data_from_youtube_api(video_url)
                if len(video_data.get('items', [])) > 0:
                    duration_str =video_data['items'][0]['contentDetails']['duration']
                    duration_str = duration_str.replace("PT", "")
                    hours = minutes = seconds = 0
                    if "H" in duration_str:
                        hours, duration_str = duration_str.split("H")
                        hours = int(hours)

                    if "M" in duration_str:
                        minutes, duration_str = duration_str.split("M")
                        minutes = int(minutes)

                    if "S" in duration_str:
                        seconds = int(duration_str[:-1])
                    time1 = timedelta(hours=hours, minutes=minutes, seconds=seconds)
                    video_info = video_data['items'][0]['snippet']
                    video = Video(
                        video_id=vidID,
                        playlist_id=playList,
                        video_name=video_info['title'],
                        video_description=video_info['description'],
                        published_date=video_info['publishedAt'],
                        view_count=video_data['items'][0]['statistics'].get("viewCount"),
                        like_count=video_data['items'][0]['statistics'].get("likeCount"),
                        dislike_count='',
                        favorite_count=video_data['items'][0]['statistics'].get("favoriteCount"),
                        comment_count=video_data['items'][0]['statistics'].get("commentCount"), 
                        duration= time1,
                        thumbnail=video_data['items'][0]['snippet']['thumbnails']['default'].get("url"),
                        caption_status=video_data['items'][0]['status']['uploadStatus']
                    )
                    insert_video_data(video, _connection)
            next_page_token = data.get('nextPageToken')  
            if not next_page_token:
                break 
               
def Duplicate_Check(User_input):
    input = str(User_input)
    mycursor=_connection.cursor(buffered=True)
    mycursor.execute("SELECT count(channel_id) FROM Youtube_Project.Channel Where channel_id =  %s", (input,))
    duplicate_count = mycursor.fetchone()
    number_of_rows=duplicate_count[0]
    if number_of_rows > 0:
        return True
    else:
        return False


def fetch_and_insert_comment_data(User_input):
    mycursor = _connection.cursor(buffered=True)
    mycursor.execute("SELECT DISTINCT video.video_id, channel.channel_name FROM Youtube_Project.channel INNER JOIN playlist ON channel.channel_id = playlist.channel_id INNER JOIN video ON playlist.playlist_id = video.playlist_id WHERE channel.channel_id = %s;", (User_input,))
    videos = mycursor.fetchall()
    for video_id, channel_name in videos:
        url_comments = f"https://www.googleapis.com/youtube/v3/commentThreads?videoId={video_id}&part=snippet,id,replies&maxResults=50&key=AIzaSyBPOr-dsgx3zMIUlOz6r3XC4RJ2S3yAVkA"
        comment_data = fetch_data_from_youtube_api(url_comments)
        if 'items' in comment_data:
            for comment_item in comment_data['items']:
                comment = Comment(
                    comment_id=comment_item['snippet']['topLevelComment']['id'],
                    video_id=video_id,
                    comment_text=comment_item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    comment_author=comment_item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    comment_published_date=comment_item['snippet']['topLevelComment']['snippet']['publishedAt']
                )          
                insert_comment_data(comment, _connection)
    
                    
   

st.title("YouTube Data Harvesting and Warehousing using SQL")

User_input=st.text_input("ENTER CHANNEL ID")

connect_to_mysql()                                                               # This function helps to connect Mysql server
create_database() 


def click_button():

    if len(User_input) > 0:  
        create_Channel_table()
        create_Playlist_table()
        create_video_table()
        create_Comment_table()
        _connection.commit()
        if Duplicate_Check(User_input):
            st.markdown(''':green['Record Alredy Exists']''')
        else:
            fetch_and_insert_channel_data(User_input)
            _connection.commit()
            fetch_and_insert_playlist_data(User_input)
            _connection.commit()
            fetch_and_insert_video_data(User_input)
            _connection.commit()
            fetch_and_insert_comment_data(User_input)
            _connection.commit()
            st.markdown(''':green['Record Created']''')


pr=st.button("SEARCH",on_click=click_button)                                   # if search button is clicked it calls click_button function

with st.sidebar:
    selected=option_menu(
        menu_title="LIST",
        options=["Result","List Of Channels"],
    )
if selected== "List Of Channels":
    query1="SELECT DISTINCT channel_name FROM Youtube_Project.channel inner join playlist on channel.channel_id=playlist.channel_id inner join video on playlist.playlist_id=video.playlist_id;"
    mycursor=_connection.cursor(buffered=True)
    mycursor.execute(query1)    
    t1=mycursor.fetchall()
    df=pd.DataFrame(t1,columns=["Channel_name"])
    AgGrid(df)

if selected=="Result":

    question=st.sidebar.selectbox("Select Question",("Select your question",
                                    "1) What are the names of all the videos and their corresponding channels?",
                                    "2) Which channels have the most number of videos and how many videos do they have?",
                                    "3) What are the top 10 most viewed videos and their respective channels?",
                                    "4) How many comments were made on each video, and what are theircorresponding video names?",
                                    "5) Which videos have the highest number of likes, and what are their corresponding channel names?",
                                    "6) What is the total number of likes each video and what are their corresponding video names?",
                                    "7) What is the total number of views for each channel and what are their corresponding channel names?",
                                    "8) What are the names of all the channels that have published videos in the year 2022?",
                                    "9) What is the average duration of all videos in each channel and what are their corresponding channel names?",
                                    "10) Which videos have the highest number of comments and what are their corresponding channel names?"))



    if  question=="Select your question":
        st.write("")
        
    elif question=="1) What are the names of all the videos and their corresponding channels?":
        query1="SELECT DISTINCT channel_name,video_name FROM Youtube_Project.channel inner join playlist on channel.channel_id=playlist.channel_id inner join video on playlist.playlist_id=video.playlist_id;"
        mycursor=_connection.cursor(buffered=True)
        mycursor.execute(query1)    
        t1=mycursor.fetchall()
        df=pd.DataFrame(t1,columns=["Channel_name","Video_name"])
        AgGrid(df)
        

    elif question=="2) Which channels have the most number of videos and how many videos do they have?":
        query2=" SELECT channel_name,count(video_name) as Number_of_videos FROM Youtube_Project.channel inner join playlist on channel.channel_id=playlist.channel_id inner join video on playlist.playlist_id=video.playlist_id group by channel_name ;"
        mycursor=_connection.cursor(buffered=True)
        mycursor.execute(query2)
        t2=mycursor.fetchall()
        df2=pd.DataFrame(t2,columns=["channel name","No of videos"])
        AgGrid(df2)

    elif question=="3) What are the top 10 most viewed videos and their respective channels?":
        query3='''SELECT DISTINCT video_name, channel_name, view_count FROM Youtube_Project.channel inner join playlist on channel.channel_id=playlist.channel_id
        inner join video on playlist.playlist_id=video.playlist_id 
        order by video.view_count desc
        LIMIT 10;'''
        mycursor=_connection.cursor(buffered=True)
        mycursor.execute(query3)
        t3=mycursor.fetchall()
        df3=pd.DataFrame(t3,columns=["Video_Name","channel_Name","view_count"])
        AgGrid(df3)

    elif question=="4) How many comments were made on each video, and what are theircorresponding video names?":
        query4='''SELECT DISTINCT channel_name,video_name,comment_count FROM Youtube_Project.channel inner join playlist on channel.channel_id=playlist.channel_id
        inner join video on playlist.playlist_id=video.playlist_id 
        inner join comments on video.video_id=Video.video_id
        order by comment_count desc;'''
        mycursor=_connection.cursor(buffered=True)
        mycursor.execute(query4)
        t4=mycursor.fetchall()
        df4=pd.DataFrame(t4,columns=["Channel_Name","video_Name","Comment_Count"])
        AgGrid(df4)

    elif question=="5) Which videos have the highest number of likes, and what are their corresponding channel names?":
        query5='''SELECT DISTINCT Channel_Name, Video_Name, like_count FROM Youtube_Project.channel inner join playlist on channel.channel_id=playlist.channel_id
        inner join video on playlist.playlist_id=video.playlist_id 
        inner join comments on video.video_id=Video.video_id  
    ORDER BY `video`.`comment_count` DESC;'''
        mycursor=_connection.cursor(buffered=True)
        mycursor.execute(query5)
        t5=mycursor.fetchall()
        df5=pd.DataFrame(t5,columns=["Channel_Name","Video_Name","like_count"])
        AgGrid(df5)

    elif question=="6) What is the total number of likes each video and what are their corresponding video names?":
        query6='''SELECT DISTINCT  Video_name, Like_count FROM Youtube_Project.channel inner join playlist on channel.channel_id=playlist.channel_id
        inner join video on playlist.playlist_id=video.playlist_id 
        inner join comments on video.video_id=Video.video_id  
    ORDER BY `video`.`comment_count` DESC;'''
        mycursor=_connection.cursor(buffered=True)
        mycursor.execute(query6)
        t6=mycursor.fetchall()
        df6=pd.DataFrame(t6,columns=["Video_Name","Like_count"])
        AgGrid(df6)

    elif question=="7) What is the total number of views for each channel and what are their corresponding channel names?":
        query7='''SELECT channel.channel_name,channel.channel_views from Youtube_Project.channel;'''
        mycursor=_connection.cursor(buffered=True)
        mycursor.execute(query7)
        t7=mycursor.fetchall()
        df7=pd.DataFrame(t7,columns=["channel_name","total_views"])
        AgGrid(df7)

    elif question=="8) What are the names of all the channels that have published videos in the year 2022?":
        query8='''SELECT channel.channel_name,count(video.video_name) 
    from Youtube_Project.channel inner join playlist on channel.channel_id=playlist.channel_id inner join video on playlist.playlist_id=video.playlist_id inner join comments on video.video_id=comments.video_id 
    WHERE video.published_date BETWEEN '2022-01-01' AND '2022-12-12';'''
        mycursor=_connection.cursor(buffered=True)
        mycursor.execute(query8)
        t8=mycursor.fetchall()
        df8=pd.DataFrame(t8,columns=["channel_name","Video_count"])
        AgGrid(df8)

    elif question=="9) What is the average duration of all videos in each channel and what are their corresponding channel names?":
        query9='''SELECT 
        channel.channel_name,  SEC_TO_TIME(AVG(TIME_TO_SEC(video.duration))) AS AvgTime 
    FROM 
        Youtube_Project.channel  INNER JOIN playlist ON channel.channel_id = playlist.channel_id INNER JOIN video ON playlist.playlist_id = video.playlist_id INNER JOIN comments ON video.video_id = comments.video_id 
    GROUP BY 
        channel.channel_name;'''
        mycursor=_connection.cursor(buffered=True)
        mycursor.execute(query9)
        t9=mycursor.fetchall()
        df9=pd.DataFrame(t9,columns=["channel_name","Average_Duration"])
        AgGrid(df9)
    

    elif question=="10) Which videos have the highest number of comments and what are their corresponding channel names?":
        query10='''SELECT channel_name,video_name,comment_count FROM Youtube_Project.channel inner join playlist on channel.channel_id=playlist.channel_id
        inner join video on playlist.playlist_id=video.playlist_id 
        order by comment_count desc
        LIMIT 1;'''
        mycursor=_connection.cursor(buffered=True)
        mycursor.execute(query10)
        t10=mycursor.fetchall()
        df10=pd.DataFrame(t10,columns=["video title","channel name","comments"])
        AgGrid(df10)






