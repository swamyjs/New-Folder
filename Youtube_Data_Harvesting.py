import pymongo
import psycopg2
import pandas as pd
import streamlit as st
from googleapiclient.discovery import build


#API key connection
def Api_connect():
    Api_Id="AIzaSyCnIWfabAR6EIICDD0x7kFPIOaJenEW2JU"

    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name,api_version,developerKey=Api_Id)
    return youtube

youtube=Api_connect()


#get channel information
def get_channel_info(channel_id):
    
    request = youtube.channels().list(
                part = "snippet,contentDetails,Statistics",
                id = channel_id)
            
    response1=request.execute()

    for i in range(0,len(response1["items"])):
        data = dict(
                    Channel_Name = response1["items"][i]["snippet"]["title"],
                    Channel_Id = response1["items"][i]["id"],
                    Subscription_Count= response1["items"][i]["statistics"]["subscriberCount"],
                    Views = response1["items"][i]["statistics"]["viewCount"],
                    Total_Videos = response1["items"][i]["statistics"]["videoCount"],
                    Channel_Description = response1["items"][i]["snippet"]["description"],
                    Playlist_Id = response1["items"][i]["contentDetails"]["relatedPlaylists"]["uploads"],
                    )
        return data
    
#get playlist ids
def get_playlist_info(channel_id):
    All_data = []
    next_page_token = None
    next_page = True
    while next_page:

        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
            )
        response = request.execute()

        for item in response['items']: 
            data={'PlaylistId':item['id'],
                    'Title':item['snippet']['title'],
                    'ChannelId':item['snippet']['channelId'],
                    'ChannelName':item['snippet']['channelTitle'],
                    'PublishedAt':item['snippet']['publishedAt'],
                    'VideoCount':item['contentDetails']['itemCount']}
            All_data.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            next_page=False
    return All_data

#get video ids
def get_channel_videos(channel_id):
    video_ids = []
    # get Uploads playlist id
    res = youtube.channels().list(id=channel_id, 
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        res = youtube.playlistItems().list( 
                                           part = 'snippet',
                                           playlistId = playlist_id, 
                                           maxResults = 50,
                                           pageToken = next_page_token).execute()
        
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_ids


#get video information
def get_video_info(video_ids):

    video_data = []

    for video_id in video_ids:
        request = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id= video_id)
        response = request.execute()

        for item in response["items"]:
            data = dict(Channel_Name = item['snippet']['channelTitle'],
                        Channel_Id = item['snippet']['channelId'],
                        Video_Id = item['id'],
                        Title = item['snippet']['title'],
                        Tags = item['snippet'].get('tags'),
                        Thumbnail = item['snippet']['thumbnails']['default']['url'],
                        Description = item['snippet']['description'],
                        Published_Date = item['snippet']['publishedAt'],
                        Duration = item['contentDetails']['duration'],
                        Views = item['statistics']['viewCount'],
                        Likes = item['statistics'].get('likeCount'),
                        Comments = item['statistics'].get('commentCount'),
                        Favorite_Count = item['statistics']['favoriteCount'],
                        Definition = item['contentDetails']['definition'],
                        Caption_Status = item['contentDetails']['caption']
                        )
            video_data.append(data)
    return video_data


#get comment information
def get_comment_info(video_ids):
        Comment_Information = []
        try:
                for video_id in video_ids:

                        request = youtube.commentThreads().list(
                                part = "snippet",
                                videoId = video_id,
                                maxResults = 50
                                )
                        response5 = request.execute()
                        
                        for item in response5["items"]:
                                comment_information = dict(
                                        Comment_Id = item["snippet"]["topLevelComment"]["id"],
                                        Video_Id = item["snippet"]["videoId"],
                                        Comment_Text = item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                                        Comment_Author = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                        Comment_Published = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])

                                Comment_Information.append(comment_information)
        except:
                pass
                
        return Comment_Information


#MongoDB Connection
client = pymongo.MongoClient("mongodb+srv://swamyjs891:swamy@swamyj.iifmklq.mongodb.net/?retryWrites=true&w=majority&appName=swamyj")
db = client["youtube_data"]

# upload to MongoDB

def channel_details(channel_id):
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_info(channel_id)
    vi_ids = get_channel_videos(channel_id)
    vi_details = get_video_info(vi_ids)
    com_details = get_comment_info(vi_ids)

    coll1 = db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,"video_information":vi_details,
                     "comment_information":com_details})
    
    return "upload completed successfully"


#Table creation for channels,playlists, videos, comments
def channels_table():
    mydb = psycopg2.connect(host="localhost",
            user="postgres",
            password="swamy",
            database= "youtube_data",
            port = "5432"
            )
    cursor = mydb.cursor()

    drop_query = "drop table if exists channels"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''create table if not exists channels(Channel_Name varchar(100),
                        Channel_Id varchar(80) primary key, 
                        Subscription_Count bigint, 
                        Views bigint,
                        Total_Videos int,
                        Channel_Description text,
                        Playlist_Id varchar(50))'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        st.write("Channels Table alredy created")


    ch_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df = pd.DataFrame(ch_list)
    
    for index,row in df.iterrows():
        insert_query = '''INSERT into channels(Channel_Name,
                                                    Channel_Id,
                                                    Subscription_Count,
                                                    Views,
                                                    Total_Videos,
                                                    Channel_Description,
                                                    Playlist_Id)
                                        VALUES(%s,%s,%s,%s,%s,%s,%s)'''


        values =(
                row['Channel_Name'],
                row['Channel_Id'],
                row['Subscription_Count'],
                row['Views'],
                row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_Id'])
        try:                     
            cursor.execute(insert_query,values)
            mydb.commit()    
        except:
            st.write("Channels values are already inserted")    

def playlist_table():
        mydb = psycopg2.connect(host="localhost",
                user="postgres",
                password="swamy",
                database= "youtube_data",
                port = "5432"
                )
        cursor = mydb.cursor()

        drop_query = "drop table if exists playlist"
        cursor.execute(drop_query)
        mydb.commit()


        create_query = '''create table if not exists playlist(PlaylistId varchar(100) primary key,
                        Title varchar(100) , 
                        ChannelId varchar(100), 
                        ChannelName varchar(100),
                        PublishedAt timestamp,
                        VideoCount int
                        )'''
        cursor.execute(create_query)
        mydb.commit()

        pl_list = []
        db = client["youtube_data"]
        coll1 = db["channel_details"]
        for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
                for i in range(len(pl_data["playlist_information"])):
                        pl_list.append(pl_data["playlist_information"][i])
        df1=pd.DataFrame(pl_list)

        for index,row in df1.iterrows():
                insert_query = '''INSERT into playlist(PlaylistId,
                                                        Title,
                                                        ChannelId,
                                                        ChannelName,
                                                        PublishedAt,
                                                        VideoCount
                                                        )
                                                VALUES(%s,%s,%s,%s,%s,%s)'''


                values =(
                        row['PlaylistId'],
                        row['Title'],
                        row['ChannelId'],
                        row['ChannelName'],
                        row['PublishedAt'],
                        row['VideoCount']
                        )
                                
                cursor.execute(insert_query,values)
                mydb.commit()


def videos_table():
        mydb = psycopg2.connect(host="localhost",
                user="postgres",
                password="swamy",
                database= "youtube_data",
                port = "5432"
                )
        cursor = mydb.cursor()

        drop_query = "drop table if exists videos"
        cursor.execute(drop_query)
        mydb.commit()

        create_query = '''create table if not exists videos(
                                Channel_Name varchar(150),
                                Channel_Id varchar(100),
                                Video_Id varchar(50) primary key, 
                                Title varchar(150), 
                                Tags text,
                                Thumbnail varchar(225),
                                Description text, 
                                Published_Date timestamp,
                                Duration interval, 
                                Views bigint, 
                                Likes bigint,
                                Comments int,
                                Favorite_Count int, 
                                Definition varchar(10), 
                                Caption_Status varchar(50) 
                                )''' 
                                
        cursor.execute(create_query)             
        mydb.commit()

        vi_list = []
        db = client["youtube_data"]
        coll1 = db["channel_details"]
        for vi_data in coll1.find({},{"_id":0,"video_information":1}):
                for i in range(len(vi_data["video_information"])):
                        vi_list.append(vi_data["video_information"][i])
        df2=pd.DataFrame(vi_list)

        for index,row in df2.iterrows():
                insert_query = '''INSERT into videos(Channel_Name,
                                                        Channel_Id,
                                                        Video_Id,
                                                        Title,
                                                        Tags,
                                                        Thumbnail,
                                                        Description,
                                                        Published_Date,
                                                        Duration,
                                                        Views,
                                                        Likes,
                                                        Comments,
                                                        Favorite_Count,
                                                        Definition,
                                                        Caption_Status
                                                        )
                                        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''


                values =(
                        row['Channel_Name'],
                        row['Channel_Id'],
                        row['Video_Id'],
                        row['Title'],
                        row['Tags'],
                        row['Thumbnail'],
                        row['Description'],
                        row['Published_Date'],
                        row['Duration'],
                        row['Views'],
                        row['Likes'],
                        row['Comments'],
                        row['Favorite_Count'],
                        row['Definition'],
                        row['Caption_Status']
                        )
                            
                cursor.execute(insert_query,values)
                mydb.commit()


def comments_table():       
        mydb = psycopg2.connect(host="localhost",
                user="postgres",
                password="swamy",
                database= "youtube_data",
                port = "5432"
                )
        cursor = mydb.cursor()

        drop_query = "drop table if exists comments"
        cursor.execute(drop_query)
        mydb.commit()

        create_query = '''create table if not exists comments(
                        Comment_Id varchar(100) primary key,
                        Video_Id varchar(80),
                        Comment_Text text, 
                        Comment_Author varchar(150),
                        Comment_Published timestamp)'''
        
        cursor.execute(create_query)             
        mydb.commit()

        com_list = []
        db = client["youtube_data"]
        coll1 = db["channel_details"]
        for com_data in coll1.find({},{"_id":0,"comment_information":1}):
                for i in range(len(com_data["comment_information"])):
                        com_list.append(com_data["comment_information"][i])
        df3=pd.DataFrame(com_list)

        for index,row in df3.iterrows():
                insert_query = '''INSERT into comments(Comment_Id,
                                                        Video_Id,
                                                        Comment_Text,
                                                        Comment_Author,
                                                        Comment_Published
                                                        )
                                                VALUES(%s,%s,%s,%s,%s)'''


                values =(
                        row['Comment_Id'],
                        row['Video_Id'],
                        row['Comment_Text'],
                        row['Comment_Author'],
                        row['Comment_Published']
                        )
                                
                cursor.execute(insert_query,values)
                mydb.commit()


def tables():
    channels_table()
    playlist_table()
    videos_table()
    comments_table()

    return"tables created successfully"


def show_channel_table():
    ch_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df = st.dataframe(ch_list)

    return df

def show_playlist_table():
        pl_list = []
        db = client["youtube_data"]
        coll1 = db["channel_details"]
        for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
                for i in range(len(pl_data["playlist_information"])):
                        pl_list.append(pl_data["playlist_information"][i])
        df1=st.dataframe(pl_list)

        return df1

def show_video_table():
        vi_list = []
        db = client["youtube_data"]
        coll1 = db["channel_details"]
        for vi_data in coll1.find({},{"_id":0,"video_information":1}):
                for i in range(len(vi_data["video_information"])):
                        vi_list.append(vi_data["video_information"][i])
        df2=st.dataframe(vi_list)

        return df2

def show_comment_table():
        com_list = []
        db = client["youtube_data"]
        coll1 = db["channel_details"]
        for com_data in coll1.find({},{"_id":0,"comment_information":1}):
                for i in range(len(com_data["comment_information"])):
                        com_list.append(com_data["comment_information"][i])
        df3=st.dataframe(com_list)

        return df3

#streamlit 

with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSE]")
    st.header("Skill Take Away")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("Api Intigration")
    st.caption("Data Management using MongoDB and SQL")
    
channel_id=st.text_input("Enter the Channel ID")

if st.button("Collect and store Data"):
    ch_ids=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])

    if channel_id in ch_ids:
        st.success("channel details of the given channel id already exists")

    else:
        insert=channel_details(channel_id)
        st.success(insert)
if st.button("Migrate to Sql"):
    Table=tables()
    st.success(Table)
show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLIST","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channel_table()
elif show_table=="PLAYLIST":
    show_playlist_table()
elif show_table=="VIDEOS":
    show_video_table()
elif show_table=="COMMENTS":
    show_comment_table()




#SQL Conection
mydb = psycopg2.connect(host="localhost",
        user="postgres",
        password="swamy",
        database= "youtube_data",
        port = "5432"
        )
cursor = mydb.cursor()

question=st.selectbox("Selection your question",("1.All the videos and channel Name",
                                                "2.channels with most number of videos",
                                                "3.10 most viewed videos",
                                                "4.comments in each videos",
                                                "5.Videos with higest likes",
                                                "6.likes of all videos",
                                                "7.views of each channel",
                                                "8.videos published in the year 2022",
                                                "9.average duration of all videos in each channel",
                                                "10.videos with highest number of comments"))

if question=="1.All the videos and channel Name":
        query1="select title as videos,channel_name as channelname from videos;"
        cursor.execute(query1)
        mydb.commit()
        t1=cursor.fetchall()
        df=pd.DataFrame(t1,columns=["video title","channel name"])
        st.write(df)

elif question=="2.channels with most number of videos":
        query2='''select channel_name as channelname,total_videos as no_videos from channels
                        order by total_videos desc'''
        cursor.execute(query2)
        mydb.commit()
        t2=cursor.fetchall()
        df2=pd.DataFrame(t2,columns=["Channel name","No of Videos"])
        st.write(df2)



elif question=="3.10 most viewed videos":
        query3='''select views as views,channel_name as channelname,title as videotitle from videos
                        where views is not null order by views desc limit 10'''
        cursor.execute(query3)
        mydb.commit()
        t3=cursor.fetchall()
        df3=pd.DataFrame(t3,columns=["Views","Channel Name","Video Title"])
        st.write(df3)

elif question=="4.comments in each videos":
        query4='''select comments as no_comments,title as videotitle from videos where comments is not null'''
        cursor.execute(query4)
        mydb.commit()
        t4=cursor.fetchall()
        df4=pd.DataFrame(t4,columns=["no of comments","videotitle"])
        st.write(df4)

        
elif question=="5.Videos with higest likes":
        query5='''select Title as VideoTitle, Channel_Name as ChannelName, Likes as LikesCount from videos 
                        where Likes is not null order by Likes desc'''
        cursor.execute(query5)
        mydb.commit()
        t5=cursor.fetchall()
        df5=pd.DataFrame(t5,columns=["VideoTitle","ChannelName","LikesCount"])
        st.write(df5)

elif question=="6.likes of all videos":
        query6='''select Likes as likeCount,Title as VideoTitle from videos'''
        cursor.execute(query6)
        mydb.commit()
        t6=cursor.fetchall()
        df6=pd.DataFrame(t6,columns=["likeCount","VideoTitle"])
        st.write(df6)

elif question=="7.views of each channel":
        query7='''select views as Views,Channel_Name as ChannelName from channels'''
        cursor.execute(query7)
        mydb.commit()
        t7=cursor.fetchall()
        df7=pd.DataFrame(t7,columns=["Views","ChannelName"])
        st.write(df7) 


elif question=="8.videos published in the year 2022":
        query8='''select Title as Video_Title, Published_Date as VideoRelease, Channel_Name as ChannelName from videos 
                        where extract(year from Published_Date) = 2022'''
        cursor.execute(query8)
        mydb.commit()
        t8=cursor.fetchall()
        df8=pd.DataFrame(t8,columns=["Video_Title","VideoRelease","ChannelName"])
        st.write(df8)

elif question=="9.average duration of all videos in each channel":
        query9='''select Channel_Name as ChannelTitle, AVG(Duration) AS average_duration FROM videos GROUP BY Channel_Name
        '''
        cursor.execute(query9)
        mydb.commit()
        t9=cursor.fetchall()
        df9=pd.DataFrame(t9,columns=["ChannelTitle","averageduration"])
        T9=[]
        for index,row in df9.iterrows():
                channel_title=row["ChannelTitle"]
                average_duration=row["averageduration"]
                average_duration_str=str(average_duration)
                T9.append(dict(channeltitle=channel_title,avgduration=average_duration))
        df1=pd.DataFrame(T9)        
        st.write(df1)

elif question=="10.videos with highest number of comments":
        query10='''select Title as VideoTitle, Channel_Name as ChannelName, Comments as Comments from videos 
                       where Comments is not null order by Comments desc'''
        cursor.execute(query10)
        mydb.commit()
        t10=cursor.fetchall()
        df10=pd.DataFrame(t10,columns=["VideoTitle","ChannelName","Comments"])
        st.write(df10)        
