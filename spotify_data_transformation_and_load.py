import json
import boto3
from datetime import datetime
import pytz
import pandas as pd
from io import StringIO

def album(data):
     album_list= []
     for rows in data['items']:
         album_id=rows['track']['album']['id']
         album_name=rows['track']['album']['name']
         album_release_date=rows['track']['album']['release_date']
         total_tracks_in_album=rows['track']['album']['total_tracks']
         album_url=rows['track']['album']['external_urls']['spotify']
         album_element={'album_id':album_id,'name':album_name,'release_date':album_release_date,
                        'total_tracks_in_album':total_tracks_in_album,'url':album_url}
     
         album_list.append(album_element)
     return album_list

def artist(data):
     artist_list=[]
     for row in data['items']:
         for key, value in row.items():
             if key=='track':
                 for artists in value['artists']:
                     artist_dict={'artist_id':artists['id'],'artist_name':artists['name'],'external_url':artists['href']}
                     artist_list.append(artist_dict)
                     
     return artist_list                
     
     
def songs(data):
     song_list=[]
     for row in data['items']:
         song_id=row['track']['id']
         song_name=row['track']['name']
         song_duration=row['track']['duration_ms']
         song_url=row['track']['external_urls']['spotify']
         song_popularity=row['track']['popularity']
         song_added=row['added_at']
         album_id=row['track']['album']['id']
         artist_id=row['track']['album']['artists'][0]['id']
         song_element= {'song_id':song_id,'song_name':song_name,'duration_ms':song_duration,'url':song_url,'popularity':song_popularity,
                       'song_added':song_added,'album_id':album_id,'artist_id':artist_id}
         song_list.append(song_element)
         
     return song_list


def lambda_handler(event, context):
     s3_data=boto3.client('s3')
     Bucket="spotify-snowflake-etl"
     Key="raw_data/to_be_processed/"
     
     
     spotify_data=[]
     spotify_keys=[]
     for data in s3_data.list_objects(Bucket=Bucket,Prefix=Key)['Contents']:
          file_key=data['Key']
          if file_key.split('.')[-1]=="json":
               response=s3_data.get_object(Bucket=Bucket,Key=file_key)
               content=response['Body']
               jsonObject=json.loads(content.read())
               spotify_data.append(jsonObject)
               spotify_keys.append(file_key)
               
     utc_now = datetime.now(pytz.utc)
     local_timezone = pytz.timezone('America/Edmonton')  
     local_date = utc_now.astimezone(local_timezone).date()
     local_time = utc_now.astimezone(local_timezone).time()
     
     for data in spotify_data:
          album_list=album(data)
          artist_list=artist(data)
          song_list=songs(data)
          
         
          
          
          album_df=pd.DataFrame.from_dict(album_list)
          album_df=album_df.drop_duplicates(subset=['album_id'])
          album_df['release_date']=pd.to_datetime(album_df['release_date'])
          album_df['chart_date']=local_date
          album_df['chart_date']=pd.to_datetime(album_df['chart_date'])
          
          artist_df=pd.DataFrame.from_dict(artist_list)
          artist_df=artist_df.drop_duplicates(subset=['artist_id'])
          artist_df['chart_date']=local_date
          artist_df['chart_date']=pd.to_datetime(artist_df['chart_date'])
          
          song_df=pd.DataFrame.from_dict(song_list)
          song_df['song_added']=pd.to_datetime(song_df['song_added'])
          song_df['chart_date']=local_date
          song_df['chart_date']=pd.to_datetime(song_df['chart_date'])
          
          song_key="transformed_data/songs_data/song_transformed" +"_" + str(local_date) +" " +str(local_time)+".csv"
          song_buffer=StringIO()
          song_df.to_csv(song_buffer,index=False)
          song_content=song_buffer.getvalue()
          s3_data.put_object(Bucket=Bucket,Key=song_key,Body=song_content)
          
          album_key="transformed_data/album_data/album_transformed" +"_" + str(local_date) +" " + str(local_time)+".csv"
          album_buffer=StringIO()
          album_df.to_csv(album_buffer,index=False)
          album_content=album_buffer.getvalue()
          s3_data.put_object(Bucket=Bucket,Key=album_key,Body=album_content)
          
          artist_key="transformed_data/artist_data/artist_transformed" +"_" + str(local_date) +" " + str(local_time)+".csv"
          artist_buffer=StringIO()
          artist_df.to_csv(artist_buffer,index=False)
          artist_content=artist_buffer.getvalue()
          s3_data.put_object(Bucket=Bucket,Key=artist_key,Body=artist_content)
          
     s3_resource=boto3.resource('s3')
     for key in spotify_keys:
          copy_source= {
               'Bucket': Bucket,
               'Key': key
               
          }
          s3_resource.meta.client.copy(copy_source,Bucket,'raw_data/processed/' + key.split("/")[-1])
          s3_resource.Object(Bucket,key).delete()