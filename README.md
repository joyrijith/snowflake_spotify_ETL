# Building Snowflake DWH using Spotify Data

## Project Synopsis
This project is aimed at extracting the daily Top 50 Songs Playlist Data and store in **Snowflake DWH** .<br> The project is designed to automatically extract, process, and store raw data using **AWS LAMBDA** function and **SNOWPIPE** from a daily-updated top 50 songs playlist.<br> This data is crucial for performing in-depth analyses on **Music Trends**, **Artist Popularity**, and **Album Success** over time

## Dataset
The dataset used for this project is obtained from this [Spotify](https://open.spotify.com/playlist/37i9dQZF1DXcBWIGoYBM5M) playlist. <br> This playlist is updated everyday with the top 50 songs for the current day. The raw data is obtained in **JSON** semi-structured format , which is cleaned and the data is stored in different tables accordingly.

## Tools Used
[Spotify API](https://developer.spotify.com/) - Create account in Spotify for Developers to access the data from Spotify API  
[AWS](https://aws.amazon.com/free/?gclid=Cj0KCQjwwuG1BhCnARIsAFWBUC1FznXUoF_Ju4iKAFVF8E2ax7irQ81uRWDkMhYsj97HIWnt6FIkr_saAvJpEALw_wcB&trk=e747cc26-a307-4ae0-981a-6dc5c1cb4121&sc_channel=ps&ef_id=Cj0KCQjwwuG1BhCnARIsAFWBUC1FznXUoF_Ju4iKAFVF8E2ax7irQ81uRWDkMhYsj97HIWnt6FIkr_saAvJpEALw_wcB:G:s&s_kwcid=AL!4422!3!453053794209!e!!g!!aws!10705896207!102406402981&all-free-tier.sort-by=item.additionalFields.SortRank&all-free-tier.sort-order=asc&awsf.Free%20Tier%20Types=*all&awsf.Free%20Tier%20Categories=*all) - Need AWS account to access services like **S3**, **Lambda Functions**, **Triggers** and **Cloud Watch Events** to extract and process Data <br>
[Snowflake](https://www.snowflake.com/en/) - Need Snowflake account to build Database,Tables, Schemas and to buid **Snowpipes** to automate the process of loading data from External Storages **S3** used in this project.

## Project Architecture and Flow
![image](https://github.com/user-attachments/assets/4bcca508-92ae-42c5-96a6-322d61d41c29)

1.Create an account in Spotify for Developers to access the api to extract the data <br>
2.Create your AWS account  <br>
3.Build the lamdba function to extract the raw JSON data for the playlist you want to work with. The function is triggered using AWS cloud watch events<br>
4.On extracting the raw data store it in S3 bucket<br>
5.Build a lambda function to transform the raw data extracted and stored in S3 Database. The function is triggered using S3 trigger<br>
6.Using snowpipe the transformed data from S3 bucket is loaded into Snowflake Database<br>

## Prerequisite
1.AWS Account . Need access to S3, Lambda function
2.Snowflake Account.

## Running this Project
- Create Developer account for accesing spotify API <br>
- Generate the spotify Secret key and Secret access code <br>
- Select the playlist you want to work with in Spotify<br>
- Clone the repository in VS code to have a copy of the code<br>
- For extracting the data from Spotify API<br>
  - In AWS lambda store the Secret Key and Secret Password in Credential Manager and reference the credentials <br>
  - Reference the playlist and extract the json data <br>
  - Using the boto package in lambda store the data in S3 Bucket <br>
  - Use Cloudwatch Events to extract data as per your need at the required interval (The data is extracted once per day in this project)
    ![image](https://github.com/user-attachments/assets/b88ace0b-d169-4233-bc52-aaec78a3b74b)

- For transforming data
  - Clean and split the data into 3 different tables **Artist**, **Songs** and **Album**
  - Store the cleaned data in different folders in S3
  - Use S3 trigger to trigger this cleaning module once the raw data is availble in the S3 folder
  ![image](https://github.com/user-attachments/assets/017352d6-25e0-4f3b-b7b8-f19e0630d9d4)

- Snowflake
  - Create Database, Schemas and Table to store the data in Snowflake DWH
  - Create Stage, File Format and Secure connection to access the Data in AWS S3 Bucket
 ![image](https://github.com/user-attachments/assets/fda227d0-60c2-4a3e-bd67-c2b1d6d5ddd9)

- Snowpipe
  - Create the connection between Snowflake DB and the S3 bucket with the necessary details to build the trust and connection
  - Snowpipe will be triggered once the data is available in the source folder which has the transformed data.
![image](https://github.com/user-attachments/assets/32b10d4c-e2df-4c55-9544-555583f89aec)

 
## Visualization
The data analysis and visualisation for this project will be carried out in the future once the Datawarehouse has enough data over a period of time for the analysis<br>



