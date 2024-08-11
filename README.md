# Building Snowflake DWH using Spotify Data

## Project Synopsis
This project is aimed at extracting the daily Top 50 Songs Playlist Data and store in **Snowflake DWH** .<br> The project is designed to automatically extract, process, and store raw data using **AWS LAMBDA** function and **SNOWPIPE** from a daily-updated top 50 songs playlist.<br> This data is crucial for performing in-depth analyses on **music trends**, **artist popularity**, and **album success** over time

## Dataset
The dataset used for this project is obtained from this [Spotify](https://open.spotify.com/playlist/37i9dQZF1DXcBWIGoYBM5M) playlist. <br> This playlist is updated everyday with the top 50 songs for the current day. The raw data is obtained in **JSON** semi-structured format , which is cleaned and the data is stored in different tables accordingly.

