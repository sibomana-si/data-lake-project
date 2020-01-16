# SPARKIFY SONG PLAY ANALYSIS: DATA LAKE

## Description

This codebase implements an [ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load) pipeline that loads song and event log data, retrieved from files stored on [AWS S3](https://aws.amazon.com/s3/), into analytics tables corresponding to a [star schema](https://en.wikipedia.org/wiki/Star_schema) design. The analytics tables are stored on S3 as partitoned parquet files.

The goal is to facilitate the Sparkify analytics team in their search for insights on the songs played by their users.


## Database Design and ETL Pipeline
Our schema consists of:
- **Analytics** tables: one fact table (songplays), and four dimension tables (users, songs, artists, and time).  


#### FACT Table

|         **songplays**                         	|
|:-------------------------------------------------:|
| 		  songplay_id  								|
| 		  start_time      	                		|
| 		  user_id            	            		|
| 		  level                      	            |
| 		  song_id           	            		|
| 		  artist_id         	            		|
| 		  session_id        	            		|
| 		  location                   	            |
| 		  user_agent                 	            |
|		  year 										|
|		  month										|


#### DIMENSION Tables

##### 1. users table

|        **users**           	|		
|:-----------------------------:|
| 		 user_id   				|
| 		 first_name  			|
| 		 last_name    			|
| 		 gender               	|
| 		 level                	|


##### 2. songs table

|        **songs**           	|
|:-----------------------------:|
| 		song_id 	  			|
| 		title			       	|
| 		artist_id		 		|
| 		year			        |
| 		duration			    |


##### 3. artists table

|        **artists**             |
|:------------------------------:|
| 		artist_id	   			 |
| 		name		           	 |
| 		location	             |
| 		latitude            	 |
| 		longitude           	 |


##### 4. time table

|        **time**             		|
|:---------------------------------:|
| 		start_time  				|
| 		hour                        |
| 		day                         |
| 		week                        |
| 		month                       |
| 		year                        |
| 		weekday                     |


#### ETL Pipeline

The ETL pipeline consists of the following components:
- **dl.cfg:** The configuration file where we specify the AWS access key credentials.
- **etl.py:** The module that retrieves data from S3, processes the data using Spark, and writes it into the fact and dimension tables, stored as partitioned parquet files, on S3.


##### Usage

1. retrieve, process, and store the sparkify song and event log data in analytics tables on S3

	`$ python etl.py`
	