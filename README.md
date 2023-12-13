# Spotify Playlist Batch Processing ETL Pipeline

## Overview
This project leverages Python and AWS to execute a daily batch processing pipeline for a Spotify playlist Tableau report. Utilizing AWS Lambda triggered by Amazon CloudWatch daily, it extracts data from the Spotify Web API, storing the raw data in an S3 bucket. The process continues with an Amazon EventBridge triggering an AWS Lambda for data transformation in Python upon new arrivals in the S3 bucket.

In the loading stage, we've explored two methods: AWS Glue Crawler, Glue Data Catalog, and Athena for schema inference and SQL analytics; and a serverless Amazon Redshift combined with Tableau for crafting the Spotify playlist report.

## Architecture
<img src="Spotify-Architecture.png">

## Technology used
1. Spotify Web API
2. Python
3. Tableau
4. Amazon Web Service (AWS):
- Cloudwatch
- Eventbridge
- Lambda
- S3
- Glue 
- Anthena
- Redshift Serverless
- IAM



## Tableau report
[Spotify Playlist Rankings: Top 50 Songs](https://public.tableau.com/app/profile/stanley.goh/viz/Spotify_Workbook_17004225392950/Dashboard1)


