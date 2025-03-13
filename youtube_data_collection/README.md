# YoutubeDataCollection
This code provides a complete solution for tracking YouTube analytics and uploading the data to AWS. Here's what it does:

YouTubeAnalyticsTracker class:

Gets channel statistics (subscribers, views, video count)
Retrieves recent videos from the channel
Collects detailed analytics for each video (views, likes, comments)
Gathers comment data to analyze engagement


DataExporter class:

Exports all collected data to CSV files
Timestamps the files for tracking historical data


AWSUploader class:

Uploads the CSV files to an AWS S3 bucket



To use this code, you'll need:

A YouTube Data API key
Your client's YouTube channel ID
AWS credentials (access key and secret key)
The boto3 and google-api-python-client libraries

You can run this script on a schedule (using cron jobs or AWS Lambda) to regularly track your client's YouTube performance over time.