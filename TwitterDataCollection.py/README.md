# TwitterDataCollector
Key Features:

This script provides a comprehensive solution for Twitter data analysis that:

Connects to Twitter's API using OAuth authentication

Uses environment variables for secure credential storage
Initializes the Tweepy library with proper authentication


Gathers user data and content metrics

Retrieves profile information
Collects recent tweets
Analyzes engagement rates


Generates audience insights

Samples followers for demographic analysis
Calculates metrics like average follower count, account age, etc.


Tracks trending topics

Gets current trending hashtags and topics
Includes tweet volume data


Handles AWS integration

Uploads all data to S3 as CSV files
Organizes data in a logical folder structure
Includes error handling for AWS operations


Provides a complete analysis pipeline

Processes multiple usernames in batch
Saves data locally and to AWS
Implements proper logging



To use this script, you'll need to:

Install required packages: pip install tweepy pandas numpy boto3 python-dotenv
Create a .env file with your Twitter API and AWS credentials
Customize the list of usernames to analyze