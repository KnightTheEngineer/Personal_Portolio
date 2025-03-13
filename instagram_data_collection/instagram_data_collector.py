import requests
import pandas as pd
import boto3
from datetime import datetime
import time
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Instagram API credentials
INSTAGRAM_ACCESS_TOKEN = os.getenv('INSTAGRAM_ACCESS_TOKEN')
INSTAGRAM_BUSINESS_ID = os.getenv('INSTAGRAM_BUSINESS_ID')

# AWS credentials
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
AWS_BUCKET_NAME = os.getenv('AWS_BUCKET_NAME')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')

class InstagramAnalyticsTracker:
    def __init__(self, access_token, business_id):
        self.access_token = access_token
        self.business_id = business_id
        self.base_url = "https://graph.facebook.com/v19.0/"
        
    def get_account_info(self):
        """Get basic account information"""
        endpoint = f"{self.base_url}{self.business_id}"
        params = {
            'fields': 'name,username,profile_picture_url,followers_count,follows_count',
            'access_token': self.access_token
        }
        response = requests.get(endpoint, params=params)
        return response.json()
    
    def get_media_list(self, limit=25):
        """Get recent media posts"""
        endpoint = f"{self.base_url}{self.business_id}/media"
        params = {
            'fields': 'id,caption,media_type,media_url,permalink,thumbnail_url,timestamp',
            'limit': limit,
            'access_token': self.access_token
        }
        response = requests.get(endpoint, params=params)
        return response.json().get('data', [])
    
    def get_media_insights(self, media_id):
        """Get insights for a specific media post"""
        endpoint = f"{self.base_url}{media_id}/insights"
        params = {
            'metric': 'engagement,impressions,reach,saved,video_views',
            'access_token': self.access_token
        }
        response = requests.get(endpoint, params=params)
        return response.json().get('data', [])
    
    def get_account_insights(self, period='day', days=30):
        """Get account-level insights"""
        endpoint = f"{self.base_url}{self.business_id}/insights"
        params = {
            'metric': 'audience_gender_age,audience_country,audience_city,follower_count,impressions,reach,profile_views',
            'period': period,
            'days': days,
            'access_token': self.access_token
        }
        response = requests.get(endpoint, params=params)
        return response.json().get('data', [])
    
    def get_stories(self):
        """Get current stories"""
        endpoint = f"{self.base_url}{self.business_id}/stories"
        params = {
            'fields': 'id,caption,media_type,media_url,permalink,timestamp',
            'access_token': self.access_token
        }
        response = requests.get(endpoint, params=params)
        return response.json().get('data', [])
    
    def get_story_insights(self, story_id):
        """Get insights for a specific story"""
        endpoint = f"{self.base_url}{story_id}/insights"
        params = {
            'metric': 'exits,impressions,reach,replies,taps_forward,taps_back',
            'access_token': self.access_token
        }
        response = requests.get(endpoint, params=params)
        return response.json().get('data', [])
    
    def collect_all_data(self):
        """Collect all analytics data and organize into DataFrames"""
        # Get account information
        account_info = self.get_account_info()
        
        # Get account insights
        account_insights = self.get_account_insights()
        
        # Get media posts and their insights
        media_posts = self.get_media_list(limit=50)
        media_data = []
        
        for post in media_posts:
            post_id = post.get('id')
            post_insights = self.get_media_insights(post_id)
            
            insights_dict = {}
            for insight in post_insights:
                insights_dict[insight.get('name')] = insight.get('values')[0].get('value') if insight.get('values') else None
            
            media_data.append({
                'post_id': post_id,
                'caption': post.get('caption'),
                'media_type': post.get('media_type'),
                'permalink': post.get('permalink'),
                'timestamp': post.get('timestamp'),
                'engagement': insights_dict.get('engagement'),
                'impressions': insights_dict.get('impressions'),
                'reach': insights_dict.get('reach'),
                'saved': insights_dict.get('saved'),
                'video_views': insights_dict.get('video_views')
            })
            
            # Respect rate limits - sleep for a short time between requests
            time.sleep(1)
        
        # Get stories and their insights
        stories = self.get_stories()
        story_data = []
        
        for story in stories:
            story_id = story.get('id')
            story_insights = self.get_story_insights(story_id)
            
            insights_dict = {}
            for insight in story_insights:
                insights_dict[insight.get('name')] = insight.get('values')[0].get('value') if insight.get('values') else None
            
            story_data.append({
                'story_id': story_id,
                'media_type': story.get('media_type'),
                'permalink': story.get('permalink'),
                'timestamp': story.get('timestamp'),
                'exits': insights_dict.get('exits'),
                'impressions': insights_dict.get('impressions'),
                'reach': insights_dict.get('reach'),
                'replies': insights_dict.get('replies'),
                'taps_forward': insights_dict.get('taps_forward'),
                'taps_back': insights_dict.get('taps_back')
            })
            
            # Respect rate limits
            time.sleep(1)
        
        # Create DataFrames
        account_df = pd.DataFrame([account_info])
        media_df = pd.DataFrame(media_data)
        story_df = pd.DataFrame(story_data)
        
        return {
            'account': account_df,
            'media': media_df,
            'stories': story_df,
            'timestamp': datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        }
    
    def save_to_csv(self, data_dict, output_folder='output'):
        """Save all DataFrames to CSV files"""
        timestamp = data_dict['timestamp']
        os.makedirs(output_folder, exist_ok=True)
        
        csv_files = {}
        
        for key, df in data_dict.items():
            if key != 'timestamp' and not df.empty:
                filename = f"{output_folder}/{key}_analytics_{timestamp}.csv"
                df.to_csv(filename, index=False)
                csv_files[key] = filename
        
        return csv_files

class AWSUploader:
    def __init__(self, aws_access_key, aws_secret_key, region):
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.region = region
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region
        )
    
    def upload_file(self, file_path, bucket, object_name=None):
        """Upload a file to an S3 bucket"""
        if object_name is None:
            object_name = os.path.basename(file_path)
        
        try:
            self.s3.upload_file(file_path, bucket, object_name)
            return True
        except Exception as e:
            print(f"Error uploading {file_path}: {e}")
            return False

def main():
    # Initialize Instagram analytics tracker
    tracker = InstagramAnalyticsTracker(INSTAGRAM_ACCESS_TOKEN, INSTAGRAM_BUSINESS_ID)
    
    # Collect data
    print("Collecting Instagram analytics data...")
    data = tracker.collect_all_data()
    
    # Save to CSV
    print("Saving data to CSV files...")
    csv_files = tracker.save_to_csv(data)
    
    # Initialize AWS uploader
    aws = AWSUploader(AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION)
    
    # Upload files to S3
    print("Uploading files to AWS S3...")
    for key, file_path in csv_files.items():
        s3_path = f"instagram_analytics/{key}/{os.path.basename(file_path)}"
        success = aws.upload_file(file_path, AWS_BUCKET_NAME, s3_path)
        if success:
            print(f"Successfully uploaded {key} analytics to S3: {s3_path}")
        else:
            print(f"Failed to upload {key} analytics to S3")
    
    print("Done!")

if __name__ == "__main__":
    main()