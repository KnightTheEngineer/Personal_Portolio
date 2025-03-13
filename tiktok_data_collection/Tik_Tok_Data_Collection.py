import requests
import pandas as pd
import os
import boto3
from datetime import datetime, timedelta
import time
import json

class TikTokAnalyticsTracker:
    def __init__(self, access_token, aws_access_key, aws_secret_key, aws_bucket_name):
        """
        Initialize the TikTok Analytics Tracker
        
        Args:
            access_token (str): TikTok API access token
            aws_access_key (str): AWS access key
            aws_secret_key (str): AWS secret key
            aws_bucket_name (str): AWS S3 bucket name
        """
        self.access_token = access_token
        self.base_url = "https://open.tiktokapis.com/v2/"
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        
        # AWS Configuration
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )
        self.bucket_name = aws_bucket_name
    
    def get_user_info(self):
        """Fetch basic user information"""
        endpoint = "user/info/"
        fields = ["display_name", "profile_deep_link", "follower_count", "following_count", "likes_count"]
        params = {"fields": ",".join(fields)}
        
        response = requests.get(f"{self.base_url}{endpoint}", headers=self.headers, params=params)
        return response.json()
    
    def get_followers_data(self, date_range=30):
        """Fetch follower growth data for the specified date range"""
        endpoint = "research/user/followers/"
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=date_range)
        
        params = {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d")
        }
        
        response = requests.get(f"{self.base_url}{endpoint}", headers=self.headers, params=params)
        return response.json()
    
    def get_post_analytics(self, post_ids=None, date_range=30):
        """
        Fetch analytics for specific posts or all recent posts
        
        Args:
            post_ids (list): List of post IDs to analyze. If None, fetches recent posts.
            date_range (int): Date range in days for analytics
        """
        # First, get recent videos if post_ids not provided
        if not post_ids:
            posts_endpoint = "video/list/"
            params = {"fields": "id,create_time,share_count,view_count,like_count,comment_count"}
            
            response = requests.get(f"{self.base_url}{posts_endpoint}", headers=self.headers, params=params)
            posts_data = response.json()
            
            if "data" in posts_data and "videos" in posts_data["data"]:
                post_ids = [video["id"] for video in posts_data["data"]["videos"]]
        
        # Now get detailed analytics for each post
        all_post_analytics = []
        
        for post_id in post_ids:
            analytics_endpoint = f"video/data/"
            
            params = {
                "video_id": post_id,
                "fields": "video_id,view_count,like_count,comment_count,share_count,engagement_rate"
            }
            
            response = requests.get(f"{self.base_url}{analytics_endpoint}", 
                                    headers=self.headers, 
                                    params=params)
            
            post_data = response.json()
            if "data" in post_data:
                all_post_analytics.append(post_data["data"])
            
            # Respect rate limits
            time.sleep(0.5)
            
        return all_post_analytics
    
    def get_account_analytics(self, date_range=30):
        """Fetch account-level analytics"""
        endpoint = "research/user/stats/"
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=date_range)
        
        params = {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "fields": "follower_count,profile_view,video_view_count,like_count,comment_count,share_count"
        }
        
        response = requests.get(f"{self.base_url}{endpoint}", headers=self.headers, params=params)
        return response.json()
    
    def generate_report(self, date_range=30):
        """
        Generate a complete analytics report
        
        Args:
            date_range (int): Number of days to include in report
        
        Returns:
            dict: Dictionary containing all report data
        """
        report = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "date_range": date_range,
            "user_info": self.get_user_info(),
            "followers_data": self.get_followers_data(date_range),
            "account_analytics": self.get_account_analytics(date_range),
            "post_analytics": self.get_post_analytics(date_range=date_range)
        }
        
        return report
    
    def save_to_csv(self, report, output_dir="reports"):
        """
        Save report data to CSV files
        
        Args:
            report (dict): Report data dictionary
            output_dir (str): Directory to save CSV files
        
        Returns:
            list: Paths to saved CSV files
        """
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        saved_files = []
        
        # Save user info
        if "user_info" in report and "data" in report["user_info"]:
            user_df = pd.DataFrame([report["user_info"]["data"]])
            user_file = f"{output_dir}/user_info_{timestamp}.csv"
            user_df.to_csv(user_file, index=False)
            saved_files.append(user_file)
        
        # Save followers data
        if "followers_data" in report and "data" in report["followers_data"]:
            followers_df = pd.DataFrame(report["followers_data"]["data"]["followers"])
            followers_file = f"{output_dir}/followers_{timestamp}.csv"
            followers_df.to_csv(followers_file, index=False)
            saved_files.append(followers_file)
        
        # Save account analytics
        if "account_analytics" in report and "data" in report["account_analytics"]:
            account_df = pd.DataFrame(report["account_analytics"]["data"]["stats"])
            account_file = f"{output_dir}/account_analytics_{timestamp}.csv"
            account_df.to_csv(account_file, index=False)
            saved_files.append(account_file)
        
        # Save post analytics
        if "post_analytics" in report and report["post_analytics"]:
            posts_df = pd.DataFrame(report["post_analytics"])
            posts_file = f"{output_dir}/post_analytics_{timestamp}.csv"
            posts_df.to_csv(posts_file, index=False)
            saved_files.append(posts_file)
        
        # Save complete report as JSON
        report_file = f"{output_dir}/complete_report_{timestamp}.json"
        with open(report_file, 'w') as f:
            json.dump(report, f)
        saved_files.append(report_file)
        
        return saved_files
    
    def upload_to_s3(self, file_paths):
        """
        Upload saved CSV files to AWS S3
        
        Args:
            file_paths (list): List of file paths to upload
            
        Returns:
            list: S3 URLs of uploaded files
        """
        uploaded_urls = []
        
        for file_path in file_paths:
            file_name = os.path.basename(file_path)
            
            try:
                self.s3_client.upload_file(
                    file_path,
                    self.bucket_name,
                    f"tiktok_analytics/{file_name}"
                )
                
                url = f"s3://{self.bucket_name}/tiktok_analytics/{file_name}"
                uploaded_urls.append(url)
                print(f"Uploaded {file_name} to {url}")
                
            except Exception as e:
                print(f"Error uploading {file_name}: {str(e)}")
        
        return uploaded_urls
    
    def run_analytics_pipeline(self, date_range=30):
        """
        Run the complete analytics pipeline: fetch data, save to CSV, and upload to S3
        
        Args:
            date_range (int): Number of days to include in report
            
        Returns:
            list: S3 URLs of uploaded files
        """
        print(f"Generating TikTok analytics report for the past {date_range} days...")
        report = self.generate_report(date_range)
        
        print("Saving report to CSV files...")
        saved_files = self.save_to_csv(report)
        
        print("Uploading files to AWS S3...")
        uploaded_urls = self.upload_to_s3(saved_files)
        
        print("Analytics pipeline completed successfully.")
        return uploaded_urls


# Example usage
if __name__ == "__main__":
    # Replace with your actual credentials
    TIKTOK_ACCESS_TOKEN = "your_tiktok_access_token"
    AWS_ACCESS_KEY = "your_aws_access_key"
    AWS_SECRET_KEY = "your_aws_secret_key"
    AWS_BUCKET_NAME = "your-s3-bucket-name"
    
    # Initialize the tracker
    tracker = TikTokAnalyticsTracker(
        access_token=TIKTOK_ACCESS_TOKEN,
        aws_access_key=AWS_ACCESS_KEY,
        aws_secret_key=AWS_SECRET_KEY,
        aws_bucket_name=AWS_BUCKET_NAME
    )
    
    # Run the pipeline for the last 30 days
    s3_urls = tracker.run_analytics_pipeline(date_range=30)
    
    # Print the S3 URLs where the files were uploaded
    print("\nUploaded files:")
    for url in s3_urls:
        print(url)