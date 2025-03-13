import os
import csv
import datetime
import time
from typing import Dict, List, Any

import googleapiclient.discovery
import googleapiclient.errors
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

class YouTubeAnalyticsTracker:
    def __init__(self, api_key: str, channel_id: str):
        """
        Initialize the YouTube Analytics Tracker.
        
        Args:
            api_key: YouTube Data API key
            channel_id: YouTube channel ID to track
        """
        self.api_key = api_key
        self.channel_id = channel_id
        self.youtube = googleapiclient.discovery.build(
            "youtube", "v3", developerKey=api_key
        )
        
    def get_channel_statistics(self) -> Dict[str, Any]:
        """
        Get basic channel statistics including subscriber count.
        
        Returns:
            Dictionary containing channel statistics
        """
        request = self.youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=self.channel_id
        )
        response = request.execute()
        
        if not response['items']:
            raise ValueError(f"No channel found with ID: {self.channel_id}")
            
        channel_data = response['items'][0]
        stats = channel_data['statistics']
        
        return {
            'timestamp': datetime.datetime.now().isoformat(),
            'subscriberCount': stats.get('subscriberCount', '0'),
            'viewCount': stats.get('viewCount', '0'),
            'videoCount': stats.get('videoCount', '0'),
            'channel_title': channel_data['snippet']['title']
        }
    
    def get_recent_videos(self, max_results: int = 10) -> List[Dict[str, Any]]:
        """
        Get the most recent videos from the channel.
        
        Args:
            max_results: Maximum number of videos to retrieve
            
        Returns:
            List of video data dictionaries
        """
        # Get upload playlist ID (all channel videos)
        request = self.youtube.channels().list(
            part="contentDetails",
            id=self.channel_id
        )
        response = request.execute()
        
        if not response['items']:
            raise ValueError(f"No channel found with ID: {self.channel_id}")
            
        uploads_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        
        # Get videos from uploads playlist
        request = self.youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=uploads_playlist_id,
            maxResults=max_results
        )
        response = request.execute()
        
        videos = []
        for item in response.get('items', []):
            video_id = item['contentDetails']['videoId']
            videos.append({
                'video_id': video_id,
                'title': item['snippet']['title'],
                'published_at': item['snippet']['publishedAt'],
                'thumbnail': item['snippet'].get('thumbnails', {}).get('default', {}).get('url', '')
            })
            
        return videos
    
    def get_video_analytics(self, video_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Get detailed analytics for specified videos.
        
        Args:
            video_ids: List of YouTube video IDs
            
        Returns:
            List of video analytics dictionaries
        """
        if not video_ids:
            return []
            
        # Make a batch request for efficiency
        videos_request = self.youtube.videos().list(
            part="statistics,snippet",
            id=','.join(video_ids)
        )
        videos_response = videos_request.execute()
        
        video_analytics = []
        for item in videos_response.get('items', []):
            video_id = item['id']
            statistics = item['statistics']
            
            analytics = {
                'video_id': video_id,
                'title': item['snippet']['title'],
                'published_at': item['snippet']['publishedAt'],
                'view_count': statistics.get('viewCount', '0'),
                'like_count': statistics.get('likeCount', '0'),
                'comment_count': statistics.get('commentCount', '0'),
                'timestamp': datetime.datetime.now().isoformat()
            }
            video_analytics.append(analytics)
            
        return video_analytics
    
    def get_comment_engagement(self, video_id: str, max_results: int = 20) -> List[Dict[str, Any]]:
        """
        Get recent comments for a video to analyze engagement.
        
        Args:
            video_id: YouTube video ID
            max_results: Maximum number of comments to retrieve
            
        Returns:
            List of comment data dictionaries
        """
        try:
            request = self.youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=max_results,
                order="relevance"
            )
            response = request.execute()
            
            comments = []
            for item in response.get('items', []):
                comment = item['snippet']['topLevelComment']['snippet']
                comments.append({
                    'author': comment['authorDisplayName'],
                    'text': comment['textDisplay'],
                    'like_count': comment['likeCount'],
                    'published_at': comment['publishedAt']
                })
                
            return comments
        except googleapiclient.errors.HttpError as e:
            # Comments might be disabled for this video
            print(f"Could not retrieve comments for video {video_id}: {e}")
            return []


class DataExporter:
    def __init__(self, output_dir: str = 'data'):
        """
        Initialize the data exporter.
        
        Args:
            output_dir: Directory to save CSV files
        """
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
    
    def export_to_csv(self, data: List[Dict[str, Any]], filename: str) -> str:
        """
        Export data to a CSV file.
        
        Args:
            data: List of dictionaries to export
            filename: Name of the CSV file (without .csv extension)
            
        Returns:
            Full path to the created CSV file
        """
        if not data:
            print(f"No data to export for {filename}")
            return None
            
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        full_filename = f"{filename}_{timestamp}.csv"
        file_path = os.path.join(self.output_dir, full_filename)
        
        with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = data[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for row in data:
                writer.writerow(row)
                
        print(f"Data exported to {file_path}")
        return file_path


class AWSUploader:
    def __init__(self, aws_access_key: str, aws_secret_key: str, region: str = 'us-east-1'):
        """
        Initialize the AWS uploader.
        
        Args:
            aws_access_key: AWS access key ID
            aws_secret_key: AWS secret access key
            region: AWS region name
        """
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region
        )
    
    def upload_file(self, file_path: str, bucket_name: str, object_name: str = None) -> bool:
        """
        Upload a file to an S3 bucket.
        
        Args:
            file_path: Path to the file to upload
            bucket_name: Name of the S3 bucket
            object_name: S3 object name (if None, file_name is used)
            
        Returns:
            True if upload was successful, False otherwise
        """
        if object_name is None:
            object_name = os.path.basename(file_path)
            
        try:
            self.s3_client.upload_file(file_path, bucket_name, object_name)
            print(f"Successfully uploaded {file_path} to {bucket_name}/{object_name}")
            return True
        except FileNotFoundError:
            print(f"File {file_path} not found")
            return False
        except NoCredentialsError:
            print("AWS credentials not available")
            return False
        except ClientError as e:
            print(f"AWS error: {e}")
            return False


def main():
    # Configuration (should be loaded from environment variables or config file in production)
    YOUTUBE_API_KEY = "YOUR_YOUTUBE_API_KEY"
    CHANNEL_ID = "YOUR_CHANNEL_ID"
    AWS_ACCESS_KEY = "YOUR_AWS_ACCESS_KEY"
    AWS_SECRET_KEY = "YOUR_AWS_SECRET_KEY"
    AWS_BUCKET_NAME = "your-analytics-bucket"
    
    # Initialize components
    tracker = YouTubeAnalyticsTracker(YOUTUBE_API_KEY, CHANNEL_ID)
    exporter = DataExporter()
    uploader = AWSUploader(AWS_ACCESS_KEY, AWS_SECRET_KEY)
    
    try:
        # Get channel statistics
        channel_stats = tracker.get_channel_statistics()
        channel_stats_list = [channel_stats]  # Convert to list for CSV export
        channel_stats_file = exporter.export_to_csv(channel_stats_list, "channel_statistics")
        
        # Get recent videos
        recent_videos = tracker.get_recent_videos(max_results=20)
        videos_file = exporter.export_to_csv(recent_videos, "recent_videos")
        
        # Get video IDs from recent videos
        video_ids = [video['video_id'] for video in recent_videos]
        
        # Get video analytics
        video_analytics = tracker.get_video_analytics(video_ids)
        analytics_file = exporter.export_to_csv(video_analytics, "video_analytics")
        
        # Get comments for the most recent video (if available)
        if video_ids:
            comments = tracker.get_comment_engagement(video_ids[0])
            if comments:
                comments_file = exporter.export_to_csv(comments, f"comments_{video_ids[0]}")
                if comments_file:
                    uploader.upload_file(comments_file, AWS_BUCKET_NAME)
        
        # Upload all files to AWS
        if channel_stats_file:
            uploader.upload_file(channel_stats_file, AWS_BUCKET_NAME)
        if videos_file:
            uploader.upload_file(videos_file, AWS_BUCKET_NAME)
        if analytics_file:
            uploader.upload_file(analytics_file, AWS_BUCKET_NAME)
            
        print("Analytics tracking and upload completed successfully!")
        
    except Exception as e:
        print(f"Error during execution: {e}")


if __name__ == "__main__":
    main()