import tweepy
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import boto3
from botocore.exceptions import NoCredentialsError
import logging
import os
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class TwitterAnalytics:
    def __init__(self):
        """Initialize the Twitter API connection using OAuth credentials"""
        # Twitter API credentials from environment variables
        consumer_key = os.getenv("TWITTER_CONSUMER_KEY")
        consumer_secret = os.getenv("TWITTER_CONSUMER_SECRET")
        access_token = os.getenv("TWITTER_ACCESS_TOKEN")
        access_token_secret = os.getenv("TWITTER_ACCESS_TOKEN_SECRET")
        
        # Initialize tweepy with OAuth 1.0a authentication
        auth = tweepy.OAuth1UserHandler(
            consumer_key, consumer_secret, access_token, access_token_secret
        )
        self.api = tweepy.API(auth, wait_on_rate_limit=True)
        
        # Initialize AWS S3 client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("AWS_SECRET_KEY")
        )
        self.s3_bucket = os.getenv("AWS_S3_BUCKET")
        
        # Date format for file naming
        self.date_str = datetime.now().strftime("%Y%m%d")
    
    def get_user_data(self, username):
        """Fetch details about a specific Twitter user"""
        try:
            user = self.api.get_user(screen_name=username)
            user_data = {
                'id': user.id,
                'screen_name': user.screen_name,
                'name': user.name,
                'description': user.description,
                'followers_count': user.followers_count,
                'friends_count': user.friends_count,
                'statuses_count': user.statuses_count,
                'favorites_count': user.favourites_count,
                'listed_count': user.listed_count,
                'created_at': user.created_at,
                'verified': user.verified
            }
            return user_data
        except tweepy.TweepyException as e:
            logger.error(f"Error fetching user data for {username}: {e}")
            return None
    
    def get_user_tweets(self, username, count=200):
        """Fetch recent tweets from a user"""
        try:
            # Tweepy allows up to 200 tweets per request
            tweets = self.api.user_timeline(screen_name=username, count=count, tweet_mode="extended")
            return tweets
        except tweepy.TweepyException as e:
            logger.error(f"Error fetching tweets for {username}: {e}")
            return []
    
    def analyze_user_engagement(self, username, count=200):
        """Analyze engagement metrics for a user's tweets"""
        tweets = self.get_user_tweets(username, count)
        
        if not tweets:
            return None
        
        # Calculate engagement metrics
        engagement_data = []
        for tweet in tweets:
            # Get basic tweet information
            tweet_data = {
                'id': tweet.id,
                'created_at': tweet.created_at,
                'text': tweet.full_text,
                'retweet_count': tweet.retweet_count,
                'favorite_count': tweet.favorite_count,
                'hashtags': [hashtag['text'] for hashtag in tweet.entities.get('hashtags', [])],
                'mentions': [mention['screen_name'] for mention in tweet.entities.get('user_mentions', [])],
                'hour_of_day': tweet.created_at.hour,
                'day_of_week': tweet.created_at.strftime('%A'),
            }
            
            # Calculate engagement rate = (likes + retweets) / followers_count
            tweet_data['engagement_rate'] = (tweet.favorite_count + tweet.retweet_count) / self.get_user_data(username)['followers_count'] * 100 if self.get_user_data(username)['followers_count'] > 0 else 0
            
            engagement_data.append(tweet_data)
        
        return pd.DataFrame(engagement_data)
    
    def get_trending_topics(self, woeid=1):
        """Get trending topics globally or for a specific location
        Default woeid=1 is for global trends. For US, use woeid=23424977
        """
        try:
            trends = self.api.get_place_trends(id=woeid)
            trend_data = []
            
            for trend in trends[0]['trends']:
                trend_data.append({
                    'name': trend['name'],
                    'url': trend['url'],
                    'promoted_content': trend['promoted_content'],
                    'query': trend['query'],
                    'tweet_volume': trend['tweet_volume']
                })
            
            return pd.DataFrame(trend_data)
        except tweepy.TweepyException as e:
            logger.error(f"Error fetching trending topics: {e}")
            return None
    
    def generate_audience_insights(self, username, follower_sample=100):
        """Generate insights about a user's audience based on a sample of followers"""
        try:
            user_data = self.get_user_data(username)
            if not user_data:
                return None
                
            # Get a sample of follower IDs
            followers = self.api.get_follower_ids(screen_name=username)
            sample_size = min(follower_sample, len(followers))
            follower_sample = np.random.choice(followers, size=sample_size, replace=False)
            
            # Analyze follower demographics
            demographics = {
                'verified_count': 0,
                'total_followers': 0,
                'total_tweets': 0,
                'account_age_days': []
            }
            
            for follower_id in follower_sample:
                try:
                    follower = self.api.get_user(user_id=follower_id)
                    demographics['verified_count'] += 1 if follower.verified else 0
                    demographics['total_followers'] += follower.followers_count
                    demographics['total_tweets'] += follower.statuses_count
                    account_age = (datetime.now() - follower.created_at.replace(tzinfo=None)).days
                    demographics['account_age_days'].append(account_age)
                except tweepy.TweepyException:
                    continue
            
            # Calculate averages
            audience_insights = {
                'verified_percentage': (demographics['verified_count'] / sample_size) * 100 if sample_size > 0 else 0,
                'avg_followers': demographics['total_followers'] / sample_size if sample_size > 0 else 0,
                'avg_tweets': demographics['total_tweets'] / sample_size if sample_size > 0 else 0,
                'avg_account_age_days': sum(demographics['account_age_days']) / len(demographics['account_age_days']) if demographics['account_age_days'] else 0,
                'sample_size': sample_size,
                'followers_count': user_data['followers_count']
            }
            
            return pd.DataFrame([audience_insights])
        except tweepy.TweepyException as e:
            logger.error(f"Error generating audience insights for {username}: {e}")
            return None
    
    def upload_to_s3(self, dataframe, file_name):
        """Upload a dataframe to S3 as CSV"""
        try:
            csv_buffer = dataframe.to_csv(index=False)
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=file_name,
                Body=csv_buffer
            )
            logger.info(f"Successfully uploaded {file_name} to S3 bucket {self.s3_bucket}")
            return True
        except NoCredentialsError:
            logger.error("AWS credentials not found")
            return False
        except Exception as e:
            logger.error(f"Error uploading to S3: {e}")
            return False
    
    def save_as_csv(self, dataframe, file_name):
        """Save a dataframe as a CSV file locally"""
        try:
            dataframe.to_csv(file_name, index=False)
            logger.info(f"Successfully saved {file_name} locally")
            return True
        except Exception as e:
            logger.error(f"Error saving CSV file: {e}")
            return False
    
    def full_analysis_pipeline(self, usernames, save_locally=True, upload_to_aws=True):
        """Run a complete analysis pipeline for a list of Twitter usernames"""
        for username in usernames:
            logger.info(f"Starting analysis for {username}")
            
            # 1. User data
            user_data = self.get_user_data(username)
            if user_data:
                user_df = pd.DataFrame([user_data])
                if save_locally:
                    self.save_as_csv(user_df, f"{username}_profile_{self.date_str}.csv")
                if upload_to_aws:
                    self.upload_to_s3(user_df, f"twitter_analytics/{username}/profile_{self.date_str}.csv")
            
            # 2. Engagement analysis
            engagement_df = self.analyze_user_engagement(username)
            if engagement_df is not None and not engagement_df.empty:
                if save_locally:
                    self.save_as_csv(engagement_df, f"{username}_engagement_{self.date_str}.csv")
                if upload_to_aws:
                    self.upload_to_s3(engagement_df, f"twitter_analytics/{username}/engagement_{self.date_str}.csv")
            
            # 3. Audience insights
            audience_df = self.generate_audience_insights(username)
            if audience_df is not None and not audience_df.empty:
                if save_locally:
                    self.save_as_csv(audience_df, f"{username}_audience_{self.date_str}.csv")
                if upload_to_aws:
                    self.upload_to_s3(audience_df, f"twitter_analytics/{username}/audience_{self.date_str}.csv")
        
        # 4. Trending topics (global)
        trends_df = self.get_trending_topics()
        if trends_df is not None and not trends_df.empty:
            if save_locally:
                self.save_as_csv(trends_df, f"global_trends_{self.date_str}.csv")
            if upload_to_aws:
                self.upload_to_s3(trends_df, f"twitter_analytics/trends/global_{self.date_str}.csv")
        
        logger.info("Analysis pipeline completed")


# Example usage
if __name__ == "__main__":
    twitter = TwitterAnalytics()
    
    # List of Twitter usernames to analyze
    usernames = ["elonmusk", "BarackObama", "BillGates"]
    
    # Run the full analysis pipeline
    twitter.full_analysis_pipeline(usernames)
    
    # Alternative: run specific analyses
    # Get trending topics
    trends = twitter.get_trending_topics()
    if trends is not None:
        twitter.save_as_csv(trends, f"global_trends_{twitter.date_str}.csv")
        twitter.upload_to_s3(trends, f"trends/global_{twitter.date_str}.csv")
    
    # Analyze a specific user
    username = "elonmusk"
    engagement = twitter.analyze_user_engagement(username)
    if engagement is not None:
        twitter.save_as_csv(engagement, f"{username}_engagement_{twitter.date_str}.csv")
        twitter.upload_to_s3(engagement, f"users/{username}/engagement_{twitter.date_str}.csv")