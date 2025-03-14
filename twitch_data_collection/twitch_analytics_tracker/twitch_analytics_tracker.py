import os
import time
import logging
import threading
import asyncio
import schedule
from dotenv import load_dotenv

# Import components
from twitch_data_collection.chat_handler import setup_chat_connection
from twitch_data_collection.stream_monitor import StreamMonitor
from twitch_data_collection.subscriber_tracker import SubscriberTracker
from twitch_data_collection.data_storage import setup_s3_bucket, initialize_s3_client
from apps.dashboard.flask_app import create_flask_app, start_flask_server
from apps.analytics.reports import generate_daily_report
from apps.analytics.insights import analyze_top_clips

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("twitch_analytics.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("twitch_analytics")

# Twitch API credentials
CLIENT_ID = os.getenv('TWITCH_CLIENT_ID')
CLIENT_SECRET = os.getenv('TWITCH_CLIENT_SECRET')
BROADCASTER_NAME = os.getenv('BROADCASTER_NAME')
TARGET_CHANNEL = os.getenv('TARGET_CHANNEL')

# AWS credentials
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_BUCKET_NAME = os.getenv('AWS_BUCKET_NAME')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')

# Create directories for backups if needed
os.makedirs('data/backup', exist_ok=True)
os.makedirs('static/img', exist_ok=True)
os.makedirs('templates', exist_ok=True)

# Global state
global_state = {
    'twitch': None,
    'chat': None,
    's3_client': None,
    'broadcaster_id': None
}

# Real-time metrics (for dashboard)
live_metrics = {
    'is_live': False,
    'stream_started_at': None,
    'current_viewers': 0,
    'peak_viewers': 0,
    'subscriber_count': 0,
    'new_subs_today': 0,
    'total_chat_messages': 0,
    'chat_messages_per_minute': 0,
    'unique_chatters': 0,
    'viewer_retention': [],
    'chat_activity': [],
    'recent_subscribers': [],
    'recent_events': []
}


class TwitchAnalyticsTracker:
    """
    Main class for tracking Twitch analytics data.
    
    This class coordinates all aspects of the Twitch analytics system, including
    API connections, data collection, storage, and dashboard visualization.
    """
    
    def __init__(self):
        """
        Initialize the TwitchAnalyticsTracker.
        
        Sets up the necessary connections and components for tracking Twitch analytics.
        """
        self.stream_monitor = None
        self.subscriber_tracker = None
        self.initialize_connections()
        
    def initialize_connections(self):
        """
        Initialize connections to Twitch API and AWS.
        
        Sets up authentication with Twitch API, initializes AWS S3 client, 
        and creates component instances for stream monitoring and subscriber tracking.
        
        Returns:
            None
        """
        from twitch_data_collection.stream_monitor import initialize_twitch
        
        logger.info("Initializing Twitch API connection...")
        global_state['twitch'], global_state['broadcaster_id'] = initialize_twitch(
            CLIENT_ID, CLIENT_SECRET, BROADCASTER_NAME
        )
        
        # Initialize AWS S3 connection
        logger.info("Initializing AWS S3 connection...")
        global_state['s3_client'] = initialize_s3_client(
            AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION
        )
        
        # Set up S3 bucket for analytics data
        setup_s3_bucket(
            global_state['s3_client'], 
            AWS_BUCKET_NAME, 
            AWS_REGION, 
            BROADCASTER_NAME
        )
        
        # Initialize components
        self.stream_monitor = StreamMonitor(
            global_state['twitch'],
            global_state['broadcaster_id'],
            global_state['s3_client'],
            AWS_BUCKET_NAME,
            BROADCASTER_NAME,
            live_metrics
        )
        
        self.subscriber_tracker = SubscriberTracker(
            global_state['twitch'],
            global_state['broadcaster_id'],
            global_state['s3_client'],
            AWS_BUCKET_NAME,
            BROADCASTER_NAME,
            live_metrics
        )
        
        logger.info("Initialization complete")
    
    def schedule_tasks(self):
        """
        Schedule recurring tasks for data collection and analysis.
        
        Sets up periodic tasks including stream status checks, subscriber count updates,
        clip analysis, and report generation. Tasks run on separate threads.
        
        Returns:
            None
        """
        # Check stream status every minute
        schedule.every(1).minutes.do(lambda: asyncio.run(self.stream_monitor.check_stream_status()))
        
        # Update subscriber count every 15 minutes
        schedule.every(15).minutes.do(lambda: asyncio.run(self.subscriber_tracker.get_subscriber_count()))
        
        # Analyze top clips once a day
        schedule.every().day.at("04:00").do(lambda: asyncio.run(analyze_top_clips(
            global_state['twitch'],
            global_state['broadcaster_id'],
            global_state['s3_client'],
            AWS_BUCKET_NAME,
            BROADCASTER_NAME
        )))
        
        # Generate daily report at midnight
        schedule.every().day.at("00:01").do(lambda: generate_daily_report(
            global_state['s3_client'],
            AWS_BUCKET_NAME,
            BROADCASTER_NAME
        ))
        
        # Run the scheduler in a separate thread
        def run_scheduler():
            while True:
                schedule.run_pending()
                time.sleep(1)
        
        scheduler_thread = threading.Thread(target=run_scheduler)
        scheduler_thread.daemon = True
        scheduler_thread.start()
        
        logger.info("Scheduled tasks initialized")

    async def run(self):
        """
        Run the Twitch Analytics Tracker.
        
        Connects to Twitch chat, schedules recurring tasks, performs initial checks,
        starts the web dashboard, and keeps the application running.
        
        Returns:
            None
        """
        # Connect to Twitch chat
        global_state['chat'] = await setup_chat_connection(
            global_state['twitch'], 
            TARGET_CHANNEL,
            global_state['s3_client'],
            AWS_BUCKET_NAME,
            BROADCASTER_NAME,
            live_metrics
        )
        
        # Schedule recurring tasks
        self.schedule_tasks()
        
        # Initial checks
        await self.stream_monitor.check_stream_status()
        await self.subscriber_tracker.get_subscriber_count()
        await analyze_top_clips(
            global_state['twitch'],
            global_state['broadcaster_id'],
            global_state['s3_client'],
            AWS_BUCKET_NAME,
            BROADCASTER_NAME
        )
        
        # Create and start Flask server
        flask_app, dash_app = create_flask_app(BROADCASTER_NAME, AWS_BUCKET_NAME, live_metrics)
        start_flask_server(flask_app)
        
        logger.info("Twitch Analytics Tracker is running")
        
        # Keep the chat connection alive
        while True:
            await asyncio.sleep(60)


async def main():
    """
    Main function to start the application.
    
    Initializes and runs the TwitchAnalyticsTracker.
    
    Returns:
        None
    """
    # Initialize the Twitch Analytics Tracker
    tracker = TwitchAnalyticsTracker()
    
    # Run the tracker
    await tracker.run()

if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())