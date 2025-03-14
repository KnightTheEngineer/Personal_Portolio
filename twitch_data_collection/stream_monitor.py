import datetime
import logging
import json
from twitchAPI.twitch import Twitch
from twitchAPI.oauth import UserAuthenticator

logger = logging.getLogger("twitch_analytics")

# Define Auth Scopes as constants
class AuthScope:
    """
    Constants for Twitch API authentication scopes.
    
    These are used to define the access permissions for the application
    when connecting to the Twitch API.
    """
    CHANNEL_READ_SUBSCRIPTIONS = "channel:read:subscriptions"
    CHANNEL_READ_STREAM_KEY = "channel:read:stream_key"
    MODERATOR_READ_CHATTERS = "moderator:read:chatters"
    CHANNEL_READ_ANALYTICS = "analytics:read:games"
    CHAT_READ = "chat:read"

# Global variables for storing stream metrics and viewer counts
stream_metrics = []
viewer_counts = []

def initialize_twitch(client_id, client_secret, broadcaster_name):
    """
    Initialize Twitch API connection and get broadcaster ID.
    
    Connects to the Twitch API with the provided credentials,
    authenticates with required scopes, and retrieves the 
    broadcaster ID for the specified channel.
    
    Args:
        client_id (str): Twitch API client ID.
        client_secret (str): Twitch API client secret.
        broadcaster_name (str): Name of the broadcaster/channel.
        
    Returns:
        tuple: (twitch_instance, broadcaster_id) - The initialized Twitch API 
               instance and the broadcaster's unique ID.
    """
    twitch = Twitch(client_id, client_secret)
    
    # Set up user authentication with required scopes
    scopes = [
        AuthScope.CHANNEL_READ_SUBSCRIPTIONS,
        AuthScope.CHANNEL_READ_STREAM_KEY,
        AuthScope.MODERATOR_READ_CHATTERS,
        AuthScope.CHANNEL_READ_ANALYTICS,
        AuthScope.CHAT_READ
    ]
    
    # Setup authentication
    auth = UserAuthenticator(twitch, scopes)
    token, refresh_token = auth.authenticate()
    twitch.set_user_authentication(token, scopes, refresh_token)
    
    # Get broadcaster ID
    user_info = twitch.get_users(logins=[broadcaster_name])
    broadcaster_id = user_info['data'][0]['id']
    logger.info(f"Broadcaster ID for {broadcaster_name}: {broadcaster_id}")
    
    return twitch, broadcaster_id

class StreamMonitor:
    """
    Monitors Twitch stream status and collects viewer metrics.
    
    This class checks if a stream is live, tracks viewer counts,
    and records stream events (start/end) with detailed metrics.
    """
    
    def __init__(self, twitch, broadcaster_id, s3_client, aws_bucket_name, broadcaster_name, live_metrics):
        """
        Initialize the StreamMonitor.
        
        Args:
            twitch (Twitch): Initialized Twitch API instance.
            broadcaster_id (str): Unique ID of the broadcaster.
            s3_client (boto3.client): Initialized AWS S3 client.
            aws_bucket_name (str): Name of the S3 bucket for data storage.
            broadcaster_name (str): Name of the broadcaster/channel.
            live_metrics (dict): Dictionary to store real-time metrics for dashboard.
        """
        self.twitch = twitch
        self.broadcaster_id = broadcaster_id
        self.s3_client = s3_client
        self.aws_bucket_name = aws_bucket_name
        self.broadcaster_name = broadcaster_name
        self.live_metrics = live_metrics
    
    async def check_stream_status(self):
        """
        Check if the broadcaster is currently live and update metrics.
        
        Queries the Twitch API to determine stream status, updates live metrics,
        and saves stream data to S3. Handles both stream start and end events,
        and continuously tracks viewer counts during live streams.
        
        Returns:
            None
        
        Raises:
            Exception: Any error that occurs while checking stream status or
                      saving data to S3 will be logged but not raised.
        """
        try:
            stream_info = self.twitch.get_streams(user_id=[self.broadcaster_id])
            timestamp = datetime.datetime.now().isoformat()
            
            status_data = {
                'timestamp': timestamp,
                'is_live': False,
                'viewer_count': 0,
                'game_id': None,
                'stream_id': None,
                'started_at': None
            }
            
            if stream_info['data']:
                # Streamer is live
                stream_data = stream_info['data'][0]
                status_data['is_live'] = True
                status_data['viewer_count'] = stream_data['viewer_count']
                status_data['game_id'] = stream_data['game_id']
                status_data['stream_id'] = stream_data['id']
                status_data['started_at'] = stream_data['started_at']
                
                if not self.live_metrics['is_live']:
                    # Stream just started
                    self._handle_stream_start(stream_data, timestamp)
                else:
                    # Update current metrics
                    self._update_stream_metrics(stream_data, timestamp)
            else:
                # Streamer is not live
                if self.live_metrics['is_live']:
                    # Stream just ended
                    await self._handle_stream_end(timestamp)
            
            # Save stream status to S3 for monitoring
            await self._save_stream_status(status_data)
        
        except Exception as e:
            logger.error(f"Error checking stream status: {str(e)}")
    
    def _handle_stream_start(self, stream_data, timestamp):
        """
        Handle the start of a new stream.
        
        Updates live metrics and records stream start event in S3.
        
        Args:
            stream_data (dict): Stream data from Twitch API.
            timestamp (str): ISO-formatted timestamp of the event.
            
        Returns:
            None
        """
        self.live_metrics['is_live'] = True
        self.live_metrics['stream_started_at'] = stream_data['started_at']
        self.live_metrics['current_viewers'] = stream_data['viewer_count']
        self.live_metrics['peak_viewers'] = stream_data['viewer_count']
        
        logger.info(f"Stream started at {stream_data['started_at']}")
        
        # Add to recent events
        self.live_metrics['recent_events'].append({
            'timestamp': timestamp,
            'type': 'stream',
            'message': f"Stream started at {stream_data['started_at'].split('T')[1][:8]}"
        })
        
        # Immediately save start event to S3
        start_event = {
            'timestamp': timestamp,
            'event_type': 'stream_start',
            'stream_id': stream_data['id'],
            'game_id': stream_data['game_id'],
            'started_at': stream_data['started_at']
        }
        
        from twitch_data_collection.data_storage import save_to_s3
        s3_key = f"{self.broadcaster_name.lower()}/stream_metrics/{datetime.datetime.now().strftime('%Y%m%d')}/stream_start.json"
        save_to_s3(self.s3_client, self.aws_bucket_name, s3_key, json.dumps(start_event))
    
    async def _update_stream_metrics(self, stream_data, timestamp):
        """
        Update metrics for an ongoing stream.
        
        Updates viewer counts, retention data, and other stream metrics
        and saves to S3 at regular intervals.
        
        Args:
            stream_data (dict): Stream data from Twitch API.
            timestamp (str): ISO-formatted timestamp of the update.
            
        Returns:
            None
        """
        global stream_metrics, viewer_counts
        
        # Update current metrics
        self.live_metrics['current_viewers'] = stream_data['viewer_count']
        self.live_metrics['peak_viewers'] = max(self.live_metrics['peak_viewers'], stream_data['viewer_count'])
        
        # Add to viewer retention chart
        self.live_metrics['viewer_retention'].append({
            'timestamp': timestamp,
            'viewer_count': stream_data['viewer_count']
        })
        
        # Keep only the last 60 data points
        if len(self.live_metrics['viewer_retention']) > 60:
            self.live_metrics['viewer_retention'] = self.live_metrics['viewer_retention'][-60:]
        
        # Calculate stream duration in minutes
        stream_duration = 0
        if self.live_metrics['stream_started_at']:
            start_time = datetime.datetime.fromisoformat(
                self.live_metrics['stream_started_at'].replace('Z', '+00:00')
            )
            current_time = datetime.datetime.now()
            stream_duration = (current_time - start_time).total_seconds() / 60
        
        # Add to stream metrics for historical tracking
        stream_metrics.append({
            'timestamp': timestamp,
            'viewer_count': stream_data['viewer_count'],
            'stream_duration': stream_duration,
            'game_id': stream_data['game_id'],
            'stream_id': stream_data['id']
        })
        
        # Add to viewer counts for historical tracking
        viewer_counts.append({
            'timestamp': timestamp,
            'viewer_count': stream_data['viewer_count'],
            'stream_id': stream_data['id']
        })
        
        # Immediately save the current viewer count to S3
        viewer_data = {
            'timestamp': timestamp,
            'viewer_count': stream_data['viewer_count'],
            'stream_id': stream_data['id']
        }
        
        # Save directly to S3
        from twitch_data_collection.data_storage import save_to_s3
        current_time = datetime.datetime.now()
        s3_key = f"{self.broadcaster_name.lower()}/viewer_stats/{current_time.strftime('%Y%m%d')}/viewer_count_{current_time.strftime('%H%M%S')}.json"
        save_to_s3(self.s3_client, self.aws_bucket_name, s3_key, json.dumps(viewer_data))
        
        # Save data in batches for efficiency
        if len(viewer_counts) >= 10:
            from twitch_data_collection.data_storage import save_viewer_stats
            await save_viewer_stats(
                self.s3_client, 
                self.aws_bucket_name, 
                self.broadcaster_name, 
                viewer_counts
            )
        
        if len(stream_metrics) >= 10:
            from twitch_data_collection.data_storage import save_stream_metrics
            await save_stream_metrics(
                self.s3_client, 
                self.aws_bucket_name, 
                self.broadcaster_name, 
                stream_metrics
            )
    
    async def _handle_stream_end(self, timestamp):
        """
        Handle the end of a stream.
        
        Updates metrics, records stream end event, and saves final data to S3.
        
        Args:
            timestamp (str): ISO-formatted timestamp of the event.
            
        Returns:
            None
        """
        global stream_metrics, viewer_counts
        
        self.live_metrics['is_live'] = False
        
        # Calculate stream duration
        duration_minutes = 0
        if self.live_metrics['stream_started_at']:
            start_time = datetime.datetime.fromisoformat(
                self.live_metrics['stream_started_at'].replace('Z', '+00:00')
            )
            end_time = datetime.datetime.now(datetime.timezone.utc)
            duration_minutes = int((end_time - start_time).total_seconds() / 60)
            
            # Add stream end event
            end_event = {
                'timestamp': timestamp,
                'type': 'stream',
                'message': f"Stream ended (Duration: {duration_minutes} minutes)"
            }
            
            self.live_metrics['recent_events'].append(end_event)
            
            # Record stream end event directly in S3
            stream_end_data = {
                'timestamp': timestamp,
                'event_type': 'stream_end',
                'stream_duration_minutes': duration_minutes,
                'peak_viewers': self.live_metrics['peak_viewers'],
                'unique_chatters': self.live_metrics['unique_chatters'],
                'total_chat_messages': self.live_metrics['total_chat_messages']
            }
            
            from twitch_data_collection.data_storage import save_to_s3
            current_date = datetime.datetime.now().strftime('%Y%m%d')
            s3_key = f"{self.broadcaster_name.lower()}/stream_metrics/{current_date}/stream_end.json"
            save_to_s3(self.s3_client, self.aws_bucket_name, s3_key, json.dumps(stream_end_data))
        
        # Save final metrics
        from twitch_data_collection.data_storage import save_viewer_stats, save_stream_metrics, save_chat_metrics
        await save_viewer_stats(
            self.s3_client, 
            self.aws_bucket_name, 
            self.broadcaster_name, 
            viewer_counts
        )
        await save_stream_metrics(
            self.s3_client, 
            self.aws_bucket_name, 
            self.broadcaster_name, 
            stream_metrics
        )
        from twitch_data_collection.chat_handler import chat_messages
        await save_chat_metrics(
            self.s3_client, 
            self.aws_bucket_name, 
            self.broadcaster_name, 
            chat_messages
        )
        
        logger.info("Stream ended, all metrics saved to S3")
    
    async def _save_stream_status(self, status_data):
        """
        Save stream status to S3 for monitoring.
        
        Args:
            status_data (dict): Current stream status data.
            
        Returns:
            None
        """
        date_str = datetime.datetime.now().strftime("%Y%m%d")
        s3_key = f"{self.broadcaster_name.lower()}/status/stream_status_{date_str}.json"
        
        try:
            # Check if file exists
            try:
                from twitch_data_collection.data_storage import get_from_s3
                existing_content = get_from_s3(self.s3_client, self.aws_bucket_name, s3_key)
                
                # Parse existing content and append new status
                try:
                    existing_data = json.loads(existing_content)
                    if isinstance(existing_data, list):
                        existing_data.append(status_data)
                    else:
                        existing_data = [existing_data, status_data]
                except:
                    # If parsing fails, start a new array
                    existing_data = [status_data]
                
                # Save updated content
                from twitch_data_collection.data_storage import save_to_s3
                save_to_s3(self.s3_client, self.aws_bucket_name, s3_key, json.dumps(existing_data))
            except:
                # File doesn't exist, create new
                from twitch_data_collection.data_storage import save_to_s3
                save_to_s3(self.s3_client, self.aws_bucket_name, s3_key, json.dumps([status_data]))
        except Exception as e:
            logger.error(f"Error saving stream status to S3: {str(e)}")