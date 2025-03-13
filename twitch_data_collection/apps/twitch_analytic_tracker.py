import os
import time
import json
import logging
import datetime
import pandas as pd
import numpy as np
import requests
import boto3
import io
import schedule
import threading
import asyncio
from dotenv import load_dotenv
from twitchAPI.twitch import Twitch
from twitchAPI.oauth import UserAuthenticator
# Instead of importing from twitchAPI.types, define constants directly
from twitchAPI.chat import Chat, EventData, ChatMessage
import websockets
from flask import Flask, render_template, jsonify, request, send_from_directory
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import load_figure_template

# Define Auth Scopes as constants instead of importing from twitchAPI.types
class AuthScope:
    CHANNEL_READ_SUBSCRIPTIONS = "channel:read:subscriptions"
    CHANNEL_READ_STREAM_KEY = "channel:read:stream_key"
    MODERATOR_READ_CHATTERS = "moderator:read:chatters"
    CHANNEL_READ_ANALYTICS = "analytics:read:games"
    CHAT_READ = "chat:read"

# Define Chat Events as constants instead of importing from twitchAPI.types
class ChatEvent:
    MESSAGE = "message"
    SUB = "subscription"
    RAID = "raid"

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

# Initialize data structures for real-time tracking
chat_messages = []
viewer_counts = []
subscriber_events = []
stream_metrics = []
channel_analytics = {}

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

# Global objects
twitch = None
chat = None
s3_client = None
broadcaster_id = None

# Create Flask app for the web dashboard
flask_app = Flask(__name__, 
              static_folder='static',
              template_folder='templates')

# Create Dash app for the interactive dashboard
dash_app = dash.Dash(
    __name__,
    server=flask_app,
    url_base_pathname='/dashboard/',
    external_stylesheets=[dbc.themes.DARKLY]
)
load_figure_template('darkly')

# Set up the Dash layout
dash_app.layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H1(f"{BROADCASTER_NAME} - Twitch Analytics", className="text-center mb-4"),
            html.Div(id="stream-status-badge", className="text-center mb-3"),
        ], width=12)
    ]),
    
    # Rest of the layout remains the same as before
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Current Stream Stats"),
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            html.H5("Current Viewers"),
                            html.H3(id="current-viewers", children="0"),
                        ], width=4),
                        dbc.Col([
                            html.H5("Peak Viewers"),
                            html.H3(id="peak-viewers", children="0"),
                        ], width=4),
                        dbc.Col([
                            html.H5("Stream Duration"),
                            html.H3(id="stream-duration", children="0:00"),
                        ], width=4),
                    ]),
                ]),
            ], className="mb-4"),
        ], width=12),
    ]),
    
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Subscriber Metrics"),
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            html.H5("Total Subscribers"),
                            html.H3(id="total-subscribers", children="0"),
                        ], width=6),
                        dbc.Col([
                            html.H5("New Subs Today"),
                            html.H3(id="new-subs-today", children="0"),
                        ], width=6),
                    ]),
                    html.Hr(),
                    html.H5("Recent Subscribers"),
                    html.Div(id="recent-subscribers-list"),
                ]),
            ]),
        ], width=6),
        
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Chat Engagement"),
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            html.H5("Chat Messages"),
                            html.H3(id="total-chat-messages", children="0"),
                        ], width=6),
                        dbc.Col([
                            html.H5("Messages/Minute"),
                            html.H3(id="messages-per-minute", children="0"),
                        ], width=6),
                    ]),
                    html.Hr(),
                    html.H5("Unique Chatters"),
                    html.H3(id="unique-chatters", children="0"),
                ]),
            ]),
        ], width=6),
    ], className="mb-4"),
    
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Viewer Retention"),
                dbc.CardBody([
                    dcc.Graph(id="viewer-retention-graph"),
                ]),
            ]),
        ], width=6),
        
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Chat Activity"),
                dbc.CardBody([
                    dcc.Graph(id="chat-activity-graph"),
                ]),
            ]),
        ], width=6),
    ], className="mb-4"),
    
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Recent Events"),
                dbc.CardBody([
                    html.Div(id="recent-events-list"),
                ]),
            ]),
        ], width=12),
    ], className="mb-4"),
    
    dcc.Interval(
        id='interval-component',
        interval=5*1000,  # in milliseconds (5 seconds)
        n_intervals=0
    ),
    
    dbc.Row([
        dbc.Col([
            html.Hr(),
            html.H4("Historical Analysis", className="text-center mb-4"),
        ], width=12),
    ]),
    
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Select Date Range"),
                dbc.CardBody([
                    dcc.DatePickerRange(
                        id='date-picker-range',
                        start_date=datetime.datetime.now().date() - datetime.timedelta(days=30),
                        end_date=datetime.datetime.now().date(),
                        display_format='YYYY-MM-DD'
                    ),
                    html.Button('Update Reports', id='update-reports-button', className="mt-2 btn btn-primary"),
                ]),
            ]),
        ], width=12),
    ], className="mb-4"),
    
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Subscriber Growth"),
                dbc.CardBody([
                    dcc.Graph(id="subscriber-growth-graph"),
                ]),
            ]),
        ], width=6),
        
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Stream Performance"),
                dbc.CardBody([
                    dcc.Graph(id="stream-performance-graph"),
                ]),
            ]),
        ], width=6),
    ], className="mb-4"),
    
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Viewer Count vs. Chat Activity"),
                dbc.CardBody([
                    dcc.Graph(id="viewers-chat-correlation-graph"),
                ]),
            ]),
        ], width=12),
    ], className="mb-4"),
    
], fluid=True)

# Dash callbacks remain the same as in the original code

class TwitchAnalyticsTracker:
    def __init__(self):
        self.initialize_connections()
        
    def initialize_connections(self):
        """Initialize connections to Twitch API and AWS"""
        global twitch, s3_client, broadcaster_id
        
        logger.info("Initializing Twitch API connection...")
        twitch = Twitch(CLIENT_ID, CLIENT_SECRET)
        
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
        user_info = twitch.get_users(logins=[BROADCASTER_NAME])
        broadcaster_id = user_info['data'][0]['id']
        logger.info(f"Broadcaster ID for {BROADCASTER_NAME}: {broadcaster_id}")
        
        # Initialize AWS S3 connection
        logger.info("Initializing AWS S3 connection...")
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=AWS_REGION
        )
        
        # Set up S3 bucket for analytics data
        self.setup_s3_bucket()
        
        logger.info("Initialization complete")
    
    def setup_s3_bucket(self):
        """Set up S3 bucket structure with necessary folders"""
        try:
            # Check if bucket exists, if not create it
            try:
                s3_client.head_bucket(Bucket=AWS_BUCKET_NAME)
                logger.info(f"S3 bucket {AWS_BUCKET_NAME} exists")
            except:
                logger.info(f"Creating S3 bucket {AWS_BUCKET_NAME}")
                if AWS_REGION == 'us-east-1':
                    s3_client.create_bucket(Bucket=AWS_BUCKET_NAME)
                else:
                    s3_client.create_bucket(
                        Bucket=AWS_BUCKET_NAME,
                        CreateBucketConfiguration={'LocationConstraint': AWS_REGION}
                    )
            
            # Create folder structure in S3
            folders = [
                f"{BROADCASTER_NAME.lower()}/subscribers/",
                f"{BROADCASTER_NAME.lower()}/chat_metrics/",
                f"{BROADCASTER_NAME.lower()}/viewer_stats/",
                f"{BROADCASTER_NAME.lower()}/stream_metrics/",
                f"{BROADCASTER_NAME.lower()}/reports/",
                f"{BROADCASTER_NAME.lower()}/raw_events/"
            ]
            
            for folder in folders:
                s3_client.put_object(Bucket=AWS_BUCKET_NAME, Key=folder)
            
            logger.info(f"S3 folder structure set up for {BROADCASTER_NAME}")
            
        except Exception as e:
            logger.error(f"Error setting up S3 bucket: {str(e)}")
    
    async def connect_to_chat(self):
        """Connect to Twitch chat and set up event handlers"""
        global chat
        
        logger.info(f"Connecting to Twitch chat for channel: {TARGET_CHANNEL}")
        chat = await Chat(twitch, initial_channels=[TARGET_CHANNEL])
        
        # Register event handlers
        chat.register_event(ChatEvent.MESSAGE, self.on_chat_message)
        chat.register_event(ChatEvent.SUB, self.on_subscription)
        chat.register_event(ChatEvent.RAID, self.on_raid)
        
        logger.info("Successfully connected to Twitch chat")
    
    async def on_chat_message(self, event_data: EventData, message: ChatMessage):
        """Handle chat messages with immediate AWS storage"""
        timestamp = datetime.datetime.now().isoformat()
        
        # Create message data
        message_data = {
            'timestamp': timestamp,
            'channel': message.channel.name,
            'sender': message.sender.name,
            'message': message.text,
            'is_subscriber': message.sender.is_subscriber,
            'is_mod': message.sender.is_mod,
            'badges': ','.join([badge.name for badge in message.badges]) if message.badges else '',
            'message_id': message.id
        }
        
        # Add to chat messages list
        chat_messages.append(message_data)
        
        # Immediately save directly to S3
        await self.save_event_to_s3('chat_message', message_data)
        
        # Update real-time metrics
        live_metrics['total_chat_messages'] += 1
        
        # Add unique chatter if not seen before
        unique_chatters = set(msg['sender'] for msg in chat_messages)
        live_metrics['unique_chatters'] = len(unique_chatters)
        
        # Add to recent events
        live_metrics['recent_events'].append({
            'timestamp': timestamp,
            'type': 'chat',
            'message': f"{message.sender.name}: {message.text[:50]}{'...' if len(message.text) > 50 else ''}"
        })
        
        # Update chat activity for the dashboard
        # Group by minute for the chart
        current_minute = datetime.datetime.now().replace(second=0, microsecond=0).isoformat()
        
        # Find or create a minute entry
        minute_exists = False
        for minute_data in live_metrics['chat_activity']:
            if minute_data['timestamp'] == current_minute:
                minute_data['message_count'] += 1
                minute_exists = True
                break
                
        if not minute_exists:
            live_metrics['chat_activity'].append({
                'timestamp': current_minute,
                'message_count': 1
            })
            
            # Keep only the last 30 minutes
            if len(live_metrics['chat_activity']) > 30:
                live_metrics['chat_activity'] = live_metrics['chat_activity'][-30:]
        
        # Calculate messages per minute
        if live_metrics['is_live'] and live_metrics['stream_started_at']:
            total_minutes = max(1, (datetime.datetime.now() - datetime.datetime.fromisoformat(
                live_metrics['stream_started_at'].replace('Z', '+00:00')
            )).total_seconds() / 60)
            
            live_metrics['chat_messages_per_minute'] = live_metrics['total_chat_messages'] / total_minutes
        
        # Save chat metrics every 50 messages
        if len(chat_messages) >= 50:
            await self.save_chat_metrics()
    
    async def on_subscription(self, event_data: EventData):
        """Handle subscription events with immediate AWS storage"""
        timestamp = datetime.datetime.now().isoformat()
        
        # Get subscription details
        sub_data = {
            'timestamp': timestamp,
            'channel': event_data.channel.name,
            'user': event_data.user.name,
            'tier': event_data.sub_plan,
            'is_gift': event_data.is_gift,
            'total_months': event_data.cumulative_months
        }
        
        # Add to subscriber events list
        subscriber_events.append(sub_data)
        
        # Immediately save to S3
        await self.save_event_to_s3('subscription', sub_data)
        
        # Update real-time metrics
        live_metrics['new_subs_today'] += 1
        live_metrics['recent_subscribers'].append(sub_data)
        
        # Keep only the last 20 subscribers
        if len(live_metrics['recent_subscribers']) > 20:
            live_metrics['recent_subscribers'] = live_metrics['recent_subscribers'][-20:]
        
        # Add to recent events
        tier_name = "Tier 1"
        if sub_data['tier'] == "2000":
            tier_name = "Tier 2"
        elif sub_data['tier'] == "3000":
            tier_name = "Tier 3"
            
        event_message = f"{sub_data['user']} subscribed ({tier_name})"
        if sub_data['is_gift']:
            event_message += " (gifted)"
        if sub_data['total_months'] > 1:
            event_message += f" - {sub_data['total_months']} months"
            
        live_metrics['recent_events'].append({
            'timestamp': timestamp,
            'type': 'subscription',
            'message': event_message
        })
        
        # Keep only the last 100 events
        if len(live_metrics['recent_events']) > 100:
            live_metrics['recent_events'] = live_metrics['recent_events'][-100:]
        
        logger.info(f"New subscription: {event_data.user.name}")
        
        # Immediately save subscriber data to S3
        await self.save_subscriber_data()
    
    async def on_raid(self, event_data: EventData):
        """Handle raid events with immediate AWS storage"""
        timestamp = datetime.datetime.now().isoformat()
        
        # Get raid details
        raid_data = {
            'timestamp': timestamp,
            'channel': event_data.channel.name,
            'raider': event_data.raider.name,
            'viewer_count': event_data.viewer_count
        }
        
        # Immediately save to S3
        await self.save_event_to_s3('raid', raid_data)
        
        # Add to recent events
        live_metrics['recent_events'].append({
            'timestamp': timestamp,
            'type': 'raid',
            'message': f"{raid_data['raider']} raided with {raid_data['viewer_count']} viewers"
        })
        
        # Keep only the last 100 events
        if len(live_metrics['recent_events']) > 100:
            live_metrics['recent_events'] = live_metrics['recent_events'][-100:]
        
        logger.info(f"Raid received from {raid_data['raider']} with {raid_data['viewer_count']} viewers")

    async def save_event_to_s3(self, event_type, event_data):
        """Save event data directly to S3"""
        try:
            timestamp = datetime.datetime.now()
            date_str = timestamp.strftime("%Y%m%d")
            hour_str = timestamp.strftime("%H")
            
            # Create a unique key for this event
            event_id = f"{int(timestamp.timestamp() * 1000)}_{hash(str(event_data))}"
            s3_key = f"{BROADCASTER_NAME.lower()}/raw_events/{date_str}/{hour_str}/{event_type}_{event_id}.json"
            
            # Convert data to JSON and save directly to S3
            json_data = json.dumps(event_data)
            s3_client.put_object(
                Bucket=AWS_BUCKET_NAME,
                Key=s3_key,
                Body=json_data,
                ContentType='application/json'
            )
            
            logger.debug(f"Saved {event_type} event to S3: {s3_key}")
            
        except Exception as e:
            logger.error(f"Error saving {event_type} event to S3: {str(e)}")
            # Create a backup locally just in case
            try:
                os.makedirs(f'data/backup/{date_str}', exist_ok=True)
                with open(f'data/backup/{date_str}/{event_type}_{event_id}.json', 'w') as f:
                    json.dump(event_data, f)
            except:
                pass

    async def save_chat_metrics(self):
        """Save chat message data directly to S3"""
        if not chat_messages:
            return
        
        timestamp = datetime.datetime.now()
        date_str = timestamp.strftime("%Y%m%d")
        hour_str = timestamp.strftime("%H")
        
        # Prepare data for metrics
        unique_chatters = set(msg['sender'] for msg in chat_messages)
        total_messages = len(chat_messages)
        
        # Calculate chat velocity (messages per minute)
        if len(chat_messages) >= 2:
            first_msg_time = datetime.datetime.fromisoformat(chat_messages[0]['timestamp'])
            last_msg_time = datetime.datetime.fromisoformat(chat_messages[-1]['timestamp'])
            duration_minutes = max(1, (last_msg_time - first_msg_time).total_seconds() / 60)
            chat_velocity = total_messages / duration_minutes
        else:
            chat_velocity = 0
        
        # Create metrics data
        chat_metrics = {
            'timestamp': timestamp.isoformat(),
            'message_count': total_messages,
            'unique_chatters': len(unique_chatters),
            'chat_velocity': chat_velocity,
            'subscriber_ratio': sum(1 for msg in chat_messages if msg['is_subscriber']) / total_messages if total_messages > 0 else 0,
            'mod_message_count': sum(1 for msg in chat_messages if msg['is_mod']),
            'timestamp_min': min(msg['timestamp'] for msg in chat_messages),
            'timestamp_max': max(msg['timestamp'] for msg in chat_messages)
        }
        
        # Save metrics directly to S3
        metrics_key = f"{BROADCASTER_NAME.lower()}/chat_metrics/{date_str}/metrics_{timestamp.strftime('%H%M%S')}.json"
        s3_client.put_object(
            Bucket=AWS_BUCKET_NAME,
            Key=metrics_key,
            Body=json.dumps(chat_metrics),
            ContentType='application/json'
        )
        
        # Save the raw chat messages batch
        batch_key = f"{BROADCASTER_NAME.lower()}/chat_metrics/{date_str}/raw_batch_{timestamp.strftime('%H%M%S')}.json"
        
        # For larger datasets, stream directly to S3
        if len(chat_messages) > 1000:
            # Stream JSON data to S3
            buffer = io.BytesIO()
            for message in chat_messages:
                buffer.write((json.dumps(message) + '\n').encode('utf-8'))
            
            buffer.seek(0)
            s3_client.put_object(
                Bucket=AWS_BUCKET_NAME,
                Key=batch_key,
                Body=buffer.getvalue(),
                ContentType='application/json'
            )
        else:
            # For smaller batches, save directly
            s3_client.put_object(
                Bucket=AWS_BUCKET_NAME,
                Key=batch_key,
                Body=json.dumps(chat_messages),
                ContentType='application/json'
            )
        
        # Also save as CSV for analytics tools
        csv_data = pd.DataFrame(chat_messages)
        csv_buffer = io.StringIO()
        csv_data.to_csv(csv_buffer, index=False)
        
        csv_key = f"{BROADCASTER_NAME.lower()}/chat_metrics/{date_str}/messages_{timestamp.strftime('%H%M%S')}.csv"
        s3_client.put_object(
            Bucket=AWS_BUCKET_NAME,
            Key=csv_key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        
        # Save a continuous daily record by appending to a consolidated file
        try:
            # Check if daily file exists
            daily_key = f"{BROADCASTER_NAME.lower()}/chat_metrics/daily_{date_str}.csv"
            
            try:
                # Try to get the existing file
                existing_obj = s3_client.get_object(Bucket=AWS_BUCKET_NAME, Key=daily_key)
                daily_exists = True
            except:
                daily_exists = False
            
            # Create a new CSV buffer with header only if it's a new file
            daily_buffer = io.StringIO()
            csv_data.to_csv(daily_buffer, index=False, header=not daily_exists)
            
            # If the file exists, append to it
            if daily_exists:
                s3_client.put_object(
                    Bucket=AWS_BUCKET_NAME,
                    Key=daily_key,
                    Body=daily_buffer.getvalue().split("\n", 1)[1],  # Skip header line
                    ContentType='text/csv',
                    Metadata={'append': 'true'}
                )
            else:
                # New file
                s3_client.put_object(
                    Bucket=AWS_BUCKET_NAME,
                    Key=daily_key,
                    Body=daily_buffer.getvalue(),
                    ContentType='text/csv'
                )
        except Exception as e:
            logger.error(f"Error appending to daily chat file: {str(e)}")
        
        # Clear processed messages
        chat_messages.clear()
        
        logger.info(f"Saved chat metrics directly to S3")

    async def save_subscriber_data(self):
        """Save subscriber data directly to S3"""
        if not subscriber_events:
            return
        
        timestamp = datetime.datetime.now()
        date_str = timestamp.strftime("%Y%m%d")
        
        # Save to S3 as JSON
        s3_key = f"{BROADCASTER_NAME.lower()}/subscribers/{date_str}/subscribers_{timestamp.strftime('%H%M%S')}.json"
        s3_client.put_object(
            Bucket=AWS_BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(subscriber_events),
            ContentType='application/json'
        )
        
        # Also save as CSV for analytics tools
        subs_df = pd.DataFrame(subscriber_events)
        csv_buffer = io.StringIO()
        subs_df.to_csv(csv_buffer, index=False)
        
        csv_key = f"{BROADCASTER_NAME.lower()}/subscribers/{date_str}/subscribers_{timestamp.strftime('%H%M%S')}.csv"
        s3_client.put_object(
            Bucket=AWS_BUCKET_NAME,
            Key=csv_key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        
        # Also append to daily file
        try:
            # Check if daily file exists
            daily_key = f"{BROADCASTER_NAME.lower()}/subscribers/daily_{date_str}.csv"
            
            try:
                # Try to get the existing file
                existing_obj = s3_client.get_object(Bucket=AWS_BUCKET_NAME, Key=daily_key)
                daily_exists = True
            except:
                daily_exists = False
            
            # Create a new CSV buffer with header only if it's a new file
            daily_buffer = io.StringIO()
            subs_df.to_csv(daily_buffer, index=False, header=not daily_exists)
            
            # If the file exists, append to it
            if daily_exists:
                s3_client.put_object(
                    Bucket=AWS_BUCKET_NAME,
                    Key=daily_key,
                    Body=daily_buffer.getvalue().split("\n", 1)[1],  # Skip header line
                    ContentType='text/csv',
                    Metadata={'append': 'true'}
                )
            else:
                # New file
                s3_client.put_object(
                    Bucket=AWS_BUCKET_NAME,
                    Key=daily_key,
                    Body=daily_buffer.getvalue(),
                    ContentType='text/csv'
                )
        except Exception as e:
            logger.error(f"Error appending to daily subscribers file: {str(e)}")
        
        # Clear processed events
        subscriber_events.clear()
        
        logger.info(f"Saved subscriber data directly to S3")

    async def save_viewer_stats(self):
        """Save viewer statistics directly to S3"""
        if not viewer_counts:
            return
        
        timestamp = datetime.datetime.now()
        date_str = timestamp.strftime("%Y%m%d")
        
        # Save to S3 as JSON
        s3_key = f"{BROADCASTER_NAME.lower()}/viewer_stats/{date_str}/viewers_{timestamp.strftime('%H%M%S')}.json"
        s3_client.put_object(
            Bucket=AWS_BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(viewer_counts),
            ContentType='application/json'
        )
        
        # Also save as CSV for analytics tools
        viewer_df = pd.DataFrame(viewer_counts)
        csv_buffer = io.StringIO()
        viewer_df.to_csv(csv_buffer, index=False)
        
        csv_key = f"{BROADCASTER_NAME.lower()}/viewer_stats/{date_str}/viewers_{timestamp.strftime('%H%M%S')}.csv"
        s3_client.put_object(
            Bucket=AWS_BUCKET_NAME,
            Key=csv_key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        
        # Also append to daily file
        try:
            # Check if daily file exists
            daily_key = f"{BROADCASTER_NAME.lower()}/viewer_stats/daily_{date_str}.csv"
            
            try:
                # Try to get the existing file
                existing_obj = s3_client.get_object(Bucket=AWS_BUCKET_NAME, Key=daily_key)
                daily_exists = True
            except:
                daily_exists = False
            
            # Create a new CSV buffer with header only if it's a new file
            daily_buffer = io.StringIO()
            viewer_df.to_csv(daily_buffer, index=False, header=not daily_exists)
            
            # If the file exists, append to it
            if daily_exists:
                s3_client.put_object(
                    Bucket=AWS_BUCKET_NAME,
                    Key=daily_key,
                    Body=daily_buffer.getvalue().split("\n", 1)[1],  # Skip header line
                    ContentType='text/csv',
                    Metadata={'append': 'true'}
                )
            else:
                # New file
                s3_client.put_object(
                    Bucket=AWS_BUCKET_NAME,
                    Key=daily_key,
                    Body=daily_buffer.getvalue(),
                    ContentType='text/csv'
                )
        except Exception as e:
            logger.error(f"Error appending to daily viewer stats file: {str(e)}")
        
        # Clear processed viewer counts
        viewer_counts.clear()
        
        logger.info(f"Saved viewer statistics directly to S3")

    async def save_stream_metrics(self):
        """Save stream metrics directly to S3"""
        if not stream_metrics:
            return
        
        timestamp = datetime.datetime.now()
        date_str = timestamp.strftime("%Y%m%d")
        
        # Save to S3 as JSON
        s3_key = f"{BROADCASTER_NAME.lower()}/stream_metrics/{date_str}/metrics_{timestamp.strftime('%H%M%S')}.json"
        s3_client.put_object(
            Bucket=AWS_BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(stream_metrics),
            ContentType='application/json'
        )
        
        # Also save as CSV for analytics tools
        metrics_df = pd.DataFrame(stream_metrics)
        csv_buffer = io.StringIO()
        metrics_df.to_csv(csv_buffer, index=False)
        
        csv_key = f"{BROADCASTER_NAME.lower()}/stream_metrics/{date_str}/metrics_{timestamp.strftime('%H%M%S')}.csv"
        s3_client.put_object(
            Bucket=AWS_BUCKET_NAME,
            Key=csv_key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        
        # Also append to daily file
        try:
            # Check if daily file exists
            daily_key = f"{BROADCASTER_NAME.lower()}/stream_metrics/daily_{date_str}.csv"
            
            try:
                # Try to get the existing file
                existing_obj = s3_client.get_object(Bucket=AWS_BUCKET_NAME, Key=daily_key)
                daily_exists = True
            except:
                daily_exists = False
            
            # Create a new CSV buffer with header only if it's a new file
            daily_buffer = io.StringIO()
            metrics_df.to_csv(daily_buffer, index=False, header=not daily_exists)
            
            # If the file exists, append to it
            if daily_exists:
                s3_client.put_object(
                    Bucket=AWS_BUCKET_NAME,
                    Key=daily_key,
                    Body=daily_buffer.getvalue().split("\n", 1)[1],  # Skip header line
                    ContentType='text/csv',
                    Metadata={'append': 'true'}
                )
            else:
                # New file
                s3_client.put_object(
                    Bucket=AWS_BUCKET_NAME,
                    Key=daily_key,
                    Body=daily_buffer.getvalue(),
                    ContentType='text/csv'
                )
        except Exception as e:
            logger.error(f"Error appending to daily stream metrics file: {str(e)}")
        
        # Clear processed metrics
        stream_metrics.clear()
        
        logger.info(f"Saved stream metrics directly to S3")

    def generate_daily_report(self):
        """Generate a daily analytics report with insights for algorithm optimization"""
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        date_str = yesterday.strftime("%Y%m%d")
        
        # Try to load data from S3
        try:
            # Load chat data
            chat_key = f"{BROADCASTER_NAME.lower()}/chat_metrics/daily_{date_str}.csv"
            chat_data = None
            try:
                chat_obj = s3_client.get_object(Bucket=AWS_BUCKET_NAME, Key=chat_key)
                chat_data = pd.read_csv(io.BytesIO(chat_obj['Body'].read()))
            except Exception as e:
                logger.warning(f"Could not load chat data from S3: {str(e)}")
            
            # Load viewer data
            viewer_key = f"{BROADCASTER_NAME.lower()}/viewer_stats/daily_{date_str}.csv"
            viewer_data = None
            try:
                viewer_obj = s3_client.get_object(Bucket=AWS_BUCKET_NAME, Key=viewer_key)
                viewer_data = pd.read_csv(io.BytesIO(viewer_obj['Body'].read()))
            except Exception as e:
                logger.warning(f"Could not load viewer data from S3: {str(e)}")
            
            # Load subscriber data
            subs_key = f"{BROADCASTER_NAME.lower()}/subscribers/daily_{date_str}.csv"
            subs_data = None
            try:
                subs_obj = s3_client.get_object(Bucket=AWS_BUCKET_NAME, Key=subs_key)
                subs_data = pd.read_csv(io.BytesIO(subs_obj['Body'].read()))
            except Exception as e:
                logger.warning(f"Could not load subscriber data from S3: {str(e)}")
            
            # Load stream metrics
            stream_key = f"{BROADCASTER_NAME.lower()}/stream_metrics/daily_{date_str}.csv"
            stream_data = None
            try:
                stream_obj = s3_client.get_object(Bucket=AWS_BUCKET_NAME, Key=stream_key)
                stream_data = pd.read_csv(io.BytesIO(stream_obj['Body'].read()))
            except Exception as e:
                logger.warning(f"Could not load stream metrics from S3: {str(e)}")
            
            # Generate report
            report = {
                'date': date_str,
                'channel': BROADCASTER_NAME,
                'summary': {},
                'insights': [],
                'recommendations': []
            }
            
            # Process subscriber data
            if subs_data is not None and not subs_data.empty:
                report['summary']['new_subscribers'] = len(subs_data)
                report['summary']['gift_subs'] = subs_data['is_gift'].sum() if 'is_gift' in subs_data.columns else 0
                
                # Tier distribution
                if 'tier' in subs_data.columns:
                    tier_counts = subs_data['tier'].value_counts().to_dict()
                    report['summary']['tier_distribution'] = tier_counts
            
            # Process chat data
            if chat_data is not None and not chat_data.empty:
                report['summary']['total_chat_messages'] = len(chat_data)
                report['summary']['unique_chatters'] = len(chat_data['sender'].unique()) if 'sender' in chat_data.columns else 0
                
                # Analyze chat engagement patterns
                if 'timestamp' in chat_data.columns:
                    chat_data['timestamp'] = pd.to_datetime(chat_data['timestamp'])
                    chat_data['hour'] = chat_data['timestamp'].dt.hour
                    
                    # Group by hour and count messages
                    hourly_counts = chat_data.groupby('hour').size()
                    if not hourly_counts.empty:
                        peak_hour = hourly_counts.idxmax()
                        report['insights'].append({
                            'type': 'peak_engagement',
                            'message': f"Peak chat engagement occurs around {peak_hour}:00",
                            'value': int(peak_hour)
                        })
            
            # Process viewer data
            if viewer_data is not None and not viewer_data.empty:
                report['summary']['peak_viewers'] = viewer_data['viewer_count'].max() if 'viewer_count' in viewer_data.columns else 0
                report['summary']['avg_viewers'] = viewer_data['viewer_count'].mean() if 'viewer_count' in viewer_data.columns else 0
                
                # Analyze viewer retention
                if 'timestamp' in viewer_data.columns and len(viewer_data) > 10:
                    viewer_data['timestamp'] = pd.to_datetime(viewer_data['timestamp'])
                    viewer_data = viewer_data.sort_values('timestamp')
                    
                    # Calculate viewer retention rate
                    start_viewers = viewer_data['viewer_count'].iloc[0]
                    mid_viewers = viewer_data['viewer_count'].iloc[len(viewer_data)//2]
                    end_viewers = viewer_data['viewer_count'].iloc[-1]
                    
                    retention_mid = (mid_viewers / start_viewers) * 100 if start_viewers > 0 else 0
                    retention_end = (end_viewers / start_viewers) * 100 if start_viewers > 0 else 0
                    
                    report['summary']['retention_mid_percent'] = retention_mid
                    report['summary']['retention_end_percent'] = retention_end
                    
                    # Add insights based on retention
                    if retention_end < 50:
                        report['insights'].append({
                            'type': 'retention_issue',
                            'message': "Strong viewer drop-off detected throughout stream",
                            'value': retention_end
                        })
                        report['recommendations'].append({
                            'type': 'content_pacing',
                            'message': "Consider introducing new content segments every 30 minutes to maintain viewer interest and improve algorithm ranking"
                        })
                    elif retention_end > 80:
                        report['insights'].append({
                            'type': 'retention_positive',
                            'message': "Excellent viewer retention throughout stream",
                            'value': retention_end
                        })
                        report['recommendations'].append({
                            'type': 'content_strategy',
                            'message': "This content format performs well for retention. Consider creating more similar content to maintain algorithm favor."
                        })
            
            # Process stream metrics
            if stream_data is not None and not stream_data.empty:
                report['summary']['stream_duration'] = stream_data['stream_duration'].max() if 'stream_duration' in stream_data.columns else 0
                
                # Analyze potential algorithm impact
                if 'viewer_count' in stream_data.columns and len(stream_data) > 5:
                    stream_data = stream_data.sort_values('timestamp') if 'timestamp' in stream_data.columns else stream_data
                    # Check viewer growth pattern
                    viewer_growth = stream_data['viewer_count'].pct_change().mean() * 100
                    report['summary']['avg_viewer_growth_pct'] = viewer_growth
                    
                    if viewer_growth > 5:
                        report['insights'].append({
                            'type': 'algorithm_boost',
                            'message': "Strong positive viewer growth rate indicates algorithm favor",
                            'value': viewer_growth
                        })
                        report['recommendations'].append({
                            'type': 'stream_duration',
                            'message': "Consider extending streams by 30-60 minutes to capitalize on algorithm boost and increase discoverability"
                        })
                    elif viewer_growth < -5:
                        report['insights'].append({
                            'type': 'algorithm_concern',
                            'message': "Negative viewer trend may indicate algorithm deprioritization",
                            'value': viewer_growth
                        })
                        report['recommendations'].append({
                            'type': 'content_variety',
                            'message': "Increase content variety and engagement prompts to boost algorithm metrics"
                        })
            
            # Save report directly to S3
            report_key = f"{BROADCASTER_NAME.lower()}/reports/daily_report_{date_str}.json"
            s3_client.put_object(
                Bucket=AWS_BUCKET_NAME,
                Key=report_key,
                Body=json.dumps(report, indent=4),
                ContentType='application/json'
            )
            
            logger.info(f"Generated daily report for {date_str} and saved directly to S3")
            return report
        
        except Exception as e:
            logger.error(f"Error generating daily report: {str(e)}")
            return None

    async def check_stream_status(self):
        """Check if the broadcaster is currently live with immediate S3 update"""
        try:
            stream_info = twitch.get_streams(user_id=[broadcaster_id])
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
                
                if not live_metrics['is_live']:
                    # Stream just started
                    live_metrics['is_live'] = True
                    live_metrics['stream_started_at'] = stream_data['started_at']
                    live_metrics['current_viewers'] = stream_data['viewer_count']
                    live_metrics['peak_viewers'] = stream_data['viewer_count']
                    
                    logger.info(f"Stream started at {stream_data['started_at']}")
                    
                    # Add to recent events
                    live_metrics['recent_events'].append({
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
                    
                    s3_key = f"{BROADCASTER_NAME.lower()}/stream_metrics/{datetime.datetime.now().strftime('%Y%m%d')}/stream_start.json"
                    s3_client.put_object(
                        Bucket=AWS_BUCKET_NAME,
                        Key=s3_key,
                        Body=json.dumps(start_event),
                        ContentType='application/json'
                    )
                else:
                    # Update current metrics
                    live_metrics['current_viewers'] = stream_data['viewer_count']
                    live_metrics['peak_viewers'] = max(live_metrics['peak_viewers'], stream_data['viewer_count'])
                    
                    # Add to viewer retention chart
                    live_metrics['viewer_retention'].append({
                        'timestamp': timestamp,
                        'viewer_count': stream_data['viewer_count']
                    })
                    
                    # Keep only the last 60 data points
                    if len(live_metrics['viewer_retention']) > 60:
                        live_metrics['viewer_retention'] = live_metrics['viewer_retention'][-60:]
                    
                    # Add to stream metrics for historical tracking
                    stream_metrics.append({
                        'timestamp': timestamp,
                        'viewer_count': stream_data['viewer_count'],
                        'stream_duration': (datetime.datetime.now() - datetime.datetime.fromisoformat(
                            live_metrics['stream_started_at'].replace('Z', '+00:00')
                        )).total_seconds() / 60,  # Duration in minutes
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
                    s3_key = f"{BROADCASTER_NAME.lower()}/viewer_stats/{datetime.datetime.now().strftime('%Y%m%d')}/viewer_count_{datetime.datetime.now().strftime('%H%M%S')}.json"
                    s3_client.put_object(
                        Bucket=AWS_BUCKET_NAME,
                        Key=s3_key,
                        Body=json.dumps(viewer_data),
                        ContentType='application/json'
                    )
                    
                    # Save data in batches for efficiency
                    if len(viewer_counts) >= 10:
                        await self.save_viewer_stats()
                    
                    if len(stream_metrics) >= 10:
                        await self.save_stream_metrics()
            else:
                # Streamer is not live
                if live_metrics['is_live']:
                    # Stream just ended
                    live_metrics['is_live'] = False
                    
                    # Calculate stream duration
                    if live_metrics['stream_started_at']:
                        start_time = datetime.datetime.fromisoformat(live_metrics['stream_started_at'].replace('Z', '+00:00'))
                        end_time = datetime.datetime.now(datetime.timezone.utc)
                        duration_minutes = int((end_time - start_time).total_seconds() / 60)
                        
                        # Add stream end event
                        end_event = {
                            'timestamp': timestamp,
                            'type': 'stream',
                            'message': f"Stream ended (Duration: {duration_minutes} minutes)"
                        }
                        
                        live_metrics['recent_events'].append(end_event)
                        
                        # Record stream end event directly in S3
                        stream_end_data = {
                            'timestamp': timestamp,
                            'event_type': 'stream_end',
                            'stream_duration_minutes': duration_minutes,
                            'peak_viewers': live_metrics['peak_viewers'],
                            'unique_chatters': live_metrics['unique_chatters'],
                            'total_chat_messages': live_metrics['total_chat_messages']
                        }
                        
                        s3_key = f"{BROADCASTER_NAME.lower()}/stream_metrics/{datetime.datetime.now().strftime('%Y%m%d')}/stream_end.json"
                        s3_client.put_object(
                            Bucket=AWS_BUCKET_NAME,
                            Key=s3_key,
                            Body=json.dumps(stream_end_data),
                            ContentType='application/json'
                        )
                    
                    # Save final metrics
                    await self.save_viewer_stats()
                    await self.save_stream_metrics()
                    await self.save_chat_metrics()
                    
                    logger.info("Stream ended, all metrics saved to S3")
            
            # Save stream status to S3 for monitoring
            date_str = datetime.datetime.now().strftime("%Y%m%d")
            s3_key = f"{BROADCASTER_NAME.lower()}/status/stream_status_{date_str}.json"
            
            try:
                # Check if file exists
                try:
                    existing_obj = s3_client.get_object(Bucket=AWS_BUCKET_NAME, Key=s3_key)
                    existing_content = existing_obj['Body'].read().decode('utf-8')
                    
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
                    s3_client.put_object(
                        Bucket=AWS_BUCKET_NAME,
                        Key=s3_key,
                        Body=json.dumps(existing_data),
                        ContentType='application/json'
                    )
                except:
                    # File doesn't exist, create new
                    s3_client.put_object(
                        Bucket=AWS_BUCKET_NAME,
                        Key=s3_key,
                        Body=json.dumps([status_data]),
                        ContentType='application/json'
                    )
            except Exception as e:
                logger.error(f"Error saving stream status to S3: {str(e)}")
        
        except Exception as e:
            logger.error(f"Error checking stream status: {str(e)}")

    async def get_subscriber_count(self):
        """Get the current subscriber count with immediate S3 save"""
        try:
            # Get subscriber count from Twitch API
            sub_response = twitch.get_broadcaster_subscriptions(broadcaster_id=broadcaster_id)
            timestamp = datetime.datetime.now().isoformat()
            
            if 'total' in sub_response:
                sub_count = sub_response['total']
                live_metrics['subscriber_count'] = sub_count
                logger.info(f"Current subscriber count: {live_metrics['subscriber_count']}")
                
                # Save subscriber count data directly to S3
                sub_count_data = {
                    'timestamp': timestamp,
                    'subscriber_count': sub_count
                }
                
                date_str = datetime.datetime.now().strftime("%Y%m%d")
                s3_key = f"{BROADCASTER_NAME.lower()}/subscribers/{date_str}/count_{datetime.datetime.now().strftime('%H%M%S')}.json"
                s3_client.put_object(
                    Bucket=AWS_BUCKET_NAME,
                    Key=s3_key,
                    Body=json.dumps(sub_count_data),
                    ContentType='application/json'
                )
        except Exception as e:
            logger.error(f"Error getting subscriber count: {str(e)}")

    async def analyze_top_clips(self):
        """Analyze top clips and save results directly to S3"""
        try:
            # Get top clips for the channel
            clips = twitch.get_clips(broadcaster_id=broadcaster_id, first=20)
            
            if 'data' in clips and clips['data']:
                date_str = datetime.datetime.now().strftime("%Y%m%d")
                
                # Extract relevant clip data
                clip_data = []
                for clip in clips['data']:
                    clip_data.append({
                        'id': clip['id'],
                        'title': clip['title'],
                        'created_at': clip['created_at'],
                        'duration': clip['duration'],
                        'view_count': clip['view_count'],
                        'game_id': clip['game_id'],
                        'thumbnail_url': clip['thumbnail_url']
                    })
                
                # Save clips data directly to S3
                clips_key = f"{BROADCASTER_NAME.lower()}/clip_analysis/top_clips_{date_str}.json"
                s3_client.put_object(
                    Bucket=AWS_BUCKET_NAME,
                    Key=clips_key,
                    Body=json.dumps(clip_data, indent=4),
                    ContentType='application/json'
                )
                
                # Analyze clips for insights
                if clip_data:
                    # Sort by view count
                    sorted_clips = sorted(clip_data, key=lambda x: x['view_count'], reverse=True)
                    
                    # Find most popular game
                    game_counts = {}
                    for clip in sorted_clips:
                        game_id = clip.get('game_id', 'unknown')
                        game_counts[game_id] = game_counts.get(game_id, 0) + 1
                    
                    most_popular_game = max(game_counts.items(), key=lambda x: x[1])[0]
                    
                    # Find average clip duration
                    avg_duration = sum(clip['duration'] for clip in sorted_clips) / len(sorted_clips)
                    
                    # Log insights
                    logger.info(f"Top clip analysis: Most popular game ID: {most_popular_game}")
                    logger.info(f"Top clip analysis: Average clip duration: {avg_duration:.2f} seconds")
                    
                    # Save analysis results directly to S3
                    analysis_results = {
                        'date': date_str,
                        'most_popular_game': most_popular_game,
                        'avg_duration': avg_duration,
                        'top_5_clips': sorted_clips[:5]
                    }
                    
                    analysis_key = f"{BROADCASTER_NAME.lower()}/clip_analysis/analysis_{date_str}.json"
                    s3_client.put_object(
                        Bucket=AWS_BUCKET_NAME,
                        Key=analysis_key,
                        Body=json.dumps(analysis_results, indent=4),
                        ContentType='application/json'
                    )
                    
                    # Return insights for potential recommendations
                    return analysis_results
            
            return None
        except Exception as e:
            logger.error(f"Error analyzing top clips: {str(e)}")
            return None

    def schedule_tasks(self):
        """Schedule recurring tasks"""
        # Check stream status every minute
        schedule.every(1).minutes.do(lambda: asyncio.run(self.check_stream_status()))
        
        # Update subscriber count every 15 minutes
        schedule.every(15).minutes.do(lambda: asyncio.run(self.get_subscriber_count()))
        
        # Analyze top clips once a day
        schedule.every().day.at("04:00").do(lambda: asyncio.run(self.analyze_top_clips()))
        
        # Generate daily report at midnight
        schedule.every().day.at("00:01").do(self.generate_daily_report)
        
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
        """Run the Twitch Analytics Tracker"""
        # Connect to Twitch chat
        await self.connect_to_chat()
        
        # Schedule recurring tasks
        self.schedule_tasks()
        
        # Initial checks
        await self.check_stream_status()
        await self.get_subscriber_count()
        await self.analyze_top_clips()
        
        logger.info("Twitch Analytics Tracker is running")
        
        # Keep the chat connection alive
        while True:
            await asyncio.sleep(60)


# Create HTML template for the web dashboard
def create_html_template():
    """Create HTML template file if it doesn't exist"""
    index_path = 'templates/index.html'
    
    if not os.path.exists(index_path):
        html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{BROADCASTER_NAME} - Twitch Analytics</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.0.2/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body {{ background-color: #121212; color: #f8f9fa; }}
        .card {{ background-color: #1e1e1e; border-color: #2d2d2d; margin-bottom: 20px; }}
        .card-header {{ background-color: #2d2d2d; border-color: #2d2d2d; }}
        .trending-up {{ color: #4caf50; }}
        .trending-down {{ color: #f44336; }}
        .algorithm-insights {{ background-color: #263238; padding: 15px; border-radius: 5px; margin-top: 20px; }}
    </style>
</head>
<body>
    <div class="container mt-4">
        <div class="row">
            <div class="col-12 text-center mb-4">
                <h1>{BROADCASTER_NAME} Twitch Analytics</h1>
                <p>View detailed analytics on the <a href="/dashboard/" class="text-info">Dashboard</a></p>
            </div>
        </div>
        
        <div class="row">
            <div class="col-md-6">
                <div class="card">
                    <div class="card-header">Stream Status</div>
                    <div class="card-body">
                        <div id="stream-status"></div>
                    </div>
                </div>
            </div>
            
            <div class="col-md-6">
                <div class="card">
                    <div class="card-header">Quick Stats</div>
                    <div class="card-body">
                        <div id="quick-stats"></div>
                    </div>
                </div>
            </div>
        </div>
        
        <div class="row">
            <div class="col-12">
                <div class="card">
                    <div class="card-header">Recent Events</div>
                    <div class="card-body">
                        <div id="recent-events"></div>
                    </div>
                </div>
            </div>
        </div>

        <div class="row">
            <div class="col-12">
                <div class="card">
                    <div class="card-header">Algorithm Insights</div>
                    <div class="card-body">
                        <div id="algorithm-insights" class="algorithm-insights">
                            <h5>Twitch Algorithm Recommendations</h5>
                            <div id="algorithm-tips"></div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        
        <div class="row mt-4">
            <div class="col-12">
                <div class="card">
                    <div class="card-header">AWS Storage Status</div>
                    <div class="card-body">
                        <div id="aws-status">
                            <p><strong>S3 Bucket:</strong> {AWS_BUCKET_NAME}</p>
                            <p><strong>Direct Storage:</strong> Enabled (All data is immediately saved to AWS)</p>
                            <p><strong>Storage Path:</strong> s3://{AWS_BUCKET_NAME}/{BROADCASTER_NAME.lower()}/</p>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>
    
    <script>
        // Update metrics every 5 seconds
        function updateMetrics() {{
            fetch('/api/metrics')
                .then(response => response.json())
                .then(data => {{
                    // Update stream status
                    const streamStatus = document.getElementById('stream-status');
                    streamStatus.innerHTML = `
                        <h3>
                            ${{data.is_live ? 
                                '<span class="badge bg-danger">LIVE</span>' : 
                                '<span class="badge bg-secondary">OFFLINE</span>'}}
                        </h3>
                        ${{data.is_live && data.stream_started_at ? 
                            `<p>Started at: ${{data.stream_started_at.replace('T', ' ').substring(0, 19)}}</p>` : 
                            ''}}
                    `;
                    
                    // Update quick stats
                    const quickStats = document.getElementById('quick-stats');
                    quickStats.innerHTML = `
                        <div class="row">
                            <div class="col-6">
                                <p><strong>Current Viewers:</strong> ${{data.current_viewers}}</p>
                                <p><strong>Peak Viewers:</strong> ${{data.peak_viewers}}</p>
                            </div>
                            <div class="col-6">
                                <p><strong>Subscribers:</strong> ${{data.subscriber_count}}</p>
                                <p><strong>Chat Messages:</strong> ${{data.total_chat_messages}}</p>
                            </div>
                        </div>
                    `;
                    
                    // Update recent events
                    const recentEvents = document.getElementById('recent-events');
                    let eventsHtml = '<ul class="list-group">';
                    
                    const events = data.recent_events || [];
                    events.slice(-10).reverse().forEach(event => {{
                        let badgeClass = 'bg-info';
                        if (event.type === 'subscription') badgeClass = 'bg-success';
                        if (event.type === 'raid') badgeClass = 'bg-warning';
                        if (event.type === 'stream') badgeClass = 'bg-danger';
                        
                        eventsHtml += `
                            <li class="list-group-item bg-dark text-light">
                                <span class="badge ${{badgeClass}} me-2">${{event.type.toUpperCase()}}</span>
                                ${{event.message}} - 
                                <small class="text-muted">${{event.timestamp.split('T')[1].substring(0, 8)}}</small>
                            </li>
                        `;
                    }});
                    
                    eventsHtml += '</ul>';
                    recentEvents.innerHTML = eventsHtml;
                    
                    // Update algorithm tips
                    const algorithmTips = document.getElementById('algorithm-tips');
                    
                    // Generate algorithm tips based on current metrics
                    let tips = '<ul class="list-group">';
                    
                    if (data.is_live) {{
                        // Viewer retention tip
                        const viewerRetention = data.viewer_retention || [];
                        if (viewerRetention.length > 5) {{
                            const initialViewers = viewerRetention[0]?.viewer_count || 0;
                            const currentViewers = viewerRetention[viewerRetention.length-1]?.viewer_count || 0;
                            const retentionRate = initialViewers > 0 ? (currentViewers / initialViewers) : 0;
                            
                            if (retentionRate < 0.7) {{
                                tips += `
                                    <li class="list-group-item bg-dark text-light">
                                        <i class="trending-down">▼</i> <strong>Viewer Retention:</strong> 
                                        Retention rate is below target. Consider increasing chat interaction to boost algorithm ranking.
                                    </li>
                                `;
                            }} else {{
                                tips += `
                                    <li class="list-group-item bg-dark text-light">
                                        <i class="trending-up">▲</i> <strong>Viewer Retention:</strong> 
                                        Strong retention rate! Current content is maintaining audience interest.
                                    </li>
                                `;
                            }}
                        }}
                        
                        // Chat engagement tip
                        if (data.chat_messages_per_minute < 5) {{
                            tips += `
                                <li class="list-group-item bg-dark text-light">
                                    <i class="trending-down">▼</i> <strong>Chat Engagement:</strong> 
                                    Low chat activity may reduce algorithm visibility. Try asking engaging questions.
                                </li>
                            `;
                        }} else {{
                            tips += `
                                <li class="list-group-item bg-dark text-light">
                                    <i class="trending-up">▲</i> <strong>Chat Engagement:</strong> 
                                    Good chat activity! This helps with algorithm placement.
                                </li>
                            `;
                        }}
                    }} else {{
                        // Offline recommendations
                        tips += `
                            <li class="list-group-item bg-dark text-light">
                                <strong>Channel Growth:</strong> 
                                Post your next stream schedule to Discord/social media to improve initial viewer count.
                            </li>
                            <li class="list-group-item bg-dark text-light">
                                <strong>Content Planning:</strong> 
                                Review your top clips to identify content that performs well with the algorithm.
                            </li>
                        `;
                    }}
                    
                    tips += '</ul>';
                    algorithmTips.innerHTML = tips;
                }})
                .catch(error => console.error('Error fetching metrics:', error));
        }}
        
        // Initial update and set interval
        updateMetrics();
        setInterval(updateMetrics, 5000);
    </script>
</body>
</html>
        """
        
        with open(index_path, 'w') as f:
            f.write(html_content)
        
        logger.info(f"Created HTML template at {index_path}")

def start_flask_server():
    """Start the Flask server in a separate thread"""
    def run_flask():
        flask_app.run(host='0.0.0.0', port=5000, debug=False)
    
    flask_thread = threading.Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()
    
    logger.info("Flask server started on http://0.0.0.0:5000")

async def main():
    """Main function to start the application"""
    # Create HTML template
    create_html_template()
    
    # Initialize the Twitch Analytics Tracker
    tracker = TwitchAnalyticsTracker()
    
    # Start Flask server
    start_flask_server()
    
    # Run the tracker
    await tracker.run()

if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main()) 