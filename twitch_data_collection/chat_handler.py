import datetime
import logging
import json
from twitchAPI.chat import Chat, EventData, ChatMessage

logger = logging.getLogger("twitch_analytics")

# Define Chat Events as constants
class ChatEvent:
    MESSAGE = "message"
    SUB = "subscription"
    RAID = "raid"

chat_messages = []
subscriber_events = []

async def on_chat_message(event_data, message, s3_client, aws_bucket_name, broadcaster_name, live_metrics):
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
    from twitch_data_collection.data_storage import save_event_to_s3
    await save_event_to_s3(s3_client, aws_bucket_name, broadcaster_name, 'chat_message', message_data)
    
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
        from twitch_data_collection.data_storage import save_chat_metrics
        await save_chat_metrics(s3_client, aws_bucket_name, broadcaster_name, chat_messages)

async def on_subscription(event_data, s3_client, aws_bucket_name, broadcaster_name, live_metrics):
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
    from twitch_data_collection.data_storage import save_event_to_s3, save_subscriber_data
    await save_event_to_s3(s3_client, aws_bucket_name, broadcaster_name, 'subscription', sub_data)
    
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
    await save_subscriber_data(s3_client, aws_bucket_name, broadcaster_name, subscriber_events)

async def on_raid(event_data, s3_client, aws_bucket_name, broadcaster_name, live_metrics):
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
    from twitch_data_collection.data_storage import save_event_to_s3
    await save_event_to_s3(s3_client, aws_bucket_name, broadcaster_name, 'raid', raid_data)
    
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

async def setup_chat_connection(twitch, target_channel, s3_client, aws_bucket_name, broadcaster_name, live_metrics):
    """Connect to Twitch chat and set up event handlers"""
    logger.info(f"Connecting to Twitch chat for channel: {target_channel}")
    chat = await Chat(twitch, initial_channels=[target_channel])
    
    # Register event handlers with custom params via lambda
    chat.register_event(ChatEvent.MESSAGE, 
        lambda event_data, message: on_chat_message(
            event_data, message, s3_client, aws_bucket_name, broadcaster_name, live_metrics
        )
    )
    
    chat.register_event(ChatEvent.SUB,
        lambda event_data: on_subscription(
            event_data, s3_client, aws_bucket_name, broadcaster_name, live_metrics
        )
    )
    
    chat.register_event(ChatEvent.RAID,
        lambda event_data: on_raid(
            event_data, s3_client, aws_bucket_name, broadcaster_name, live_metrics
        )
    )
    
    logger.info("Successfully connected to Twitch chat")
    return chat