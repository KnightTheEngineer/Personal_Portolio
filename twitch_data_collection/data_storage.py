import os
import io
import json
import logging
import datetime
import boto3
import pandas as pd

logger = logging.getLogger("twitch_analytics")

def initialize_s3_client(aws_access_key, aws_secret_key, aws_region):
    """Initialize and return S3 client using AWS credentials"""
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )
    return s3_client

def setup_s3_bucket(s3_client, bucket_name, region, broadcaster_name):
    """Set up S3 bucket structure with necessary folders"""
    try:
        # Check if bucket exists, if not create it
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            logger.info(f"S3 bucket {bucket_name} exists")
        except:
            logger.info(f"Creating S3 bucket {bucket_name}")
            if region == 'us-east-1':
                s3_client.create_bucket(Bucket=bucket_name)
            else:
                s3_client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': region}
                )
        
        # Create folder structure in S3
        folders = [
            f"{broadcaster_name.lower()}/subscribers/",
            f"{broadcaster_name.lower()}/chat_metrics/",
            f"{broadcaster_name.lower()}/viewer_stats/",
            f"{broadcaster_name.lower()}/stream_metrics/",
            f"{broadcaster_name.lower()}/reports/",
            f"{broadcaster_name.lower()}/raw_events/"
        ]
        
        for folder in folders:
            s3_client.put_object(Bucket=bucket_name, Key=folder)
        
        logger.info(f"S3 folder structure set up for {broadcaster_name}")
        
    except Exception as e:
        logger.error(f"Error setting up S3 bucket: {str(e)}")

def save_to_s3(s3_client, bucket_name, key, data, content_type='application/json'):
    """Save data directly to S3"""
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=data,
            ContentType=content_type
        )
        return True
    except Exception as e:
        logger.error(f"Error saving to S3 ({key}): {str(e)}")
        return False

def get_from_s3(s3_client, bucket_name, key):
    """Get data from S3"""
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        return response['Body'].read().decode('utf-8')
    except Exception as e:
        logger.error(f"Error getting from S3 ({key}): {str(e)}")
        return None

async def save_event_to_s3(s3_client, bucket_name, broadcaster_name, event_type, event_data):
    """Save event data directly to S3"""
    try:
        timestamp = datetime.datetime.now()
        date_str = timestamp.strftime("%Y%m%d")
        hour_str = timestamp.strftime("%H")
        
        # Create a unique key for this event
        event_id = f"{int(timestamp.timestamp() * 1000)}_{hash(str(event_data))}"
        s3_key = f"{broadcaster_name.lower()}/raw_events/{date_str}/{hour_str}/{event_type}_{event_id}.json"
        
        # Convert data to JSON and save directly to S3
        json_data = json.dumps(event_data)
        save_to_s3(s3_client, bucket_name, s3_key, json_data)
        
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

async def save_chat_metrics(s3_client, bucket_name, broadcaster_name, chat_messages):
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
    metrics_key = f"{broadcaster_name.lower()}/chat_metrics/{date_str}/metrics_{timestamp.strftime('%H%M%S')}.json"
    save_to_s3(s3_client, bucket_name, metrics_key, json.dumps(chat_metrics))
    
    # Save the raw chat messages batch
    batch_key = f"{broadcaster_name.lower()}/chat_metrics/{date_str}/raw_batch_{timestamp.strftime('%H%M%S')}.json"
    
    # For larger datasets, stream directly to S3
    if len(chat_messages) > 1000:
        # Stream JSON data to S3
        buffer = io.BytesIO()
        for message in chat_messages:
            buffer.write((json.dumps(message) + '\n').encode('utf-8'))
        
        buffer.seek(0)
        s3_client.put_object(
            Bucket=bucket_name,
            Key=batch_key,
            Body=buffer.getvalue(),
            ContentType='application/json'
        )
    else:
        # For smaller batches, save directly
        save_to_s3(s3_client, bucket_name, batch_key, json.dumps(chat_messages))
    
    # Also save as CSV for analytics tools
    csv_data = pd.DataFrame(chat_messages)
    csv_buffer = io.StringIO()
    csv_data.to_csv(csv_buffer, index=False)
    
    csv_key = f"{broadcaster_name.lower()}/chat_metrics/{date_str}/messages_{timestamp.strftime('%H%M%S')}.csv"
    save_to_s3(s3_client, bucket_name, csv_key, csv_buffer.getvalue(), 'text/csv')
    
    # Save a continuous daily record by appending to a consolidated file
    try:
        # Check if daily file exists
        daily_key = f"{broadcaster_name.lower()}/chat_metrics/daily_{date_str}.csv"
        
        try:
            # Try to get the existing file
            existing_obj = s3_client.get_object(Bucket=bucket_name, Key=daily_key)
            daily_exists = True
        except:
            daily_exists = False
        
        # Create a new CSV buffer with header only if it's a new file
        daily_buffer = io.StringIO()
        csv_data.to_csv(daily_buffer, index=False, header=not daily_exists)
        
        # If the file exists, append to it
        if daily_exists:
            s3_client.put_object(
                Bucket=bucket_name,
                Key=daily_key,
                Body=daily_buffer.getvalue().split("\n", 1)[1],  # Skip header line
                ContentType='text/csv',
                Metadata={'append': 'true'}
            )
        else:
            # New file
            save_to_s3(s3_client, bucket_name, daily_key, daily_buffer.getvalue(), 'text/csv')
    except Exception as e:
        logger.error(f"Error appending to daily chat file: {str(e)}")
    
    # Clear processed messages
    chat_messages.clear()
    
    logger.info(f"Saved chat metrics directly to S3")

async def save_subscriber_data(s3_client, bucket_name, broadcaster_name, subscriber_events):
    """Save subscriber data directly to S3"""
    if not subscriber_events:
        return
    
    timestamp = datetime.datetime.now()
    date_str = timestamp.strftime("%Y%m%d")
    
    # Save to S3 as JSON
    s3_key = f"{broadcaster_name.lower()}/subscribers/{date_str}/subscribers_{timestamp.strftime('%H%M%S')}.json"
    save_to_s3(s3_client, bucket_name, s3_key, json.dumps(subscriber_events))
    
    # Also save as CSV for analytics tools
    subs_df = pd.DataFrame(subscriber_events)
    csv_buffer = io.StringIO()
    subs_df.to_csv(csv_buffer, index=False)
    
    csv_key = f"{broadcaster_name.lower()}/subscribers/{date_str}/subscribers_{timestamp.strftime('%H%M%S')}.csv"
    save_to_s3(s3_client, bucket_name, csv_key, csv_buffer.getvalue(), 'text/csv')
    
    # Also append to daily file
    try:
        # Check if daily file exists
        daily_key = f"{broadcaster_name.lower()}/subscribers/daily_{date_str}.csv"
        
        try:
            # Try to get the existing file
            existing_obj = s3_client.get_object(Bucket=bucket_name, Key=daily_key)
            daily_exists = True
        except:
            daily_exists = False
        
        # Create a new CSV buffer with header only if it's a new file
        daily_buffer = io.StringIO()
        subs_df.to_csv(daily_buffer, index=False, header=not daily_exists)
        
        # If the file exists, append to it
        if daily_exists:
            s3_client.put_object(
                Bucket=bucket_name,
                Key=daily_key,
                Body=daily_buffer.getvalue().split("\n", 1)[1],  # Skip header line
                ContentType='text/csv',
                Metadata={'append': 'true'}
            )
        else:
            # New file
            save_to_s3(s3_client, bucket_name, daily_key, daily_buffer.getvalue(), 'text/csv')
    except Exception as e:
        logger.error(f"Error appending to daily subscribers file: {str(e)}")
    
    # Clear processed events
    subscriber_events.clear()
    
    logger.info(f"Saved subscriber data directly to S3")

async def save_viewer_stats(s3_client, bucket_name, broadcaster_name, viewer_counts):
    """Save viewer statistics directly to S3"""
    if not viewer_counts:
        return
    
    timestamp = datetime.datetime.now()
    date_str = timestamp.strftime("%Y%m%d")
    
    # Save to S3 as JSON
    s3_key = f"{broadcaster_name.lower()}/viewer_stats/{date_str}/viewers_{timestamp.strftime('%H%M%S')}.json"
    save_to_s3(s3_client, bucket_name, s3_key, json.dumps(viewer_counts))
    
    # Also save as CSV for analytics tools
    viewer_df = pd.DataFrame(viewer_counts)
    csv_buffer = io.StringIO()
    viewer_df.to_csv(csv_buffer, index=False)
    
    csv_key = f"{broadcaster_name.lower()}/viewer_stats/{date_str}/viewers_{timestamp.strftime('%H%M%S')}.csv"
    save_to_s3(s3_client, bucket_name, csv_key, csv_buffer.getvalue(), 'text/csv')
    
    # Also append to daily file
    try:
        # Check if daily file exists
        daily_key = f"{broadcaster_name.lower()}/viewer_stats/daily_{date_str}.csv"
        
        try:
            # Try to get the existing file
            existing_obj = s3_client.get_object(Bucket=bucket_name, Key=daily_key)
            daily_exists = True
        except:
            daily_exists = False
        
        # Create a new CSV buffer with header only if it's a new file
        daily_buffer = io.StringIO()
        viewer_df.to_csv(daily_buffer, index=False, header=not daily_exists)
        
        # If the file exists, append to it
        if daily_exists:
            s3_client.put_object(
                Bucket=bucket_name,
                Key=daily_key,
                Body=daily_buffer.getvalue().split("\n", 1)[1],  # Skip header line
                ContentType='text/csv',
                Metadata={'append': 'true'}
            )
        else:
            # New file
            save_to_s3(s3_client, bucket_name, daily_key, daily_buffer.getvalue(), 'text/csv')
    except Exception as e:
        logger.error(f"Error appending to daily viewer stats file: {str(e)}")
    
    # Clear processed viewer counts
    viewer_counts.clear()
    
    logger.info(f"Saved viewer statistics directly to S3")

async def save_stream_metrics(s3_client, bucket_name, broadcaster_name, stream_metrics):
    """Save stream metrics directly to S3"""
    if not stream_metrics:
        return
    
    timestamp = datetime.datetime.now()
    date_str = timestamp.strftime("%Y%m%d")
    
    # Save to S3 as JSON
    s3_key = f"{broadcaster_name.lower()}/stream_metrics/{date_str}/metrics_{timestamp.strftime('%H%M%S')}.json"
    save_to_s3(s3_client, bucket_name, s3_key, json.dumps(stream_metrics))
    
    # Also save as CSV for analytics tools
    metrics_df = pd.DataFrame(stream_metrics)
    csv_buffer = io.StringIO()
    metrics_df.to_csv(csv_buffer, index=False)
    
    csv_key = f"{broadcaster_name.lower()}/stream_metrics/{date_str}/metrics_{timestamp.strftime('%H%M%S')}.csv"
    save_to_s3(s3_client, bucket_name, csv_key, csv_buffer.getvalue(), 'text/csv')
    
    # Also append to daily file
    try:
        # Check if daily file exists
        daily_key = f"{broadcaster_name.lower()}/stream_metrics/daily_{date_str}.csv"
        
        try:
            # Try to get the existing file
            existing_obj = s3_client.get_object(Bucket=bucket_name, Key=daily_key)
            daily_exists = True
        except:
            daily_exists = False
        
        # Create a new CSV buffer with header only if it's a new file
        daily_buffer = io.StringIO()
        metrics_df.to_csv(daily_buffer, index=False, header=not daily_exists)
        
        # If the file exists, append to it
        if daily_exists:
            s3_client.put_object(
                Bucket=bucket_name,
                Key=daily_key,
                Body=daily_buffer.getvalue().split("\n", 1)[1],  # Skip header line
                ContentType='text/csv',
                Metadata={'append': 'true'}
            )
        else:
            # New file
            save_to_s3(s3_client, bucket_name, daily_key, daily_buffer.getvalue(), 'text/csv')
    except Exception as e:
        logger.error(f"Error appending to daily stream metrics file: {str(e)}")
    
    # Clear processed metrics
    stream_metrics.clear()
    
    logger.info(f"Saved stream metrics directly to S3")