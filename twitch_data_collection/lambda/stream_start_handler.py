"""
Lambda handler for processing Twitch stream start events.

This module contains the handler function for processing and recording
the start of a Twitch stream, storing relevant metadata in S3.
"""

import json
import boto3
import os
import datetime
import logging
from typing import Dict, Any

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for processing Twitch stream start events.
    
    Records stream start events with relevant metadata and stores in S3.
    
    Args:
        event (Dict[str, Any]): Lambda event containing stream start data.
                Expected keys:
                - broadcaster_name: Name of the broadcaster
                - stream_id: Unique ID for the stream
                - category: Stream category/game
        context (Any): Lambda context object
        
    Returns:
        Dict[str, Any]: Response object with status code and message
        
    Example Event:
        {
            "broadcaster_name": "example_streamer",
            "stream_id": "12345678",
            "category": "Just Chatting"
        }
    """
    try:
        logger.info("Processing stream start event")
        
        # Extract parameters from event
        broadcaster_name = event.get('broadcaster_name')
        stream_id = event.get('stream_id')
        category = event.get('category')
        
        if not broadcaster_name or not stream_id:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Missing required parameters (broadcaster_name, stream_id)'})
            }
        
        # Get environment variables
        s3_bucket = os.environ.get('S3_BUCKET_NAME')
        
        if not s3_bucket:
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'Missing S3 bucket configuration'})
            }
        
        # Initialize S3 client
        s3_client = boto3.client('s3')
        
        # Record stream start
        timestamp = datetime.datetime.now().isoformat()
        stream_data = {
            'timestamp': timestamp,
            'broadcaster_name': broadcaster_name,
            'event': 'stream_start',
            'stream_id': stream_id,
            'category': category
        }
        
        # Save to S3
        date_str = datetime.datetime.now().strftime('%Y%m%d')
        key = f"{broadcaster_name.lower()}/events/{date_str}/stream_start_{int(datetime.datetime.now().timestamp())}.json"
        
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=key,
            Body=json.dumps(stream_data),
            ContentType='application/json'
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'status': 'Stream start event processed',
                'timestamp': timestamp,
                'stream_id': stream_id,
                's3_key': key
            })
        }
            
    except Exception as e:
        logger.error(f"Error processing stream start event: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f'Error processing stream start event: {str(e)}'})
        }