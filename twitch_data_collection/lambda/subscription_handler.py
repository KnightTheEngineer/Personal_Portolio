"""
Lambda handler for processing Twitch subscription events.

This module contains the handler function for processing and recording
Twitch subscription events, storing relevant metadata in S3.
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
    Lambda handler for processing Twitch subscription events.
    
    Records subscription events with relevant metadata and stores in S3.
    
    Args:
        event (Dict[str, Any]): Lambda event containing subscription data.
                Expected keys:
                - broadcaster_name: Name of the broadcaster
                - subscription_data: Detailed subscription information
        context (Any): Lambda context object
        
    Returns:
        Dict[str, Any]: Response object with status code and message
        
    Example Event:
        {
            "broadcaster_name": "example_streamer",
            "subscription_data": {
                "user": "subscriber_name",
                "tier": "1000",
                "is_gift": false,
                "total_months": 3
            }
        }
    """
    try:
        logger.info("Processing subscription event")
        
        # Extract parameters from event
        broadcaster_name = event.get('broadcaster_name')
        sub_data = event.get('subscription_data', {})
        
        if not broadcaster_name or not sub_data:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'Missing required parameters (broadcaster_name, subscription_data)'
                })
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
        
        # Add timestamp if not present
        if 'timestamp' not in sub_data:
            sub_data['timestamp'] = datetime.datetime.now().isoformat()
            
        # Save to S3
        date_str = datetime.datetime.now().strftime('%Y%m%d')
        key = f"{broadcaster_name.lower()}/subscribers/{date_str}/sub_{int(datetime.datetime.now().timestamp())}.json"
        
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=key,
            Body=json.dumps(sub_data),
            ContentType='application/json'
        )
        
        # Get subscription tier name for response
        tier_name = "Tier 1"
        if sub_data.get('tier') == "2000":
            tier_name = "Tier 2"
        elif sub_data.get('tier') == "3000":
            tier_name = "Tier 3"
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'status': 'Subscription event processed',
                'user': sub_data.get('user', 'unknown'),
                'tier': tier_name,
                'is_gift': sub_data.get('is_gift', False),
                's3_key': key
            })
        }
            
    except Exception as e:
        logger.error(f"Error processing subscription event: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f'Error processing subscription event: {str(e)}'})
        }