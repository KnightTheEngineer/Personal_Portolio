"""
Lambda handler for analyzing Twitch clips data.

This module contains the handler function for processing and analyzing
Twitch clips to identify trends, popular games, and engagement patterns.
"""

import json
import boto3
import os
import logging
from typing import Dict, Any
from lambda.utils import analyze_twitch_clips

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for analyzing top clips data.
    
    Processes Twitch clips to identify trends, popular games, and engagement patterns.
    
    Args:
        event (Dict[str, Any]): Lambda event containing analysis parameters.
                Expected keys:
                - broadcaster_name: Name of the broadcaster
                - limit: Optional number of clips to analyze (defaults to 20)
        context (Any): Lambda context object
        
    Returns:
        Dict[str, Any]: Response object with status code and clip analysis
        
    Example Event:
        {
            "broadcaster_name": "example_streamer",
            "limit": 30  // Optional
        }
    """
    try:
        logger.info("Processing clip analytics")
        
        # Extract parameters from event
        broadcaster_name = event.get('broadcaster_name')
        clip_limit = event.get('limit', 20)
        
        if not broadcaster_name:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Missing required parameter: broadcaster_name'})
            }
        
        # Get environment variables
        s3_bucket = os.environ.get('S3_BUCKET_NAME')
        twitch_client_id = os.environ.get('TWITCH_CLIENT_ID')
        twitch_client_secret = os.environ.get('TWITCH_CLIENT_SECRET')
        
        if not s3_bucket or not twitch_client_id or not twitch_client_secret:
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'Missing required configuration'})
            }
        
        # Initialize S3 client
        s3_client = boto3.client('s3')
        
        # Process clips
        result = analyze_twitch_clips(
            s3_client, 
            s3_bucket, 
            broadcaster_name, 
            twitch_client_id, 
            twitch_client_secret,
            clip_limit
        )
        
        if result:
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'status': 'Clip analytics processed',
                    'clips_analyzed': result.get('clips_analyzed', 0),
                    'top_game': result.get('top_game'),
                    'avg_duration': result.get('avg_duration'),
                    'report_key': result.get('report_key')
                })
            }
        else:
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'Could not process clips, data not found'})
            }
    
    except Exception as e:
        logger.error(f"Error processing clips: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f'Error processing clips: {str(e)}'})
        }