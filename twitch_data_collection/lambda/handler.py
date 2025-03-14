"""
Twitch analytics Lambda functions for serverless processing.

This module contains separate AWS Lambda handler functions for different
aspects of Twitch analytics data processing.

Each function serves a single, focused purpose following the AWS Lambda
best practice of having one responsibility per function.
"""

import json
import boto3
import os
import datetime
import logging
from typing import Dict, Any, Optional, Union
from lambda.utils import (
    process_twitch_data, 
    generate_metrics_report, 
    save_processed_data,
    generate_analytics_report,
    analyze_twitch_clips
)

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def process_stream_start(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
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

def process_stream_end(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for processing Twitch stream end events.
    
    Processes and analyzes stream data when a stream ends, 
    generating reports and metrics.
    
    Args:
        event (Dict[str, Any]): Lambda event containing stream end data.
                Expected keys:
                - broadcaster_name: Name of the broadcaster
                - stream_id: Unique ID for the stream
                - duration: Stream duration in minutes
                - viewer_data: List of viewer count snapshots
        context (Any): Lambda context object
        
    Returns:
        Dict[str, Any]: Response object with status code and message
        
    Example Event:
        {
            "broadcaster_name": "example_streamer",
            "stream_id": "12345678",
            "duration": 120,
            "viewer_data": [
                {"timestamp": "2025-01-01T01:00:00Z", "viewer_count": 100},
                {"timestamp": "2025-01-01T01:15:00Z", "viewer_count": 150}
            ]
        }
    """
    try:
        logger.info("Processing stream end event")
        
        # Extract parameters from event
        broadcaster_name = event.get('broadcaster_name')
        stream_id = event.get('stream_id')
        stream_duration = event.get('duration')
        viewer_data = event.get('viewer_data', [])
        
        if not broadcaster_name or not stream_id or stream_duration is None:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'Missing required parameters (broadcaster_name, stream_id, duration)'
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
        
        # Process stream metrics
        metrics = process_twitch_data(broadcaster_name, viewer_data)
        
        # Generate report
        report = generate_metrics_report(broadcaster_name, metrics, stream_duration, stream_id)
        
        # Save processed data and report
        report_key = save_processed_data(s3_client, s3_bucket, broadcaster_name, report)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'status': 'Stream end event processed',
                'stream_id': stream_id,
                'duration_minutes': stream_duration,
                'peak_viewers': metrics.get('viewer_metrics', {}).get('peak_viewers', 0),
                'report_key': report_key
            })
        }
            
    except Exception as e:
        logger.error(f"Error processing stream end event: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f'Error processing stream end event: {str(e)}'})
        }

def process_subscription(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
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

def generate_daily_report(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for generating daily analytics reports.
    
    Generates a comprehensive daily report by analyzing chat, viewer,
    subscriber, and stream data for the specified date.
    
    Args:
        event (Dict[str, Any]): Lambda event containing report parameters.
                Expected keys:
                - broadcaster_name: Name of the broadcaster
                - date: Optional date string in format YYYYMMDD (defaults to yesterday)
        context (Any): Lambda context object
        
    Returns:
        Dict[str, Any]: Response object with status code and message
        
    Example Event:
        {
            "broadcaster_name": "example_streamer",
            "date": "20250101"  // Optional
        }
    """
    try:
        logger.info("Generating daily report")
        
        # Extract parameters from event
        broadcaster_name = event.get('broadcaster_name')
        date_str = event.get('date')
        
        if not broadcaster_name:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Missing required parameter: broadcaster_name'})
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
        
        # Use yesterday's date if not provided
        if not date_str:
            yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
            date_str = yesterday.strftime('%Y%m%d')
        
        # Generate report
        report = generate_analytics_report(s3_client, s3_bucket, broadcaster_name, date_str, 'daily')
        
        if report:
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'status': 'Daily report generated',
                    'date': date_str,
                    'report_key': report.get('report_key'),
                    'summary': report.get('summary', {})
                })
            }
        else:
            return {
                'statusCode': 404,
                'body': json.dumps({
                    'error': 'Could not generate report, data not found',
                    'date': date_str
                })
            }
    
    except Exception as e:
        logger.error(f"Error generating daily report: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f'Error generating daily report: {str(e)}'})
        }

def analyze_clips_data(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
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