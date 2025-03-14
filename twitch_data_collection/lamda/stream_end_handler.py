"""
Lambda handler for processing Twitch stream end events.

This module contains the handler function for processing and analyzing
data when a Twitch stream ends, generating reports and metrics.
"""

import json
import boto3
import os
import datetime
import logging
from typing import Dict, Any
from lambda.utils import process_twitch_data, generate_metrics_report, save_processed_data

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
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