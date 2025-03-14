"""
Lambda handler for generating daily Twitch analytics reports.

This module contains the handler function for generating comprehensive
daily reports by analyzing chat, viewer, subscriber, and stream data.
"""

import json
import boto3
import os
import datetime
import logging
from typing import Dict, Any
from lambda.utils import generate_analytics_report

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
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