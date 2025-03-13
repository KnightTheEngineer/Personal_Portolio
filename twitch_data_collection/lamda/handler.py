import json
import boto3
import os
import datetime
import logging
from lambda.utils import process_twitch_data, generate_metrics_report, save_processed_data

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def process_stream_event(event, context):
    """Lambda handler for processing Twitch stream events"""
    try:
        logger.info("Processing stream event")
        
        # Extract parameters from event
        broadcaster_name = event.get('broadcaster_name')
        event_type = event.get('event_type')
        
        if not broadcaster_name or not event_type:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Missing required parameters'})
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
        
        # Process data based on event type
        if event_type == 'stream_start':
            logger.info(f"Processing stream start for {broadcaster_name}")
            
            # Record stream start
            timestamp = datetime.datetime.now().isoformat()
            stream_data = {
                'timestamp': timestamp,
                'broadcaster_name': broadcaster_name,
                'event': 'stream_start',
                'stream_id': event.get('stream_id'),
                'category': event.get('category')
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
                'body': json.dumps({'status': 'Stream start event processed'})
            }
            
        elif event_type == 'stream_end':
            logger.info(f"Processing stream end for {broadcaster_name}")
            
            # Get stream data from event
            stream_duration = event.get('duration')
            viewer_data = event.get('viewer_data', {})
            
            # Process stream metrics
            metrics = process_twitch_data(broadcaster_name, viewer_data)
            
            # Generate report
            report = generate_metrics_report(broadcaster_name, metrics, stream_duration)
            
            # Save processed data and report
            save_processed_data(s3_client, s3_bucket, broadcaster_name, report)
            
            return {
                'statusCode': 200,
                'body': json.dumps({'status': 'Stream end event processed'})
            }
            
        elif event_type == 'subscription':
            logger.info(f"Processing subscription event for {broadcaster_name}")
            
            # Extract subscription data
            sub_data = event.get('subscription_data', {})
            
            # Save to S3
            date_str = datetime.datetime.now().strftime('%Y%m%d')
            key = f"{broadcaster_name.lower()}/subscribers/{date_str}/sub_{int(datetime.datetime.now().timestamp())}.json"
            
            s3_client.put_object(
                Bucket=s3_bucket,
                Key=key,
                Body=json.dumps(sub_data),
                ContentType='application/json'
            )
            
            return {
                'statusCode': 200,
                'body': json.dumps({'status': 'Subscription event processed'})
            }
            
        else:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': f'Unknown event type: {event_type}'})
            }
    
    except Exception as e:
        logger.error(f"Error processing event: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f'Error processing event: {str(e)}'})
        }

def generate_report(event, context):
    """Lambda handler for generating periodic reports"""
    try:
        logger.info("Generating periodic report")
        
        # Extract parameters from event
        broadcaster_name = event.get('broadcaster_name')
        report_type = event.get('report_type', 'daily')
        date_str = event.get('date')
        
        if not broadcaster_name:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Missing required parameters'})
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
        
        # Use the date provided or default to yesterday for daily reports
        if not date_str:
            if report_type == 'daily':
                yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
                date_str = yesterday.strftime('%Y%m%d')
            else:
                date_str = datetime.datetime.now().strftime('%Y%m%d')
        
        # Import analytics function
        from lambda.utils import generate_analytics_report
        
        # Generate report
        report = generate_analytics_report(s3_client, s3_bucket, broadcaster_name, date_str, report_type)
        
        if report:
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'status': f'{report_type.capitalize()} report generated',
                    'report_key': report.get('report_key')
                })
            }
        else:
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'Could not generate report, data not found'})
            }
    
    except Exception as e:
        logger.error(f"Error generating report: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f'Error generating report: {str(e)}'})
        }

def process_clips(event, context):
    """Lambda handler for processing clip analytics"""
    try:
        logger.info("Processing clip analytics")
        
        # Extract parameters from event
        broadcaster_name = event.get('broadcaster_name')
        
        if not broadcaster_name:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Missing required parameters'})
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
        
        # Import Twitch API processing function
        from lambda.utils import analyze_twitch_clips
        
        # Process clips
        result = analyze_twitch_clips(
            s3_client, 
            s3_bucket, 
            broadcaster_name, 
            twitch_client_id, 
            twitch_client_secret
        )
        
        if result:
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'status': 'Clip analytics processed',
                    'clips_analyzed': result.get('clips_analyzed', 0),
                    'top_game': result.get('top_game')
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