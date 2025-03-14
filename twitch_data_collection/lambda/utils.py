"""
Utility functions for Twitch analytics Lambda operations.

This module provides helper functions for processing and analyzing
Twitch data in AWS Lambda functions, including data transformation,
report generation, and S3 storage operations.
"""

import json
import logging
import datetime
import boto3
import io
import pandas as pd
from typing import Dict, List, Any, Optional, Union, Tuple
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def process_twitch_data(broadcaster_name: str, viewer_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Process Twitch viewer data to extract metrics.
    
    Analyzes viewer count data points to calculate average viewers,
    peak viewers, retention rate, and growth trends.
    
    Args:
        broadcaster_name (str): Name of the broadcaster/channel.
        viewer_data (List[Dict[str, Any]]): List of viewer count snapshots.
            Each item should have:
            - timestamp: ISO format timestamp
            - viewer_count: Number of viewers at that time
    
    Returns:
        Dict[str, Any]: Dictionary containing processed metrics:
            - timestamp: Processing timestamp
            - broadcaster_name: Channel name
            - viewer_metrics: Dictionary of calculated metrics:
                - avg_viewers: Average viewer count
                - peak_viewers: Maximum viewer count
                - retention_pct: End viewers as percentage of start viewers
                - avg_growth_rate_pct: Average percentage growth between snapshots
    
    Example:
        >>> data = [
        ...     {"timestamp": "2025-01-01T01:00:00Z", "viewer_count": 100},
        ...     {"timestamp": "2025-01-01T02:00:00Z", "viewer_count": 150}
        ... ]
        >>> process_twitch_data("example_streamer", data)
        {
            'timestamp': '2025-01-01T03:00:00Z',
            'broadcaster_name': 'example_streamer',
            'viewer_metrics': {
                'avg_viewers': 125.0,
                'peak_viewers': 150,
                'retention_pct': 150.0,
                'avg_growth_rate_pct': 50.0
            }
        }
    """
    metrics = {
        'timestamp': datetime.datetime.now().isoformat(),
        'broadcaster_name': broadcaster_name,
        'viewer_metrics': {}
    }
    
    try:
        # Process viewer counts
        if viewer_data and isinstance(viewer_data, list) and len(viewer_data) > 0:
            # Calculate average viewers
            avg_viewers = sum(item.get('viewer_count', 0) for item in viewer_data) / len(viewer_data)
            metrics['viewer_metrics']['avg_viewers'] = avg_viewers
            
            # Calculate peak viewers
            peak_viewers = max(item.get('viewer_count', 0) for item in viewer_data)
            metrics['viewer_metrics']['peak_viewers'] = peak_viewers
            
            # Calculate retention
            if len(viewer_data) >= 2:
                start_viewers = viewer_data[0].get('viewer_count', 0)
                end_viewers = viewer_data[-1].get('viewer_count', 0)
                
                if start_viewers > 0:
                    retention = (end_viewers / start_viewers) * 100
                    metrics['viewer_metrics']['retention_pct'] = retention
                else:
                    metrics['viewer_metrics']['retention_pct'] = 0
            
            # Calculate growth rate
            # Sort by timestamp to ensure chronological order
            sorted_data = sorted(viewer_data, key=lambda x: x.get('timestamp', ''))
            if len(sorted_data) >= 2:
                growth_rates = []
                for i in range(1, len(sorted_data)):
                    prev_count = sorted_data[i-1].get('viewer_count', 0)
                    curr_count = sorted_data[i].get('viewer_count', 0)
                    
                    if prev_count > 0:
                        growth_rate = ((curr_count - prev_count) / prev_count) * 100
                        growth_rates.append(growth_rate)
                
                if growth_rates:
                    avg_growth_rate = sum(growth_rates) / len(growth_rates)
                    metrics['viewer_metrics']['avg_growth_rate_pct'] = avg_growth_rate
    except Exception as e:
        logger.error(f"Error processing viewer data: {str(e)}")
    
    return metrics

def generate_metrics_report(
    broadcaster_name: str, 
    metrics: Dict[str, Any], 
    stream_duration: int,
    stream_id: str
) -> Dict[str, Any]:
    """
    Generate a comprehensive report from processed stream metrics.
    
    Analyzes metrics to create insights and recommendations for improving
    stream performance and viewer engagement.
    
    Args:
        broadcaster_name (str): Name of the broadcaster/channel.
        metrics (Dict[str, Any]): Processed metrics from process_twitch_data().
        stream_duration (int): Stream duration in minutes.
        stream_id (str): Unique identifier for the stream.
    
    Returns:
        Dict[str, Any]: Comprehensive report containing:
            - timestamp: Report generation timestamp
            - broadcaster_name: Channel name
            - stream_id: Unique stream identifier
            - stream_duration_minutes: Length of the stream
            - metrics: Viewer metrics from the metrics parameter
            - insights: List of automatically generated insights
            - recommendations: List of improvement recommendations
    
    Example:
        >>> metrics = {
        ...     'viewer_metrics': {
        ...         'avg_viewers': 125.0,
        ...         'peak_viewers': 150,
        ...         'retention_pct': 85.0
        ...     }
        ... }
        >>> generate_metrics_report("example_streamer", metrics, 120, "12345678")
        {
            'timestamp': '2025-01-01T03:00:00Z',
            'broadcaster_name': 'example_streamer',
            'stream_id': '12345678',
            'stream_duration_minutes': 120,
            'metrics': {
                'avg_viewers': 125.0,
                'peak_viewers': 150,
                'retention_pct': 85.0
            },
            'insights': [
                {
                    'type': 'retention_positive',
                    'message': 'Excellent viewer retention throughout stream',
                    'value': 85.0
                }
            ],
            'recommendations': [
                {
                    'type': 'content_strategy',
                    'message': 'This content format performs well for retention...'
                }
            ]
        }
    """
    report = {
        'timestamp': datetime.datetime.now().isoformat(),
        'broadcaster_name': broadcaster_name,
        'stream_id': stream_id,
        'stream_duration_minutes': stream_duration,
        'metrics': metrics.get('viewer_metrics', {}),
        'insights': [],
        'recommendations': []
    }
    
    # Generate insights and recommendations based on metrics
    try:
        viewer_metrics = metrics.get('viewer_metrics', {})
        
        # Retention insights
        retention = viewer_metrics.get('retention_pct', 0)
        if retention < 50:
            report['insights'].append({
                'type': 'retention_issue',
                'message': "Strong viewer drop-off detected throughout stream",
                'value': retention
            })
            report['recommendations'].append({
                'type': 'content_pacing',
                'message': "Consider introducing new content segments every 30 minutes to maintain viewer interest"
            })
        elif retention > 80:
            report['insights'].append({
                'type': 'retention_positive',
                'message': "Excellent viewer retention throughout stream",
                'value': retention
            })
            report['recommendations'].append({
                'type': 'content_strategy',
                'message': "This content format performs well for retention. Consider creating more similar content."
            })
        
        # Growth rate insights
        growth_rate = viewer_metrics.get('avg_growth_rate_pct', 0)
        if growth_rate > 5:
            report['insights'].append({
                'type': 'algorithm_boost',
                'message': "Strong positive viewer growth rate indicates algorithm favor",
                'value': growth_rate
            })
            report['recommendations'].append({
                'type': 'stream_duration',
                'message': "Consider extending streams by 30-60 minutes to capitalize on algorithm boost"
            })
        elif growth_rate < -5:
            report['insights'].append({
                'type': 'algorithm_concern',
                'message': "Negative viewer trend may indicate algorithm deprioritization",
                'value': growth_rate
            })
            report['recommendations'].append({
                'type': 'content_variety',
                'message': "Increase content variety and engagement prompts to boost algorithm metrics"
            })
    
    except Exception as e:
        logger.error(f"Error generating report: {str(e)}")
    
    return report

def save_processed_data(
    s3_client: boto3.client, 
    bucket_name: str, 
    broadcaster_name: str, 
    data: Dict[str, Any]
) -> str:
    """
    Save processed data to S3.
    
    Args:
        s3_client (boto3.client): Initialized S3 client.
        bucket_name (str): Name of the S3 bucket.
        broadcaster_name (str): Name of the broadcaster/channel.
        data (Dict[str, Any]): Processed data/report to save.
    
    Returns:
        str: S3 key where the data was saved.
    
    Raises:
        Exception: If there's an error saving to S3, it logs the error
                  but doesn't raise it to the calling function.
    """
    try:
        date_str = datetime.datetime.now().strftime('%Y%m%d')
        timestamp = datetime.datetime.now().strftime('%H%M%S')
        
        # Save as JSON
        key = f"{broadcaster_name.lower()}/reports/{date_str}/stream_report_{timestamp}.json"
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=json.dumps(data, indent=4),
            ContentType='application/json'
        )
        
        logger.info(f"Saved report to S3: {key}")
        return key
    
    except Exception as e:
        logger.error(f"Error saving data to S3: {str(e)}")
        return ""

def get_s3_data(s3_client: boto3.client, bucket_name: str, key: str) -> Optional[str]:
    """
    Get data from S3.
    
    Args:
        s3_client (boto3.client): Initialized S3 client.
        bucket_name (str): Name of the S3 bucket.
        key (str): S3 object key (path within the bucket).
    
    Returns:
        Optional[str]: The content as a string if successful, None otherwise.
    
    Raises:
        ClientError: If there's an error with the S3 client request.
                    The error is logged but not propagated.
    """
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        return response['Body'].read().decode('utf-8')
    except ClientError as e:
        logger.error(f"Error getting data from S3: {str(e)}")
        return None

def generate_analytics_report(
    s3_client: boto3.client, 
    bucket_name: str, 
    broadcaster_name: str, 
    date_str: str, 
    report_type: str = 'daily'
) -> Optional[Dict[str, Any]]:
    """
    Generate analytics report from S3 data.
    
    Creates comprehensive analytics reports by aggregating data from
    multiple sources in S3. Supports different report types (daily,
    weekly, monthly).
    
    Args:
        s3_client (boto3.client): Initialized S3 client.
        bucket_name (str): Name of the S3 bucket.
        broadcaster_name (str): Name of the broadcaster/channel.
        date_str (str): Date string in format YYYYMMDD.
        report_type (str, optional): Type of report to generate.
                                    Defaults to 'daily'.
    
    Returns:
        Optional[Dict[str, Any]]: The generated report with detailed metrics,
                                or None if the report couldn't be generated.
    
    Raises:
        Exception: Any errors during report generation are logged but not raised.
    
    Example Return:
        {
            'date': '20250101',
            'broadcaster_name': 'example_streamer',
            'report_type': 'daily',
            'report_key': 's3/path/to/report.json',
            'summary': {
                'total_chat_messages': 1250,
                'unique_chatters': 85,
                'peak_viewers': 150,
                'avg_viewers': 125.0
            },
            'insights': [
                {
                    'type': 'viewer_milestone',
                    'message': 'Reached 150 peak viewers!'
                }
            ],
            'recommendations': [...]
        }
    """
    try:
        # Different report types would have different logic
        if report_type == 'daily':
            # Load chat data
            chat_key = f"{broadcaster_name.lower()}/chat_metrics/daily_{date_str}.csv"
            chat_data = None
            try:
                chat_obj = s3_client.get_object(Bucket=bucket_name, Key=chat_key)
                chat_data = pd.read_csv(io.BytesIO(chat_obj['Body'].read()))
            except Exception as e:
                logger.warning(f"Could not load chat data from S3: {str(e)}")
            
            # Load viewer data
            viewer_key = f"{broadcaster_name.lower()}/viewer_stats/daily_{date_str}.csv"
            viewer_data = None
            try:
                viewer_obj = s3_client.get_object(Bucket=bucket_name, Key=viewer_key)
                viewer_data = pd.read_csv(io.BytesIO(viewer_obj['Body'].read()))
            except Exception as e:
                logger.warning(f"Could not load viewer data from S3: {str(e)}")
            
            # Check if we have enough data to generate a report
            if chat_data is None and viewer_data is None:
                logger.warning(f"Insufficient data to generate report for {date_str}")
                return None
            
            # Process data and generate report
            report = {
                'date': date_str,
                'broadcaster_name': broadcaster_name,
                'report_type': 'daily',
                'summary': {},
                'insights': [],
                'recommendations': []
            }
            
            # Add metrics based on available data
            if chat_data is not None:
                report['summary']['total_chat_messages'] = len(chat_data)
                report['summary']['unique_chatters'] = len(chat_data['sender'].unique()) if 'sender' in chat_data.columns else 0
                
                # Add chat engagement analysis
                if 'timestamp' in chat_data.columns and len(chat_data) > 0:
                    chat_data['timestamp'] = pd.to_datetime(chat_data['timestamp'])
                    chat_data['hour'] = chat_data['timestamp'].dt.hour
                    
                    # Find peak chat hour
                    hourly_counts = chat_data.groupby('hour').size()
                    if not hourly_counts.empty:
                        peak_hour = hourly_counts.idxmax()
                        report['summary']['peak_chat_hour'] = int(peak_hour)
                        report['insights'].append({
                            'type': 'peak_engagement',
                            'message': f"Peak chat engagement occurs around {peak_hour}:00"
                        })
            
            if viewer_data is not None:
                report['summary']['peak_viewers'] = viewer_data['viewer_count'].max() if 'viewer_count' in viewer_data.columns else 0
                report['summary']['avg_viewers'] = viewer_data['viewer_count'].mean() if 'viewer_count' in viewer_data.columns else 0
                
                # Generate viewer insights
                if 'viewer_count' in viewer_data.columns:
                    peak_viewers = report['summary'].get('peak_viewers', 0)
                    
                    if peak_viewers > 100:
                        report['insights'].append({
                            'type': 'viewer_milestone',
                            'message': f"Reached {peak_viewers} peak viewers!"
                        })
                    
                    # Check for viewer retention if we have timestamps
                    if 'timestamp' in viewer_data.columns and len(viewer_data) > 10:
                        viewer_data['timestamp'] = pd.to_datetime(viewer_data['timestamp'])
                        viewer_data = viewer_data.sort_values('timestamp')
                        
                        # Calculate retention metrics
                        start_viewers = viewer_data['viewer_count'].iloc[0]
                        end_viewers = viewer_data['viewer_count'].iloc[-1]
                        
                        if start_viewers > 0:
                            retention = (end_viewers / start_viewers) * 100
                            report['summary']['viewer_retention_pct'] = retention
                            
                            if retention < 50:
                                report['insights'].append({
                                    'type': 'retention_issue',
                                    'message': "Strong viewer drop-off detected"
                                })
                                report['recommendations'].append({
                                    'type': 'content_pacing',
                                    'message': "Consider introducing new content segments every 30 minutes"
                                })
                            elif retention > 80:
                                report['insights'].append({
                                    'type': 'retention_positive',
                                    'message': "Excellent viewer retention"
                                })
            
            # Save report to S3
            report_key = f"{broadcaster_name.lower()}/reports/daily_report_{date_str}.json"
            s3_client.put_object(
                Bucket=bucket_name,
                Key=report_key,
                Body=json.dumps(report, indent=4),
                ContentType='application/json'
            )
            
            report['report_key'] = report_key
            return report
            
        elif report_type == 'weekly':
            # Implementation for weekly reports
            # Would aggregate data across multiple days
            logger.info("Weekly report generation not yet implemented")
            return None
            
        elif report_type == 'monthly':
            # Implementation for monthly reports
            # Would provide month-over-month comparisons and trends
            logger.info("Monthly report generation not yet implemented")
            return None
        
        else:
            logger.warning(f"Unknown report type: {report_type}")
            return None
    
    except Exception as e:
        logger.error(f"Error generating analytics report: {str(e)}")
        return None

def analyze_twitch_clips(
    s3_client: boto3.client, 
    bucket_name: str, 
    broadcaster_name: str, 
    client_id: str, 
    client_secret: str,
    clip_limit: int = 20
) -> Optional[Dict[str, Any]]:
    """
    Analyze Twitch clips using Twitch API.
    
    Fetches and analyzes top clips for a broadcaster to identify trends
    and popular content.
    
    Args:
        s3_client (boto3.client): Initialized S3 client.
        bucket_name (str): Name of the S3 bucket.
        broadcaster_name (str): Name of the broadcaster/channel.
        client_id (str): Twitch API client ID.
        client_secret (str): Twitch API client secret.
        clip_limit (int, optional): Number of clips to analyze. Defaults to 20.
    
    Returns:
        Optional[Dict[str, Any]]: Analysis results containing:
            - clips_analyzed: Number of clips processed
            - top_game: Most popular game across analyzed clips
            - avg_duration: Average clip duration in seconds
            - clips: List of processed clip data
            - report_key: S3 key where the full report is stored
        
        Returns None if analysis couldn't be completed.
    
    Raises:
        Exception: Any errors during clip analysis are logged but not raised.
    
    Note:
        This implementation requires setting up proper Twitch API authentication
        as specified in Twitch API v5 documentation.
    """
    try:
        # In a real implementation, this would initialize the Twitch API client
        # and make API calls to fetch clips for the broadcaster

        # Twitch API authentication
        from twitchAPI.twitch import Twitch
        twitch = Twitch(client_id, client_secret)
        twitch.authenticate_app([])
        
        # Get broadcaster ID
        users = twitch.get_users(logins=[broadcaster_name])
        if not users or 'data' not in users or not users['data']:
            logger.error(f"Broadcaster not found: {broadcaster_name}")
            return None
            
        broadcaster_id = users['data'][0]['id']
        
        # Get top clips
        clips_response = twitch.get_clips(broadcaster_id=broadcaster_id, first=clip_limit)
        
        if not clips_response or 'data' not in clips_response:
            logger.warning(f"No clips found for broadcaster: {broadcaster_name}")
            return None
            
        clips = clips_response['data']
        
        # Process clip data
        processed_clips = []
        game_counts = {}
        total_duration = 0
        total_views = 0
        
        for clip in clips:
            # Extract relevant clip data
            clip_data = {
                'id': clip['id'],
                'title': clip['title'],
                'created_at': clip['created_at'],
                'duration': clip['duration'],
                'view_count': clip['view_count'],
                'game_id': clip.get('game_id', 'unknown')
            }
            
            processed_clips.append(clip_data)
            
            # Update duration total
            total_duration += clip['duration']
            
            # Update view total
            total_views += clip['view_count']
            
            # Update game counts
            game_id = clip.get('game_id', 'unknown')
            game_counts[game_id] = game_counts.get(game_id, 0) + 1
        
        # Find most popular game
        top_game = max(game_counts.items(), key=lambda x: x[1])[0] if game_counts else None
        
        # Calculate averages
        clip_count = len(processed_clips)
        avg_duration = total_duration / clip_count if clip_count > 0 else 0
        avg_views = total_views / clip_count if clip_count > 0 else 0
        
        # Prepare result
        result = {
            'clips_analyzed': clip_count,
            'top_game': top_game,
            'avg_duration': round(avg_duration, 2),
            'avg_views': round(avg_views, 2),
            'clips': processed_clips[:5],  # Include top 5 clips in the response
            'analysis_date': datetime.datetime.now().isoformat()
        }
        
        # Save full result to S3
        date_str = datetime.datetime.now().strftime('%Y%m%d')
        key = f"{broadcaster_name.lower()}/clip_analysis/clips_report_{date_str}.json"
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=json.dumps(result, indent=4),
            ContentType='application/json'
        )
        
        # Add the S3 key to the result
        result['report_key'] = key
        
        return result
    
    except Exception as e:
        logger.error(f"Error analyzing Twitch clips: {str(e)}")
        return None

def format_time_duration(seconds: int) -> str:
    """
    Format a duration in seconds as a human-readable string.
    
    Args:
        seconds (int): Duration in seconds.
    
    Returns:
        str: Formatted duration string in format "HH:MM:SS".
    
    Example:
        >>> format_time_duration(3665)
        '1:01:05'
    """
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    if hours > 0:
        return f"{hours}:{minutes:02d}:{seconds:02d}"
    else:
        return f"{minutes}:{seconds:02d}"

def parse_twitch_timestamp(timestamp: str) -> datetime.datetime:
    """
    Parse a Twitch API timestamp into a Python datetime object.
    
    Args:
        timestamp (str): ISO-formatted timestamp string from Twitch API.
    
    Returns:
        datetime.datetime: Parsed datetime object.
        
    Raises:
        ValueError: If the timestamp format is invalid.
        
    Note:
        Twitch API returns timestamps in ISO 8601 format with 'Z' for UTC timezone.
    """
    # Replace 'Z' with +00:00 to make it compatible with fromisoformat
    if timestamp.endswith('Z'):
        timestamp = timestamp[:-1] + '+00:00'
    
    return datetime.datetime.fromisoformat(timestamp)

def get_viewer_growth_rate(
    current_viewers: int, 
    previous_viewers: int
) -> Tuple[float, str]:
    """
    Calculate viewer growth rate and provide a status assessment.
    
    Args:
        current_viewers (int): Current viewer count.
        previous_viewers (int): Previous viewer count for comparison.
    
    Returns:
        Tuple[float, str]: Growth rate percentage and status assessment
                          ('growing', 'stable', or 'declining').
    
    Example:
        >>> get_viewer_growth_rate(150, 100)
        (50.0, 'growing')
    """
    if previous_viewers <= 0:
        return 0.0, 'stable'
    
    growth_rate = ((current_viewers - previous_viewers) / previous_viewers) * 100
    
    if growth_rate > 5:
        status = 'growing'
    elif growth_rate < -5:
        status = 'declining'
    else:
        status = 'stable'
    
    return growth_rate, status