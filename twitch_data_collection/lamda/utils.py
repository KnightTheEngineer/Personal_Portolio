import json
import logging
import datetime
import boto3
import io
import pandas as pd
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def process_twitch_data(broadcaster_name, viewer_data):
    """Process Twitch viewer data to extract metrics"""
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

def generate_metrics_report(broadcaster_name, metrics, stream_duration):
    """Generate a report from processed metrics"""
    report = {
        'timestamp': datetime.datetime.now().isoformat(),
        'broadcaster_name': broadcaster_name,
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

def save_processed_data(s3_client, bucket_name, broadcaster_name, data):
    """Save processed data to S3"""
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
        return None

def get_s3_data(s3_client, bucket_name, key):
    """Get data from S3"""
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        return response['Body'].read().decode('utf-8')
    except ClientError as e:
        logger.error(f"Error getting data from S3: {str(e)}")
        return None

def generate_analytics_report(s3_client, bucket_name, broadcaster_name, date_str, report_type='daily'):
    """Generate analytics report from S3 data"""
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
            
            if viewer_data is not None:
                report['summary']['peak_viewers'] = viewer_data['viewer_count'].max() if 'viewer_count' in viewer_data.columns else 0
                report['summary']['avg_viewers'] = viewer_data['viewer_count'].mean() if 'viewer_count' in viewer_data.columns else 0
            
            # Generate insights (simplified example)
            if viewer_data is not None and 'viewer_count' in viewer_data.columns:
                peak_viewers = report['summary'].get('peak_viewers', 0)
                if peak_viewers > 100:
                    report['insights'].append({
                        'type': 'viewer_milestone',
                        'message': f"Reached {peak_viewers} peak viewers!"
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
            # Implementation for weekly reports would go here
            pass
            
        elif report_type == 'monthly':
            # Implementation for monthly reports would go here
            pass
            
        return None
    
    except Exception as e:
        logger.error(f"Error generating analytics report: {str(e)}")
        return None

def analyze_twitch_clips(s3_client, bucket_name, broadcaster_name, client_id, client_secret):
    """Analyze Twitch clips using Twitch API"""
    try:
        # In a real implementation, this would initialize Twitch API
        # and fetch clips for the broadcaster
        
        # Example placeholder result
        result = {
            'clips_analyzed': 20,
            'top_game': 'Minecraft',
            'clips': []
        }
        
        # Save result to S3
        date_str = datetime.datetime.now().strftime('%Y%m%d')
        key = f"{broadcaster_name.lower()}/clip_analysis/clips_report_{date_str}.json"
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=json.dumps(result, indent=4),
            ContentType='application/json'
        )
        
        return result
    
    except Exception as e:
        logger.error(f"Error analyzing Twitch clips: {str(e)}")
        return None