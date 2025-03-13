import logging
import datetime
import json
import io
import pandas as pd
from twitch_data_collection.data_storage import get_from_s3, save_to_s3

logger = logging.getLogger("twitch_analytics")

def generate_daily_report(s3_client, aws_bucket_name, broadcaster_name):
    """Generate a daily analytics report with insights for algorithm optimization"""
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    date_str = yesterday.strftime("%Y%m%d")
    
    # Try to load data from S3
    try:
        # Load chat data
        chat_key = f"{broadcaster_name.lower()}/chat_metrics/daily_{date_str}.csv"
        chat_data = None
        try:
            chat_obj = s3_client.get_object(Bucket=aws_bucket_name, Key=chat_key)
            chat_data = pd.read_csv(io.BytesIO(chat_obj['Body'].read()))
        except Exception as e:
            logger.warning(f"Could not load chat data from S3: {str(e)}")
        
        # Load viewer data
        viewer_key = f"{broadcaster_name.lower()}/viewer_stats/daily_{date_str}.csv"
        viewer_data = None
        try:
            viewer_obj = s3_client.get_object(Bucket=aws_bucket_name, Key=viewer_key)
            viewer_data = pd.read_csv(io.BytesIO(viewer_obj['Body'].read()))
        except Exception as e:
            logger.warning(f"Could not load viewer data from S3: {str(e)}")
        
        # Load subscriber data
        subs_key = f"{broadcaster_name.lower()}/subscribers/daily_{date_str}.csv"
        subs_data = None
        try:
            subs_obj = s3_client.get_object(Bucket=aws_bucket_name, Key=subs_key)
            subs_data = pd.read_csv(io.BytesIO(subs_obj['Body'].read()))
        except Exception as e:
            logger.warning(f"Could not load subscriber data from S3: {str(e)}")
        
        # Load stream metrics
        stream_key = f"{broadcaster_name.lower()}/stream_metrics/daily_{date_str}.csv"
        stream_data = None
        try:
            stream_obj = s3_client.get_object(Bucket=aws_bucket_name, Key=stream_key)
            stream_data = pd.read_csv(io.BytesIO(stream_obj['Body'].read()))
        except Exception as e:
            logger.warning(f"Could not load stream metrics from S3: {str(e)}")
        
        # Generate report
        report = {
            'date': date_str,
            'channel': broadcaster_name,
            'summary': {},
            'insights': [],
            'recommendations': []
        }
        
        # Process subscriber data
        if subs_data is not None and not subs_data.empty:
            report['summary']['new_subscribers'] = len(subs_data)
            report['summary']['gift_subs'] = subs_data['is_gift'].sum() if 'is_gift' in subs_data.columns else 0
            
            # Tier distribution
            if 'tier' in subs_data.columns:
                tier_counts = subs_data['tier'].value_counts().to_dict()
                report['summary']['tier_distribution'] = tier_counts
        
        # Process chat data
        if chat_data is not None and not chat_data.empty:
            report['summary']['total_chat_messages'] = len(chat_data)
            report['summary']['unique_chatters'] = len(chat_data['sender'].unique()) if 'sender' in chat_data.columns else 0
            
            # Analyze chat engagement patterns
            if 'timestamp' in chat_data.columns:
                chat_data['timestamp'] = pd.to_datetime(chat_data['timestamp'])
                chat_data['hour'] = chat_data['timestamp'].dt.hour
                
                # Group by hour and count messages
                hourly_counts = chat_data.groupby('hour').size()
                if not hourly_counts.empty:
                    peak_hour = hourly_counts.idxmax()
                    report['insights'].append({
                        'type': 'peak_engagement',
                        'message': f"Peak chat engagement occurs around {peak_hour}:00",
                        'value': int(peak_hour)
                    })
        
        # Process viewer data
        if viewer_data is not None and not viewer_data.empty:
            report['summary']['peak_viewers'] = viewer_data['viewer_count'].max() if 'viewer_count' in viewer_data.columns else 0
            report['summary']['avg_viewers'] = viewer_data['viewer_count'].mean() if 'viewer_count' in viewer_data.columns else 0
            
            # Analyze viewer retention
            if 'timestamp' in viewer_data.columns and len(viewer_data) > 10:
                viewer_data['timestamp'] = pd.to_datetime(viewer_data['timestamp'])
                viewer_data = viewer_data.sort_values('timestamp')
                
                # Calculate viewer retention rate
                start_viewers = viewer_data['viewer_count'].iloc[0]
                mid_viewers = viewer_data['viewer_count'].iloc[len(viewer_data)//2]
                end_viewers = viewer_data['viewer_count'].iloc[-1]
                
                retention_mid = (mid_viewers / start_viewers) * 100 if start_viewers > 0 else 0
                retention_end = (end_viewers / start_viewers) * 100 if start_viewers > 0 else 0
                
                report['summary']['retention_mid_percent'] = retention_mid
                report['summary']['retention_end_percent'] = retention_end
                
                # Add insights based on retention
                if retention_end < 50:
                    report['insights'].append({
                        'type': 'retention_issue',
                        'message': "Strong viewer drop-off detected throughout stream",
                        'value': retention_end
                    })
                    report['recommendations'].append({
                        'type': 'content_pacing',
                        'message': "Consider introducing new content segments every 30 minutes to maintain viewer interest and improve algorithm ranking"
                    })
                elif retention_end > 80:
                    report['insights'].append({
                        'type': 'retention_positive',
                        'message': "Excellent viewer retention throughout stream",
                        'value': retention_end
                    })
                    report['recommendations'].append({
                        'type': 'content_strategy',
                        'message': "This content format performs well for retention. Consider creating more similar content to maintain algorithm favor."
                    })
        
        # Process stream metrics
        if stream_data is not None and not stream_data.empty:
            report['summary']['stream_duration'] = stream_data['stream_duration'].max() if 'stream_duration' in stream_data.columns else 0
            
            # Analyze potential algorithm impact
            if 'viewer_count' in stream_data.columns and len(stream_data) > 5:
                stream_data = stream_data.sort_values('timestamp') if 'timestamp' in stream_data.columns else stream_data
                # Check viewer growth pattern
                viewer_growth = stream_data['viewer_count'].pct_change().mean() * 100
                report['summary']['avg_viewer_growth_pct'] = viewer_growth
                
                if viewer_growth > 5:
                    report['insights'].append({
                        'type': 'algorithm_boost',
                        'message': "Strong positive viewer growth rate indicates algorithm favor",
                        'value': viewer_growth
                    })
                    report['recommendations'].append({
                        'type': 'stream_duration',
                        'message': "Consider extending streams by 30-60 minutes to capitalize on algorithm boost and increase discoverability"
                    })
                elif viewer_growth < -5:
                    report['insights'].append({
                        'type': 'algorithm_concern',
                        'message': "Negative viewer trend may indicate algorithm deprioritization",
                        'value': viewer_growth
                    })
                    report['recommendations'].append({
                        'type': 'content_variety',
                        'message': "Increase content variety and engagement prompts to boost algorithm metrics"
                    })
        
        # Save report directly to S3
        report_key = f"{broadcaster_name.lower()}/reports/daily_report_{date_str}.json"
        save_to_s3(s3_client, aws_bucket_name, report_key, json.dumps(report, indent=4))
        
        logger.info(f"Generated daily report for {date_str} and saved directly to S3")
        return report
    
    except Exception as e:
        logger.error(f"Error generating daily report: {str(e)}")
        return None