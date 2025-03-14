import logging
import json
import datetime

logger = logging.getLogger("twitch_analytics")

async def analyze_top_clips(twitch, broadcaster_id, s3_client, aws_bucket_name, broadcaster_name):
    """Analyze top clips and save results directly to S3"""
    try:
        # Get top clips for the channel
        clips = twitch.get_clips(broadcaster_id=broadcaster_id, first=20)
        
        if 'data' in clips and clips['data']:
            date_str = datetime.datetime.now().strftime("%Y%m%d")
            
            # Extract relevant clip data
            clip_data = []
            for clip in clips['data']:
                clip_data.append({
                    'id': clip['id'],
                    'title': clip['title'],
                    'created_at': clip['created_at'],
                    'duration': clip['duration'],
                    'view_count': clip['view_count'],
                    'game_id': clip['game_id'],
                    'thumbnail_url': clip['thumbnail_url']
                })
            
            # Save clips data directly to S3
            from twitch_data_collection.data_storage import save_to_s3
            clips_key = f"{broadcaster_name.lower()}/clip_analysis/top_clips_{date_str}.json"
            save_to_s3(s3_client, aws_bucket_name, clips_key, json.dumps(clip_data, indent=4))
            
            # Analyze clips for insights
            if clip_data:
                # Sort by view count
                sorted_clips = sorted(clip_data, key=lambda x: x['view_count'], reverse=True)
                
                # Find most popular game
                game_counts = {}
                for clip in sorted_clips:
                    game_id = clip.get('game_id', 'unknown')
                    game_counts[game_id] = game_counts.get(game_id, 0) + 1
                
                most_popular_game = max(game_counts.items(), key=lambda x: x[1])[0]
                
                # Find average clip duration
                avg_duration = sum(clip['duration'] for clip in sorted_clips) / len(sorted_clips)
                
                # Log insights
                logger.info(f"Top clip analysis: Most popular game ID: {most_popular_game}")
                logger.info(f"Top clip analysis: Average clip duration: {avg_duration:.2f} seconds")
                
                # Save analysis results directly to S3
                analysis_results = {
                    'date': date_str,
                    'most_popular_game': most_popular_game,
                    'avg_duration': avg_duration,
                    'top_5_clips': sorted_clips[:5]
                }
                
                analysis_key = f"{broadcaster_name.lower()}/clip_analysis/analysis_{date_str}.json"
                save_to_s3(s3_client, aws_bucket_name, analysis_key, json.dumps(analysis_results, indent=4))
                
                # Return insights for potential recommendations
                return analysis_results
        
        return None
    except Exception as e:
        logger.error(f"Error analyzing top clips: {str(e)}")
        return None