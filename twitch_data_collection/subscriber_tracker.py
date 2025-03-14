import logging
import datetime
import json
from twitch_data_collection.data_storage import save_to_s3

logger = logging.getLogger("twitch_analytics")

class SubscriberTracker:
    def __init__(self, twitch, broadcaster_id, s3_client, aws_bucket_name, broadcaster_name, live_metrics):
        self.twitch = twitch
        self.broadcaster_id = broadcaster_id
        self.s3_client = s3_client
        self.aws_bucket_name = aws_bucket_name
        self.broadcaster_name = broadcaster_name
        self.live_metrics = live_metrics

    async def get_subscriber_count(self):
        """Get the current subscriber count with immediate S3 save"""
        try:
            # Get subscriber count from Twitch API
            sub_response = self.twitch.get_broadcaster_subscriptions(broadcaster_id=self.broadcaster_id)
            timestamp = datetime.datetime.now().isoformat()
            
            if 'total' in sub_response:
                sub_count = sub_response['total']
                self.live_metrics['subscriber_count'] = sub_count
                logger.info(f"Current subscriber count: {self.live_metrics['subscriber_count']}")
                
                # Save subscriber count data directly to S3
                sub_count_data = {
                    'timestamp': timestamp,
                    'subscriber_count': sub_count
                }
                
                date_str = datetime.datetime.now().strftime("%Y%m%d")
                s3_key = f"{self.broadcaster_name.lower()}/subscribers/{date_str}/count_{datetime.datetime.now().strftime('%H%M%S')}.json"
                save_to_s3(self.s3_client, self.aws_bucket_name, s3_key, json.dumps(sub_count_data))
                
                return sub_count
        except Exception as e:
            logger.error(f"Error getting subscriber count: {str(e)}")
            return None