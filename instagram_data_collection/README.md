# InstagramDataCollection
This code provides a comprehensive solution for tracking Instagram analytics and uploading the data to AWS S3. Here's what it does:

Authentication: Uses environment variables for Instagram API and AWS credentials, loaded via python-dotenv for security.
Instagram Analytics Tracking:

Collects account information (followers, following)
Gathers detailed insights for individual posts (engagement, impressions, reach)
Tracks story performance metrics
Handles rate limiting to avoid API restrictions


Data Organization:

Structures data into organized DataFrames
Timestamp-based file naming for easy tracking
Creates separate CSVs for account, media posts, and stories


AWS Integration:

Uploads CSVs to S3 with organized folder structure
Handles error reporting for failed uploads



To use this code, you'll need to:

Create a .env file with your credentials:
INSTAGRAM_ACCESS_TOKEN=your_access_token
INSTAGRAM_BUSINESS_ID=your_business_id
AWS_ACCESS_KEY=your_aws_key
AWS_SECRET_KEY=your_aws_secret
AWS_BUCKET_NAME=your_bucket_name
AWS_REGION=us-east-1

Install required packages:

pip install requests pandas boto3 python-dotenv

un the script periodically to collect analytics data.
