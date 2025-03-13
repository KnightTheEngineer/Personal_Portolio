# Importing modules for the app
import os
import time
import json
import hmac
import hashlib
from datetime import datetime
from flask import Flask, request, render_template, redirect, session, jsonify
from authlib.integrations.flask_client import OAuth
import requests

# Importing environment variables
twitch_signing_secret = os.environ.get('TWITCH_SIGNING_SECRET')
client_id = os.environ.get('TWITCH_CLIENT_ID')
client_secret = os.environ.get('TWITCH_CLIENT_SECRET')
callback_url = os.environ.get('TWITCH_CALLBACK_URL')
cookie_secret = os.environ.get('PASSPORT_COOKIE_SECRET')

# Buffer variable to grab tokens when authorization (oauth permissions) and write to file
buffer_string = None

# Define Flask app and set port to 3000
app = Flask(__name__, template_folder='views', static_folder='public')
port = int(os.environ.get('PORT', 3000))

# MIDDLEWARE configuration
app.secret_key = cookie_secret
app.config['SESSION_TYPE'] = 'filesystem'

# OAuth setup
oauth = OAuth(app)
oauth.register(
    name='twitch',
    client_id=client_id,
    client_secret=client_secret,
    access_token_url='https://id.twitch.tv/oauth2/token',
    authorize_url='https://id.twitch.tv/oauth2/authorize',
    api_base_url='https://api.twitch.tv/helix/',
    client_kwargs={'scope': 'bits:read channel:read:goals channel:read:hype_train channel:read:redemptions channel:read:subscriptions'}
)

# ROUTING
@app.route('/')
def home():
    return render_template('pages/home.html')

@app.route('/login')
def login():
    return render_template('pages/login.html')

@app.route('/failure')
def failure():
    return render_template('pages/failure.html')

@app.route('/success')
def success():
    global buffer_string
    with open('tokens/access.csv', 'a') as f:
        f.write(buffer_string + os.linesep)
    return render_template('pages/success.html')

@app.route('/auth/twitch')
def auth_twitch():
    redirect_uri = callback_url
    return oauth.twitch.authorize_redirect(redirect_uri, force_verify='true')

@app.route('/auth/twitch/callback')
def auth_twitch_callback():
    global buffer_string
    
    token = oauth.twitch.authorize_access_token()
    
    # Get user profile
    headers = {
        'Client-ID': client_id,
        'Authorization': f'Bearer {token["access_token"]}'
    }
    resp = requests.get('https://api.twitch.tv/helix/users', headers=headers)
    profile = resp.json()['data'][0]
    
    # Store tokens in session
    session['user'] = profile
    session['access_token'] = token['access_token']
    session['refresh_token'] = token.get('refresh_token', '')
    
    # Create buffer string
    buffer_string = f"{profile['login']},{token['access_token']},{token.get('refresh_token', '')}"
    
    # Print to console
    print(profile)
    print(request.args)
    
    return redirect('/success')

# Verify event received is from Twitch and not anyone else
def verify_twitch_signature(request_data, signature, message_id, timestamp):
    if not twitch_signing_secret:
        print('Twitch signing secret is empty.')
        raise ValueError("Twitch signing secret is empty.")
    
    current_time = int(time.time())
    if abs(current_time - int(timestamp)) > 600:  # needs to be < 10 minutes
        print(f'Verification Failed: timestamp > 10 minutes. Message Id: {message_id}.')
        raise ValueError("Ignore this request.")
    
    # Compute hash from secret, messageId, timestamp
    computed_signature = 'sha256=' + hmac.new(
        twitch_signing_secret.encode('utf-8'),
        msg=(message_id + timestamp + request_data).encode('utf-8'),
        digestmod=hashlib.sha256
    ).hexdigest()
    
    print(f'Message {message_id} Computed Signature: {computed_signature}')
    
    # Check if Twitch signature == our server signature
    if signature != computed_signature:
        raise ValueError("Invalid signature.")
    else:
        print("Verification successful")
        return True

# ASYNC FUNC that handles response from Twitch to /webhooks/callback
@app.route('/webhooks/callback', methods=['POST'])
def webhook_callback():
    message_type = request.headers.get('Twitch-Eventsub-Message-Type')
    message_id = request.headers.get('Twitch-Eventsub-Message-Id')
    message_timestamp = request.headers.get('Twitch-Eventsub-Message-Timestamp')
    message_signature = request.headers.get('Twitch-Eventsub-Message-Signature')
    
    # Get the raw request body
    request_data = request.get_data(as_text=True)
    
    # Verify the signature
    try:
        verify_twitch_signature(request_data, message_signature, message_id, message_timestamp)
    except ValueError as e:
        print(f"Signature verification failed: {str(e)}")
        return "", 403
    
    # Parse the request body as JSON
    data = request.json
    
    # Check if message is a callback verification
    if message_type == 'webhook_callback_verification':
        print("Webhook verified by Twitch")
        return data['challenge'], 200
    
    # Define type and event
    subscription_type = data['subscription']['type']
    event = data['event']
    
    # Define paths and naming convention for different events
    timestamp = int(datetime.now().timestamp())
    
    paths = {
        'channel.follow': f'/home/bitnami/jsons/c_follow_{timestamp}.json',
        'channel.update': f'/home/bitnami/jsons/c_update/c_update_{timestamp}.json',
        'channel.subscribe': f'/home/bitnami/jsons/c_sub/c_sub_{timestamp}.json',
        'channel.subscribe.end': f'/home/bitnami/jsons/c_sub_end/c_sub_end_{timestamp}.json',
        'channel.subscribe.gift': f'/home/bitnami/jsons/c_sub_gift/c_sub_gift_{timestamp}.json',
        'channel.cheer': f'/home/bitnami/jsons/c_cheer/c_cheer_{timestamp}.json',
        'channel.raid': f'/home/bitnami/jsons/c_raid/c_raid_{timestamp}.json',
        'channel.hype_train.begin': f'/home/bitnami/jsons/c_hype_start/c_hype_start_{timestamp}.json',
        'channel.hype_train.end': f'/home/bitnami/jsons/c_hype_end/c_hype_end_{timestamp}.json',
        'stream.online': f'/home/bitnami/jsons/stream_on/stream_on_{timestamp}.json',
        'stream.offline': f'/home/bitnami/jsons/stream_off/stream_off_{timestamp}.json',
        'channel.goal.begin': f'/home/bitnami/jsons/c_goal_start/c_goal_start_{timestamp}.json',
        'channel.goal.end': f'/home/bitnami/jsons/c_goal_end/c_goal_end_{timestamp}.json'
    }
    
    # Write event data to appropriate file
    if subscription_type in paths:
        file_path = paths[subscription_type]
        # Make sure directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        print(f'Writing to a tmp file: {file_path}')
        with open(file_path, 'w') as f:
            json.dump(event, f)
    
    # Print event to console
    print(f'A {subscription_type} event for {event.get("broadcaster_user_name", "unknown")}: {event}')
    
    # Respond to Twitch with status 200
    return "", 200

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=port, debug=True)