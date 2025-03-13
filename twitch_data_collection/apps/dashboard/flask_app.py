import os
import threading
import logging
from flask import Flask, render_template, jsonify, request, send_from_directory
from apps.dashboard.dash_app import create_dash_app

logger = logging.getLogger("twitch_analytics")

def create_html_template(broadcaster_name, aws_bucket_name):
    """Create HTML template file if it doesn't exist"""
    os.makedirs('templates', exist_ok=True)
    index_path = 'templates/index.html'
    
    if not os.path.exists(index_path):
        html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{broadcaster_name} - Twitch Analytics</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.0.2/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body {{ background-color: #121212; color: #f8f9fa; }}
        .card {{ background-color: #1e1e1e; border-color: #2d2d2d; margin-bottom: 20px; }}
        .card-header {{ background-color: #2d2d2d; border-color: #2d2d2d; }}
        .trending-up {{ color: #4caf50; }}
        .trending-down {{ color: #f44336; }}
        .algorithm-insights {{ background-color: #263238; padding: 15px; border-radius: 5px; margin-top: 20px; }}
    </style>
</head>
<body>
    <div class="container mt-4">
        <div class="row">
            <div class="col-12 text-center mb-4">
                <h1>{broadcaster_name} Twitch Analytics</h1>
                <p>View detailed analytics on the <a href="/dashboard/" class="text-info">Dashboard</a></p>
            </div>
        </div>
        
        <div class="row">
            <div class="col-md-6">
                <div class="card">
                    <div class="card-header">Stream Status</div>
                    <div class="card-body">
                        <div id="stream-status"></div>
                    </div>
                </div>
            </div>
            
            <div class="col-md-6">
                <div class="card">
                    <div class="card-header">Quick Stats</div>
                    <div class="card-body">
                        <div id="quick-stats"></div>
                    </div>
                </div>
            </div>
        </div>
        
        <div class="row">
            <div class="col-12">
                <div class="card">
                    <div class="card-header">Recent Events</div>
                    <div class="card-body">
                        <div id="recent-events"></div>
                    </div>
                </div>
            </div>
        </div>

        <div class="row">
            <div class="col-12">
                <div class="card">
                    <div class="card-header">Algorithm Insights</div>
                    <div class="card-body">
                        <div id="algorithm-insights" class="algorithm-insights">
                            <h5>Twitch Algorithm Recommendations</h5>
                            <div id="algorithm-tips"></div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        
        <div class="row mt-4">
            <div class="col-12">
                <div class="card">
                    <div class="card-header">AWS Storage Status</div>
                    <div class="card-body">
                        <div id="aws-status">
                            <p><strong>S3 Bucket:</strong> {aws_bucket_name}</p>
                            <p><strong>Direct Storage:</strong> Enabled (All data is immediately saved to AWS)</p>
                            <p><strong>Storage Path:</strong> s3://{aws_bucket_name}/{broadcaster_name.lower()}/</p>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>
    
    <script>
        // Update metrics every 5 seconds
        function updateMetrics() {{
            fetch('/api/metrics')
                .then(response => response.json())
                .then(data => {{
                    // Update stream status
                    const streamStatus = document.getElementById('stream-status');
                    streamStatus.innerHTML = `
                        <h3>
                            ${{data.is_live ? 
                                '<span class="badge bg-danger">LIVE</span>' : 
                                '<span class="badge bg-secondary">OFFLINE</span>'}}
                        </h3>
                        ${{data.is_live && data.stream_started_at ? 
                            `<p>Started at: ${{data.stream_started_at.replace('T', ' ').substring(0, 19)}}</p>` : 
                            ''}}
                    `;
                    
                    // Update quick stats
                    const quickStats = document.getElementById('quick-stats');
                    quickStats.innerHTML = `
                        <div class="row">
                            <div class="col-6">
                                <p><strong>Current Viewers:</strong> ${{data.current_viewers}}</p>
                                <p><strong>Peak Viewers:</strong> ${{data.peak_viewers}}</p>
                            </div>
                            <div class="col-6">
                                <p><strong>Subscribers:</strong> ${{data.subscriber_count}}</p>
                                <p><strong>Chat Messages:</strong> ${{data.total_chat_messages}}</p>
                            </div>
                        </div>
                    `;
                    
                    // Update recent events
                    const recentEvents = document.getElementById('recent-events');
                    let eventsHtml = '<ul class="list-group">';
                    
                    const events = data.recent_events || [];
                    events.slice(-10).reverse().forEach(event => {{
                        let badgeClass = 'bg-info';
                        if (event.type === 'subscription') badgeClass = 'bg-success';
                        if (event.type === 'raid') badgeClass = 'bg-warning';
                        if (event.type === 'stream') badgeClass = 'bg-danger';
                        
                        eventsHtml += `
                            <li class="list-group-item bg-dark text-light">
                                <span class="badge ${{badgeClass}} me-2">${{event.type.toUpperCase()}}</span>
                                ${{event.message}} - 
                                <small class="text-muted">${{event.timestamp.split('T')[1].substring(0, 8)}}</small>
                            </li>
                        `;
                    }});
                    
                    eventsHtml += '</ul>';
                    recentEvents.innerHTML = eventsHtml;
                    
                    // Update algorithm tips
                    const algorithmTips = document.getElementById('algorithm-tips');
                    
                    // Generate algorithm tips based on current metrics
                    let tips = '<ul class="list-group">';
                    
                    if (data.is_live) {{
                        // Viewer retention tip
                        const viewerRetention = data.viewer_retention || [];
                        if (viewerRetention.length > 5) {{
                            const initialViewers = viewerRetention[0]?.viewer_count || 0;
                            const currentViewers = viewerRetention[viewerRetention.length-1]?.viewer_count || 0;
                            const retentionRate = initialViewers > 0 ? (currentViewers / initialViewers) : 0;
                            
                            if (retentionRate < 0.7) {{
                                tips += `
                                    <li class="list-group-item bg-dark text-light">
                                        <i class="trending-down">▼</i> <strong>Viewer Retention:</strong> 
                                        Retention rate is below target. Consider increasing chat interaction to boost algorithm ranking.
                                    </li>
                                `;
                            }} else {{
                                tips += `
                                    <li class="list-group-item bg-dark text-light">
                                        <i class="trending-up">▲</i> <strong>Viewer Retention:</strong> 
                                        Strong retention rate! Current content is maintaining audience interest.
                                    </li>
                                `;
                            }}
                        }}
                        
                        // Chat engagement tip
                        if (data.chat_messages_per_minute < 5) {{
                            tips += `
                                <li class="list-group-item bg-dark text-light">
                                    <i class="trending-down">▼</i> <strong>Chat Engagement:</strong> 
                                    Low chat activity may reduce algorithm visibility. Try asking engaging questions.
                                </li>
                            `;
                        }} else {{
                            tips += `
                                <li class="list-group-item bg-dark text-light">
                                    <i class="trending-up">▲</i> <strong>Chat Engagement:</strong> 
                                    Good chat activity! This helps with algorithm placement.
                                </li>
                            `;
                        }}
                    }} else {{
                        // Offline recommendations
                        tips += `
                            <li class="list-group-item bg-dark text-light">
                                <strong>Channel Growth:</strong> 
                                Post your next stream schedule to Discord/social media to improve initial viewer count.
                            </li>
                            <li class="list-group-item bg-dark text-light">
                                <strong>Content Planning:</strong> 
                                Review your top clips to identify content that performs well with the algorithm.
                            </li>
                        `;
                    }}
                    
                    tips += '</ul>';
                    algorithmTips.innerHTML = tips;
                }})
                .catch(error => console.error('Error fetching metrics:', error));
        }}
        
        // Initial update and set interval
        updateMetrics();
        setInterval(updateMetrics, 5000);
    </script>
</body>
</html>
        """
        
        with open(index_path, 'w') as f:
            f.write(html_content)
        
        logger.info(f"Created HTML template at {index_path}")

def create_flask_app(broadcaster_name, aws_bucket_name, live_metrics):
    """Create and configure Flask app"""
    # Create HTML template first
    create_html_template(broadcaster_name, aws_bucket_name)
    
    # Create Flask app for the web dashboard
    flask_app = Flask(__name__, 
                  static_folder='static',
                  template_folder='templates')
    
    # Create Dash app for the interactive dashboard
    dash_app = create_dash_app(flask_app, broadcaster_name, live_metrics)
    
    # Define Flask routes
    @flask_app.route('/')
    def index():
        return render_template('index.html')
    
    @flask_app.route('/api/metrics')
    def get_metrics():
        return jsonify(live_metrics)
        
    @flask.route('/static/<path:path>')
    def serve_static(path):
        return send_from_directory('static', path)
    
    return flask_app, dash_app

def start_flask_server(flask_app):
    """Start the Flask server in a separate thread"""
    def run_flask():
        flask_app.run(host='0.0.0.0', port=5000, debug=False)
    
    flask_thread = threading.Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()
    
    logger.info("Flask server started on http://0.0.0.0:5000")