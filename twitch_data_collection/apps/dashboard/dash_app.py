import datetime
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import load_figure_template
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def create_dash_app(flask_app, broadcaster_name, live_metrics):
    """Create and configure Dash app for interactive dashboard"""
    # Create Dash app
    dash_app = dash.Dash(
        __name__,
        server=flask_app,
        url_base_pathname='/dashboard/',
        external_stylesheets=[dbc.themes.DARKLY]
    )
    load_figure_template('darkly')
    
    # Set up the Dash layout
    dash_app.layout = dbc.Container([
        dbc.Row([
            dbc.Col([
                html.H1(f"{broadcaster_name} - Twitch Analytics", className="text-center mb-4"),
                html.Div(id="stream-status-badge", className="text-center mb-3"),
            ], width=12)
        ]),
        
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Current Stream Stats"),
                    dbc.CardBody([
                        dbc.Row([
                            dbc.Col([
                                html.H5("Current Viewers"),
                                html.H3(id="current-viewers", children="0"),
                            ], width=4),
                            dbc.Col([
                                html.H5("Peak Viewers"),
                                html.H3(id="peak-viewers", children="0"),
                            ], width=4),
                            dbc.Col([
                                html.H5("Stream Duration"),
                                html.H3(id="stream-duration", children="0:00"),
                            ], width=4),
                        ]),
                    ]),
                ], className="mb-4"),
            ], width=12),
        ]),
        
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Subscriber Metrics"),
                    dbc.CardBody([
                        dbc.Row([
                            dbc.Col([
                                html.H5("Total Subscribers"),
                                html.H3(id="total-subscribers", children="0"),
                            ], width=6),
                            dbc.Col([
                                html.H5("New Subs Today"),
                                html.H3(id="new-subs-today", children="0"),
                            ], width=6),
                        ]),
                        html.Hr(),
                        html.H5("Recent Subscribers"),
                        html.Div(id="recent-subscribers-list"),
                    ]),
                ]),
            ], width=6),
            
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Chat Engagement"),
                    dbc.CardBody([
                        dbc.Row([
                            dbc.Col([
                                html.H5("Chat Messages"),
                                html.H3(id="total-chat-messages", children="0"),
                            ], width=6),
                            dbc.Col([
                                html.H5("Messages/Minute"),
                                html.H3(id="messages-per-minute", children="0"),
                            ], width=6),
                        ]),
                        html.Hr(),
                        html.H5("Unique Chatters"),
                        html.H3(id="unique-chatters", children="0"),
                    ]),
                ]),
            ], width=6),
        ], className="mb-4"),
        
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Viewer Retention"),
                    dbc.CardBody([
                        dcc.Graph(id="viewer-retention-graph"),
                    ]),
                ]),
            ], width=6),
            
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Chat Activity"),
                    dbc.CardBody([
                        dcc.Graph(id="chat-activity-graph"),
                    ]),
                ]),
            ], width=6),
        ], className="mb-4"),
        
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Recent Events"),
                    dbc.CardBody([
                        html.Div(id="recent-events-list"),
                    ]),
                ]),
            ], width=12),
        ], className="mb-4"),
        
        dcc.Interval(
            id='interval-component',
            interval=5*1000,  # in milliseconds (5 seconds)
            n_intervals=0
        ),
        
        dbc.Row([
            dbc.Col([
                html.Hr(),
                html.H4("Historical Analysis", className="text-center mb-4"),
            ], width=12),
        ]),
        
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Select Date Range"),
                    dbc.CardBody([
                        dcc.DatePickerRange(
                            id='date-picker-range',
                            start_date=datetime.datetime.now().date() - datetime.timedelta(days=30),
                            end_date=datetime.datetime.now().date(),
                            display_format='YYYY-MM-DD'
                        ),
                        html.Button('Update Reports', id='update-reports-button', className="mt-2 btn btn-primary"),
                    ]),
                ]),
            ], width=12),
        ], className="mb-4"),
        
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Subscriber Growth"),
                    dbc.CardBody([
                        dcc.Graph(id="subscriber-growth-graph"),
                    ]),
                ]),
            ], width=6),
            
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Stream Performance"),
                    dbc.CardBody([
                        dcc.Graph(id="stream-performance-graph"),
                    ]),
                ]),
            ], width=6),
        ], className="mb-4"),
        
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Viewer Count vs. Chat Activity"),
                    dbc.CardBody([
                        dcc.Graph(id="viewers-chat-correlation-graph"),
                    ]),
                ]),
            ], width=12),
        ], className="mb-4"),
        
    ], fluid=True)
    
    # Define callbacks
    @dash_app.callback(
        [
            Output("stream-status-badge", "children"),
            Output("current-viewers", "children"),
            Output("peak-viewers", "children"),
            Output("stream-duration", "children"),
            Output("total-subscribers", "children"),
            Output("new-subs-today", "children"),
            Output("total-chat-messages", "children"),
            Output("messages-per-minute", "children"),
            Output("unique-chatters", "children"),
            Output("recent-subscribers-list", "children"),
            Output("recent-events-list", "children"),
            Output("viewer-retention-graph", "figure"),
            Output("chat-activity-graph", "figure")
        ],
        [Input("interval-component", "n_intervals")]
    )
    def update_dashboard(n_intervals):
        # Create status badge
        if live_metrics['is_live']:
            status_badge = html.Span("LIVE", className="badge bg-danger p-2")
        else:
            status_badge = html.Span("OFFLINE", className="badge bg-secondary p-2")
        
        # Calculate stream duration
        stream_duration = "0:00"
        if live_metrics['is_live'] and live_metrics['stream_started_at']:
            start_time = datetime.datetime.fromisoformat(live_metrics['stream_started_at'].replace('Z', '+00:00'))
            current_time = datetime.datetime.now(datetime.timezone.utc)
            duration = current_time - start_time
            hours = duration.seconds // 3600
            minutes = (duration.seconds % 3600) // 60
            stream_duration = f"{hours}:{minutes:02d}"
        
        # Format subscribers list
        recent_subs = []
        for sub in live_metrics['recent_subscribers'][-5:]:
            tier_name = "Tier 1"
            if sub['tier'] == "2000":
                tier_name = "Tier 2"
            elif sub['tier'] == "3000":
                tier_name = "Tier 3"
            
            sub_time = datetime.datetime.fromisoformat(sub['timestamp']).strftime('%H:%M:%S')
            gift_text = " (Gifted)" if sub['is_gift'] else ""
            months_text = f" - {sub['total_months']} months" if sub['total_months'] > 1 else ""
            
            recent_subs.append(
                html.Li(
                    f"{sub['user']} ({tier_name}){gift_text}{months_text} - {sub_time}",
                    className="list-group-item bg-dark text-light"
                )
            )
        
        subscribers_list = html.Ul(recent_subs, className="list-group")
        
        # Format recent events list
        recent_events = []
        for event in live_metrics['recent_events'][-10:]:
            badge_class = "bg-info"
            if event['type'] == 'subscription':
                badge_class = "bg-success"
            elif event['type'] == 'raid':
                badge_class = "bg-warning"
            elif event['type'] == 'stream':
                badge_class = "bg-danger"
            
            event_time = datetime.datetime.fromisoformat(event['timestamp']).strftime('%H:%M:%S')
            
            recent_events.append(
                html.Li([
                    html.Span(event['type'].upper(), className=f"badge {badge_class} me-2"),
                    f"{event['message']} - ",
                    html.Small(event_time, className="text-muted")
                ], className="list-group-item bg-dark text-light")
            )
        
        events_list = html.Ul(recent_events[::-1], className="list-group")
        
        # Create viewer retention graph
        viewer_retention = live_metrics['viewer_retention']
        if viewer_retention:
            x_values = [datetime.datetime.fromisoformat(d['timestamp']).strftime('%H:%M:%S') for d in viewer_retention]
            y_values = [d['viewer_count'] for d in viewer_retention]
            
            viewer_fig = px.line(
                x=x_values, 
                y=y_values, 
                labels={'x': 'Time', 'y': 'Viewers'},
                title="Viewer Count Over Time"
            )
            viewer_fig.update_layout(
                template="darkly",
                margin=dict(l=40, r=40, t=40, b=40),
                xaxis=dict(showgrid=False),
                yaxis=dict(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
            )
        else:
            viewer_fig = go.Figure()
            viewer_fig.update_layout(
                template="darkly",
                title="No Viewer Data Available",
                xaxis=dict(showgrid=False),
                yaxis=dict(showgrid=False)
            )
        
        # Create chat activity graph
        chat_activity = live_metrics['chat_activity']
        if chat_activity:
            x_values = [datetime.datetime.fromisoformat(d['timestamp']).strftime('%H:%M') for d in chat_activity]
            y_values = [d['message_count'] for d in chat_activity]
            
            chat_fig = px.bar(
                x=x_values, 
                y=y_values, 
                labels={'x': 'Time', 'y': 'Messages'},
                title="Chat Messages per Minute"
            )
            chat_fig.update_layout(
                template="darkly",
                margin=dict(l=40, r=40, t=40, b=40),
                xaxis=dict(showgrid=False),
                yaxis=dict(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
            )
        else:
            chat_fig = go.Figure()
            chat_fig.update_layout(
                template="darkly",
                title="No Chat Activity Data Available",
                xaxis=dict(showgrid=False),
                yaxis=dict(showgrid=False)
            )
        
        return (
            status_badge,
            str(live_metrics['current_viewers']),
            str(live_metrics['peak_viewers']),
            stream_duration,
            str(live_metrics['subscriber_count']),
            str(live_metrics['new_subs_today']),
            str(live_metrics['total_chat_messages']),
            f"{live_metrics['chat_messages_per_minute']:.1f}",
            str(live_metrics['unique_chatters']),
            subscribers_list,
            events_list,
            viewer_fig,
            chat_fig
        )
    
    # Add more callbacks for historical data if needed
    
    return dash_app