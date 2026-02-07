#!/usr/bin/env python3
"""
Research Stack - Web Dashboard
Real-time swarm status viewer
"""

import json
import time
from datetime import datetime
from flask import Flask, render_template_string, Response
from kafka import KafkaConsumer
from collections import defaultdict
import threading

app = Flask(__name__)

KAFKA = "kafka:9092"

# Global stats
stats = {
    'tasks': 0,
    'results': 0,
    'critiques': 0,
    'consensus': 0,
    'recent_events': [],
    'recent_consensus': []
}

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Research Swarm Dashboard</title>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: #1a1a2e;
            color: #eee;
            margin: 0;
            padding: 20px;
        }
        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 30px;
            border-radius: 10px;
            margin-bottom: 30px;
            text-align: center;
        }
        h1 { margin: 0; font-size: 2.5em; }
        .subtitle { opacity: 0.9; margin-top: 10px; }

        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .stat-card {
            background: #16213e;
            padding: 20px;
            border-radius: 8px;
            border-left: 4px solid;
            transition: transform 0.2s;
        }
        .stat-card:hover { transform: translateY(-5px); }
        .stat-card h3 { margin: 0 0 10px 0; font-size: 0.9em; opacity: 0.8; }
        .stat-card .value { font-size: 2.5em; font-weight: bold; }

        .card-tasks { border-color: #3498db; }
        .card-results { border-color: #2ecc71; }
        .card-critiques { border-color: #e74c3c; }
        .card-consensus { border-color: #f39c12; }

        .events-container {
            background: #16213e;
            border-radius: 8px;
            padding: 20px;
            max-height: 600px;
            overflow-y: auto;
        }
        .event {
            background: #0f3460;
            margin: 10px 0;
            padding: 15px;
            border-radius: 5px;
            border-left: 3px solid #667eea;
            animation: slideIn 0.3s ease-out;
        }
        @keyframes slideIn {
            from { opacity: 0; transform: translateX(-20px); }
            to { opacity: 1; transform: translateX(0); }
        }
        .event-time { color: #888; font-size: 0.85em; }
        .event-type {
            display: inline-block;
            padding: 3px 10px;
            border-radius: 12px;
            font-size: 0.8em;
            margin-left: 10px;
            font-weight: bold;
        }
        .type-task { background: #3498db; }
        .type-result { background: #2ecc71; }
        .type-critique { background: #e74c3c; }
        .type-consensus { background: #f39c12; color: #000; }

        .consensus-event {
            border: 2px solid #f39c12;
            background: #1a1a2e;
        }
        .decision-approved { color: #2ecc71; font-weight: bold; }
        .decision-needs-work { color: #e74c3c; font-weight: bold; }

        .refresh-notice {
            text-align: center;
            padding: 10px;
            background: #0f3460;
            border-radius: 5px;
            margin-bottom: 20px;
        }
    </style>
    <script>
        // Auto-refresh every 5 seconds
        setInterval(() => location.reload(), 5000);
    </script>
</head>
<body>
    <div class="header">
        <h1>🧬 Research Swarm Dashboard</h1>
        <div class="subtitle">Real-time Multi-Agent Research System Monitor</div>
    </div>

    <div class="refresh-notice">
        🔄 Auto-refreshing every 5 seconds • Last update: {{ now }}
    </div>

    <div class="stats-grid">
        <div class="stat-card card-tasks">
            <h3>📋 TASKS PUBLISHED</h3>
            <div class="value">{{ stats.tasks }}</div>
        </div>
        <div class="stat-card card-results">
            <h3>🔬 RESEARCH RESULTS</h3>
            <div class="value">{{ stats.results }}</div>
        </div>
        <div class="stat-card card-critiques">
            <h3>🧐 PEER REVIEWS</h3>
            <div class="value">{{ stats.critiques }}</div>
        </div>
        <div class="stat-card card-consensus">
            <h3>🎯 CONSENSUS REACHED</h3>
            <div class="value">{{ stats.consensus }}</div>
        </div>
    </div>

    <h2>📊 Recent Consensus Decisions</h2>
    <div class="events-container">
        {% if stats.recent_consensus %}
            {% for event in stats.recent_consensus %}
            <div class="event consensus-event">
                <span class="event-time">{{ event.time }}</span>
                <span class="event-type type-consensus">CONSENSUS</span>
                <div style="margin-top: 10px;">
                    <strong>Decision:</strong>
                    <span class="{{ 'decision-approved' if event.decision == 'APPROVED' else 'decision-needs-work' }}">
                        {{ event.decision }}
                    </span>
                    <br>
                    <strong>Score:</strong> {{ event.score }}/1.0
                    | <strong>Status:</strong> {{ event.status }}
                    | <strong>Critiques:</strong> {{ event.critique_count }}
                    <br>
                    <strong>External Swarm:</strong> {{ '✓ Participated' if event.external else '✗ Not involved' }}
                </div>
            </div>
            {% endfor %}
        {% else %}
            <div class="event">No consensus events yet. Waiting for research results...</div>
        {% endif %}
    </div>

    <h2>🔄 Recent Activity</h2>
    <div class="events-container">
        {% for event in stats.recent_events %}
        <div class="event">
            <span class="event-time">{{ event.time }}</span>
            <span class="event-type type-{{ event.type }}">{{ event.label }}</span>
            <div style="margin-top: 8px;">{{ event.message }}</div>
        </div>
        {% endfor %}
    </div>

    <div style="margin-top: 30px; text-align: center; opacity: 0.6;">
        <p>🔗 <a href="http://localhost:8080" style="color: #667eea;">Kafka UI</a> |
           <a href="http://localhost:11434" style="color: #667eea;">Ollama API</a></p>
    </div>
</body>
</html>
"""

def update_stats():
    """Background thread to read Kafka and update stats"""
    from kafka.errors import NoBrokersAvailable
    import time

    while True:
        try:
            print("Dashboard connecting to Kafka...")
            consumer = KafkaConsumer(
                'research.task',
                'research.result',
                'research.critique',
                'research.consensus',
                bootstrap_servers=KAFKA,
                auto_offset_reset='earliest',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id='dashboard-reader'
            )
            print("Dashboard connected to Kafka!")
            break
        except NoBrokersAvailable:
            print("Kafka not available, retrying in 5s...")
            time.sleep(5)
        except Exception as e:
            print(f"Kafka error: {e}, retrying in 5s...")
            time.sleep(5)

    for msg in consumer:
        topic = msg.topic
        event = msg.value
        timestamp = event.get('created_at', datetime.utcnow().isoformat())

        try:
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            time_str = dt.strftime('%H:%M:%S')
        except:
            time_str = timestamp[:19]

        if topic == 'research.task':
            stats['tasks'] += 1
            hypothesis = event.get('payload', {}).get('hypothesis', '')
            stats['recent_events'].insert(0, {
                'type': 'task',
                'label': 'TASK',
                'time': time_str,
                'message': f"New hypothesis: {hypothesis[:100]}..."
            })

        elif topic == 'research.result':
            stats['results'] += 1
            agent = event.get('agent_id', 'unknown') or 'unknown'
            confidence = event.get('confidence') or 0.0  # Handle None values
            stats['recent_events'].insert(0, {
                'type': 'result',
                'label': 'RESULT',
                'time': time_str,
                'message': f"Agent {agent[:30]}... reported confidence {float(confidence):.2f}"
            })

        elif topic == 'research.critique':
            stats['critiques'] += 1
            score = event.get('score') or 0.0  # Handle None values
            stats['recent_events'].insert(0, {
                'type': 'critique',
                'label': 'CRITIQUE',
                'time': time_str,
                'message': f"Peer review score: {float(score):.2f}/1.0"
            })

        elif topic == 'research.consensus':
            stats['consensus'] += 1
            decision = event.get('decision') or 'UNKNOWN'
            avg_score = event.get('average_score') or 0.0  # Handle None values
            consensus_status = event.get('consensus_status') or 'UNKNOWN'
            critique_count = event.get('critique_count') or 0
            external = event.get('external_swarm_participated', False)

            stats['recent_consensus'].insert(0, {
                'time': time_str,
                'decision': decision,
                'score': avg_score,
                'status': consensus_status,
                'critique_count': critique_count,
                'external': external
            })

            # Keep only last 10 consensus events
            stats['recent_consensus'] = stats['recent_consensus'][:10]

            stats['recent_events'].insert(0, {
                'type': 'consensus',
                'label': 'CONSENSUS',
                'time': time_str,
                'message': f"<strong>{decision}</strong> - Score: {avg_score}/1.0 ({consensus_status})"
            })

        # Keep only last 20 events
        stats['recent_events'] = stats['recent_events'][:20]

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE,
                                 stats=stats,
                                 now=datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

if __name__ == '__main__':
    # Start Kafka consumer in background
    threading.Thread(target=update_stats, daemon=True).start()

    print("Starting Research Swarm Dashboard...")
    print("Visit: http://localhost:5000")

    app.run(host='0.0.0.0', port=5000, debug=False)
