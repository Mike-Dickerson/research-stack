#!/usr/bin/env python3
"""
Research Stack - Real-time Swarm Monitor
Shows live activity across all agents in the research swarm
"""

import json
import sys
from datetime import datetime
from kafka import KafkaConsumer
from collections import defaultdict

KAFKA = "localhost:9092"

def format_timestamp(iso_str):
    """Format ISO timestamp to readable time"""
    try:
        dt = datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
        return dt.strftime('%H:%M:%S')
    except:
        return iso_str

def main():
    print("=" * 80)
    print("🧬 RESEARCH SWARM MONITOR - Real-time Event Stream")
    print("=" * 80)
    print("\nConnecting to Kafka...")

    # Subscribe to all research topics
    consumer = KafkaConsumer(
        'research.task',
        'research.result',
        'research.critique',
        'research.consensus',
        bootstrap_servers=KAFKA,
        auto_offset_reset='latest',  # Only show new events
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    print("✓ Connected! Monitoring swarm activity...\n")
    print("=" * 80)

    stats = defaultdict(int)

    try:
        for msg in consumer:
            topic = msg.topic
            event = msg.value
            timestamp = format_timestamp(event.get('created_at', ''))

            stats[topic] += 1

            # Format output based on topic
            if topic == 'research.task':
                hypothesis = event.get('payload', {}).get('hypothesis', '')
                print(f"\n📋 [{timestamp}] NEW TASK")
                print(f"   Hypothesis: {hypothesis[:80]}...")
                print(f"   Task ID: {event.get('task_id', 'unknown')[:36]}")

            elif topic == 'research.result':
                agent = event.get('agent_id', 'unknown')
                confidence = event.get('confidence', 0.0)
                evidence = event.get('evidence_count', 0)
                print(f"\n🔬 [{timestamp}] RESEARCH RESULT")
                print(f"   Agent: {agent[:50]}")
                print(f"   Confidence: {confidence:.2f} | Evidence: {evidence} papers")

            elif topic == 'research.critique':
                score = event.get('score', 0.0)
                flags = event.get('flags', [])
                print(f"\n🧐 [{timestamp}] PEER REVIEW")
                print(f"   Score: {score:.2f}/1.0")
                if flags:
                    print(f"   Flags: {', '.join(flags)}")

            elif topic == 'research.consensus':
                decision = event.get('decision', 'UNKNOWN')
                avg_score = event.get('average_score', 0.0)
                consensus_status = event.get('consensus_status', 'UNKNOWN')
                critique_count = event.get('critique_count', 0)
                external = event.get('external_swarm_participated', False)

                print(f"\n{'='*80}")
                print(f"🎯 [{timestamp}] CONSENSUS REACHED")
                print(f"{'='*80}")
                print(f"   Decision: {decision}")
                print(f"   Average Score: {avg_score}/1.0")
                print(f"   Consensus: {consensus_status}")
                print(f"   Critiques: {critique_count}")
                print(f"   External Swarm: {'✓ Participated' if external else '✗ Not involved'}")
                print(f"   Task ID: {event.get('task_id', 'unknown')[:36]}")
                print(f"{'='*80}")

            # Show running stats
            print(f"\n📊 Events: Tasks={stats['research.task']} | "
                  f"Results={stats['research.result']} | "
                  f"Critiques={stats['research.critique']} | "
                  f"Consensus={stats['research.consensus']}")
            print("-" * 80)

    except KeyboardInterrupt:
        print("\n\n✓ Monitor stopped by user")
        print(f"\nFinal stats:")
        for topic, count in stats.items():
            print(f"  {topic}: {count} events")

if __name__ == "__main__":
    main()
