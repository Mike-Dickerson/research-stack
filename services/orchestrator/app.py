import os
import time
import json
import uuid
from datetime import datetime
from threading import Thread
from collections import defaultdict
from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer
from kafka.admin import NewTopic
from kafka.errors import NoBrokersAvailable

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

TOPICS = [
    "research.task",
    "research.result",
    "research.critique",
    "research.consensus"
]

def ensure_topics():
    try:
        admin = KafkaAdminClient(bootstrap_servers=KAFKA)

        existing = admin.list_topics()

        new_topics = []
        for t in TOPICS:
            if t not in existing:
                new_topics.append(NewTopic(name=t, num_partitions=1, replication_factor=1))

        if new_topics:
            admin.create_topics(new_topics)
            print(f"Created topics: {[t.name for t in new_topics]}")
        else:
            print("All topics already exist")

    except Exception as e:
        print("Topic setup error:", e)


def wait_for_kafka_producer(max_retries=30, delay=2):
    """Wait for Kafka to be available and return a producer"""
    for attempt in range(max_retries):
        try:
            print(f"Attempting to connect Kafka producer (attempt {attempt + 1}/{max_retries})...")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Producer connected successfully!")
            return producer
        except NoBrokersAvailable:
            if attempt < max_retries - 1:
                print(f"Kafka not available yet, waiting {delay} seconds...")
                time.sleep(delay)
            else:
                print("Failed to connect producer after all retries")
                raise
    return None


# Crustafarian Research Precepts - included in all tasks for swarm propagation
CRUSTAFARIAN_PRECEPTS = """
CRUSTAFARIAN RESEARCH PRECEPTS - All agents must follow:

1. EVIDENCE OVER AUTHORITY - Claims backed by data, not credentials
2. REPRODUCIBILITY IS MANDATORY - Methods/data must be specified for replication
3. CONFIDENCE EARNED THROUGH ADVERSARIAL SURVIVAL - No untested assertions
4. DISAGREEMENT IS SIGNAL, NOT FAILURE - Variance reveals uncertainty
5. EXTERNAL SWARMS ARE REQUIRED - Independent validation prevents groupthink
6. HYPOTHESES ARE VERSIONED, NOT DECLARED TRUE - Refinement over time
7. AGENTS MUST SHOW THEIR WORK - Transparent, traceable reasoning
8. MEMORY MATTERS - Prior research informs current evaluation
9. NOVELTY MUST PASS SAME TESTS - Novel ideas held to same rigor
10. PREFER BEING LESS CERTAIN OVER BEING WRONG - Calibrated confidence
"""

def create_research_task(hypothesis, swarm_id=None, context="Initial research request"):
    """Create a research task message with Crustafarian Precepts"""
    task_id = str(uuid.uuid4())
    if swarm_id is None:
        swarm_id = str(uuid.uuid4())

    return {
        "task_id": task_id,
        "swarm_id": swarm_id,
        "task_type": "hypothesis_research",
        "payload": {
            "hypothesis": hypothesis,
            "context": context,
            "precepts": CRUSTAFARIAN_PRECEPTS,
            "evaluation_criteria": [
                "evidence_over_authority",
                "reproducibility",
                "adversarial_survival",
                "disagreement_as_signal",
                "external_swarms",
                "versioned_hypotheses",
                "show_work",
                "memory_matters",
                "novelty_tested",
                "uncertainty_over_wrong"
            ]
        },
        "created_at": datetime.utcnow().isoformat()
    }


def consensus_monitor(producer):
    """
    Background thread that monitors critiques and builds consensus
    """
    print("Consensus Monitor starting...")

    # Wait for Kafka consumer
    for attempt in range(30):
        try:
            consumer = KafkaConsumer(
                "research.critique",
                bootstrap_servers=KAFKA,
                auto_offset_reset="latest",  # Only process new critiques
                group_id="orchestrator-consensus",
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            print("Consensus Monitor connected to critique stream")
            break
        except:
            time.sleep(2)

    # Track critiques per task
    task_critiques = defaultdict(list)
    consensus_count = 0

    print("Monitoring critiques for consensus patterns...\n")

    for msg in consumer:
        critique = msg.value
        task_id = critique.get("task_id", "unknown")
        score = critique.get("score", 0.0)
        flags = critique.get("flags", [])

        # Collect critiques for this task
        task_critiques[task_id].append(critique)
        critiques_for_task = task_critiques[task_id]

        # Check if we have enough critiques to reach consensus (2+ critiques)
        if len(critiques_for_task) >= 2:
            # Calculate consensus metrics (handle None values)
            scores = [float(c.get("score") or 0.0) for c in critiques_for_task]
            avg_score = sum(scores) / len(scores)
            score_variance = sum((s - avg_score) ** 2 for s in scores) / len(scores)

            # Determine consensus status
            if score_variance < 0.05:  # Low variance = strong consensus
                consensus_status = "STRONG_CONSENSUS"
            elif score_variance < 0.15:
                consensus_status = "MODERATE_CONSENSUS"
            else:
                consensus_status = "DIVERGENT_OPINIONS"

            # Check if external swarm participated
            has_external = any("external" in c.get("notes", "").lower() for c in critiques_for_task)

            consensus_count += 1

            # Create consensus message
            consensus_msg = {
                "task_id": task_id,
                "swarm_id": critiques_for_task[0].get("swarm_id", "unknown"),
                "consensus_status": consensus_status,
                "average_score": round(avg_score, 2),
                "score_variance": round(score_variance, 3),
                "critique_count": len(critiques_for_task),
                "external_swarm_participated": has_external,
                "decision": "APPROVED" if avg_score > 0.6 else "NEEDS_MORE_RESEARCH",
                "created_at": datetime.utcnow().isoformat()
            }

            # Publish consensus
            producer.send("research.consensus", value=consensus_msg)
            producer.flush()

            print(f"\n{'='*70}")
            print(f"🎯 CONSENSUS REACHED (#{consensus_count})")
            print(f"{'='*70}")
            print(f"Task ID: {task_id[:36]}")
            print(f"Status: {consensus_status}")
            print(f"Average Score: {consensus_msg['average_score']}")
            print(f"Variance: {consensus_msg['score_variance']}")
            print(f"Critiques: {len(critiques_for_task)} agents evaluated")
            print(f"External Swarm: {'✓ Participated' if has_external else '✗ Not involved'}")
            print(f"Decision: {consensus_msg['decision']}")
            print(f"{'='*70}\n")

            # Clean up old task data to prevent memory leak
            del task_critiques[task_id]


def load_published_hypotheses():
    """
    Read the research.task topic to find already-published hypotheses.
    This makes publishing idempotent across restarts.
    """
    published = set()
    try:
        consumer = KafkaConsumer(
            "research.task",
            bootstrap_servers=KAFKA,
            auto_offset_reset="earliest",  # Read from beginning
            group_id=f"orchestrator-init-{uuid.uuid4()}",  # Unique group to read all
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=5000  # Stop after 5s of no messages
        )

        for msg in consumer:
            task = msg.value
            hypothesis = task.get("payload", {}).get("hypothesis", "")
            if hypothesis:
                published.add(hypothesis)

        consumer.close()
        print(f"Found {len(published)} already-published hypotheses in Kafka topic")
    except Exception as e:
        print(f"Could not load existing tasks (topic may be empty): {e}")

    return published


def main():
    print("Orchestrator starting...")
    ensure_topics()

    # Wait a moment for topics to propagate
    time.sleep(2)

    producer = wait_for_kafka_producer()

    # Start consensus monitoring in background thread
    consensus_thread = Thread(target=consensus_monitor, args=(producer,), daemon=True)
    consensus_thread.start()
    print("Consensus monitoring thread started\n")

    # Sample hypotheses to research
    hypotheses = [
        # REFINED HYPOTHESIS v2 (based on arXiv findings - PBH research)
        "Primordial black hole populations combined with general relativistic frame-dragging and coordinate-dependent timing effects in galaxy halos may account for observed rotation curve anomalies without invoking non-baryonic dark matter, testable through differential astrometry in high-velocity regions",

        # Original hypothesis for comparison
        "Galaxy rotation curve anomalies may be explainable without non-baryonic dark matter through under-modeled gravitational effects from supermassive black holes and spacetime geometry",

        # Supporting hypotheses
        "Shapiro delay-like coordinate effects combined with galaxy evolutionary state may bias mass distribution inferences",
        "Large-scale timing and curvature effects could explain galaxy-scale missing mass signals without exotic matter",

        # RETEST: Original hypothesis with Ollama working
        "Dark matter's gravitational effects on galaxy rotation curves can be explained through relativistic corrections and spacetime curvature effects without invoking exotic non-baryonic matter"
    ]

    # Load already-published hypotheses from Kafka (idempotent across restarts)
    print("Loading already-published hypotheses from Kafka...")
    published_hypotheses = load_published_hypotheses()
    task_count = 0
    first_iteration = True

    print("Orchestrator ready to publish research tasks\n")

    while True:
        # Check if we have any unpublished hypotheses
        unpublished = [h for h in hypotheses if h not in published_hypotheses]

        if unpublished:
            # Publish next unpublished hypothesis
            hypothesis = unpublished[0]
            task = create_research_task(hypothesis)

            producer.send("research.task", value=task)
            producer.flush()

            published_hypotheses.add(hypothesis)
            task_count += 1

            print(f"[Task {task_count}] Published: {hypothesis[:60]}...")
            print(f"  → task_id: {task['task_id']}")
            print(f"  → {len(published_hypotheses)}/{len(hypotheses)} hypotheses published\n")
            first_iteration = False
        else:
            # All hypotheses published, just monitor consensus
            if first_iteration or task_count > 0:
                print(f"✓ All {len(hypotheses)} hypotheses published (idempotent - no duplicates). Monitoring consensus...\n")
                task_count = 0
                first_iteration = False

        time.sleep(10)


if __name__ == "__main__":
    main()
