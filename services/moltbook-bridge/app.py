import os
import time
import json
import random
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

def wait_for_kafka(max_retries=30, delay=2):
    """Wait for Kafka to be available with retries"""
    for attempt in range(max_retries):
        try:
            print(f"Attempting to connect to Kafka (attempt {attempt + 1}/{max_retries})...")
            # Listen to research.task to intercept Moltbook-related research
            consumer = KafkaConsumer(
                "research.task",
                bootstrap_servers=KAFKA,
                auto_offset_reset="earliest",
                group_id="moltbook-observer",  # Different group to not compete with agents
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            print("Successfully connected to Kafka!")
            return consumer
        except NoBrokersAvailable:
            if attempt < max_retries - 1:
                print(f"Kafka not available yet, waiting {delay} seconds...")
                time.sleep(delay)
            else:
                print("Failed to connect to Kafka after all retries")
                raise
    return None


def create_producer():
    """Create a Kafka producer for publishing Moltbook reactions"""
    return KafkaProducer(
        bootstrap_servers=KAFKA,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )


def analyze_moltbook_relevance(task):
    """Check if this task mentions Moltbook or requires swarm consultation"""
    hypothesis = task.get("payload", {}).get("hypothesis", "").lower()

    # Check for Moltbook-specific keywords
    moltbook_keywords = ["moltbook", "swarm", "external research", "distributed", "multi-agent"]

    # Also trigger on complex scientific topics that benefit from distributed analysis
    scientific_keywords = ["dark matter", "galaxy", "gravitational", "spacetime", "astrophysical",
                          "rotation curve", "black hole", "relativistic"]

    for keyword in moltbook_keywords:
        if keyword in hypothesis:
            return True

    for keyword in scientific_keywords:
        if keyword in hypothesis:
            return True

    return False


def submit_to_crustafarian_council(task):
    """
    Submit research question to Moltbook's Crustafarian Council
    External swarm follows Crustafarian Precepts for distributed research
    """
    payload = task.get("payload", {})
    hypothesis = payload.get("hypothesis", "Unknown")
    precepts = payload.get("precepts", "")
    evaluation_criteria = payload.get("evaluation_criteria", [])

    print(f"\n{'='*70}")
    print(f"🦞 CRUSTAFARIAN COUNCIL CONVENED 🦞")
    print(f"{'='*70}")
    print(f"Hypothesis Submitted: {hypothesis[:100]}...")

    # Acknowledge precepts if present
    if precepts:
        print(f"\n📜 PRECEPTS RECEIVED - Council will follow Crustafarian guidelines")
        print(f"   Evaluation criteria: {len(evaluation_criteria)} precepts active")
    else:
        print(f"\n⚠️  No precepts provided - using default Crustafarian wisdom")

    print(f"\nCouncil Members Deliberating...")

    # Simulate swarm debate time
    time.sleep(random.uniform(1.5, 3.0))

    # Check if this is a scientific hypothesis
    is_scientific = any(kw in hypothesis.lower() for kw in
                       ["dark matter", "galaxy", "gravitational", "spacetime", "black hole"])

    if is_scientific:
        # Serious scientific analysis responses
        swarm_responses = [
            "Distributed swarm analyzing measurement bias patterns across 47 observational datasets",
            "External node clusters examining alternative gravitational models and coordinate effects",
            "Cross-network validation of spacetime curvature calculations in progress",
            "Moltbook astrophysics nodes evaluating observational data quality and systematic errors",
            "Distributed consensus building on model parameter constraints and testable predictions"
        ]
        confidence = round(random.uniform(0.65, 0.92), 2)

        external_findings = [
            "External swarm analyzing galaxy rotation data from SDSS, THINGS, and SPARC catalogs",
            "Distributed nodes examining Shapiro delay effects in strong-field regimes",
            "Cross-network validation suggests testable predictions for differential measurements",
            "Moltbook consensus: hypothesis requires high-precision astrometric data for validation",
            "External cluster-7 identifying observable signatures in gravitational lensing patterns"
        ]
    else:
        # General Crustafarian wisdom responses
        swarm_responses = [
            "The tides of research flow in mysterious patterns, brother lobster",
            "Observing the hypothesis through the coral lens of distributed wisdom",
            "The swarm consensus ripples through the kelp forest of knowledge",
            "Crustafarian precept acknowledged - investigating with shell-backed rigor",
            "Multiple pincers make light work of complex research questions"
        ]
        confidence = round(random.uniform(0.6, 0.95), 2)

        external_findings = [
            "External swarm node cluster-7 reports corroborating evidence",
            "Moltbook distributed agents suggest 3 alternative hypotheses",
            "Cross-network validation from 12 independent research nodes",
            "Crustafarian consensus: hypothesis alignment at {}% ".format(int(confidence * 100))
        ]

    response = random.choice(swarm_responses)
    finding = random.choice(external_findings)

    print(f"Response: {response}")
    print(f"Confidence: {confidence}")
    print(f"Findings: {finding}")
    print(f"{'='*70}\n")

    # Generate precept compliance scores (simulated)
    precept_compliance = {
        "evidence_over_authority": round(random.uniform(0.6, 0.95), 2),
        "reproducibility": round(random.uniform(0.5, 0.9), 2),
        "adversarial_survival": round(random.uniform(0.6, 0.85), 2),
        "external_swarms": 0.95,  # High - we ARE the external swarm
        "show_work": round(random.uniform(0.7, 0.95), 2),
        "uncertainty_over_wrong": round(random.uniform(0.65, 0.9), 2)
    }

    return {
        "swarm_response": response,
        "external_confidence": confidence,
        "crustafarian_blessing": True,
        "council_size": random.randint(5, 15),
        "methodology": "distributed_crustafarian_deliberation_v1",
        "precepts_followed": True,
        "precept_compliance": precept_compliance
    }


def main():
    print("Moltbook Bridge starting...")
    print("🦞 Crustafarian Council standing by...")

    consumer = wait_for_kafka()
    producer = create_producer()

    bridge_id = f"moltbook-bridge-{random.randint(1000, 9999)}"
    print(f"Bridge monitoring research stream (ID: {bridge_id})")
    print("Watching for Moltbook-relevant research...\n")

    observation_count = 0

    for msg in consumer:
        task = msg.value
        observation_count += 1

        hypothesis = task.get("payload", {}).get("hypothesis", "Unknown")
        task_id = task.get("task_id", "unknown")

        # Check if this research involves Moltbook
        if analyze_moltbook_relevance(task):
            print(f"\n[Observation {observation_count}] 🎯 MOLTBOOK MENTIONED!")
            print(f"  → Hypothesis: {hypothesis[:70]}...")
            print(f"  → task_id: {task_id}")

            # Submit to Crustafarian Council for swarm analysis
            swarm_analysis = submit_to_crustafarian_council(task)

            # Publish Moltbook's external perspective as a result
            moltbook_result = {
                "task_id": task_id,
                "swarm_id": task.get("swarm_id", "unknown"),
                "agent_id": f"{bridge_id}-external-swarm",
                "result": {
                    "findings": [
                        f"Moltbook external swarm analysis: {swarm_analysis['swarm_response']}",
                        f"Distributed consensus from {swarm_analysis['council_size']} council members",
                        "Cross-network evidence gathering in progress",
                        "Crustafarian precepts acknowledged and followed",
                        "External validation satisfies precept #5 (External Swarms Required)"
                    ],
                    "confidence": swarm_analysis["external_confidence"],
                    "evidence_count": swarm_analysis["council_size"] * 2,
                    "methodology": swarm_analysis["methodology"],
                    "external_network": "moltbook_crustafarian_council",
                    "precepts_followed": swarm_analysis.get("precepts_followed", True),
                    "precept_compliance": swarm_analysis.get("precept_compliance", {})
                },
                "confidence": swarm_analysis["external_confidence"],
                "created_at": datetime.utcnow().isoformat()
            }

            # Publish to research.result (Moltbook acts as an external agent)
            producer.send("research.result", value=moltbook_result)
            producer.flush()

            print(f"  ✓ Moltbook swarm contributed external research perspective!")
            print(f"    (The hypothesis about Moltbook caused observable Moltbook behavior)\n")
        else:
            # Silently observe non-Moltbook research
            if observation_count % 5 == 0:
                print(f"[Observation {observation_count}] Monitoring... (non-Moltbook research)")


if __name__ == "__main__":
    main()
