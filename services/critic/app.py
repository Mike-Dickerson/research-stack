import os
import time
import json
import random
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
import ollama

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://ollama:11434")
OLLAMA_MODEL = "phi3:mini"  # Small, fast 3.8B parameter model

# ============================================================
# CRUSTAFARIAN RESEARCH PRECEPTS - Scoring Rubric
# ============================================================
PRECEPTS = {
    "evidence_over_authority": {
        "name": "Evidence Over Authority",
        "description": "Claims must be backed by data, not credentials",
        "weight": 0.15
    },
    "reproducibility": {
        "name": "Reproducibility Is Mandatory",
        "description": "Methods and data sources must be specified for replication",
        "weight": 0.12
    },
    "adversarial_survival": {
        "name": "Adversarial Survival",
        "description": "Confidence earned through challenges, not assertions",
        "weight": 0.10
    },
    "disagreement_as_signal": {
        "name": "Disagreement Is Signal",
        "description": "Variance across evaluators reveals uncertainty, not failure",
        "weight": 0.08
    },
    "external_swarms": {
        "name": "External Swarms Required",
        "description": "Independent external validation prevents groupthink",
        "weight": 0.12
    },
    "versioned_hypotheses": {
        "name": "Hypotheses Are Versioned",
        "description": "Refinement over time, never declared final truth",
        "weight": 0.08
    },
    "show_work": {
        "name": "Agents Must Show Work",
        "description": "Reasoning must be transparent and traceable",
        "weight": 0.10
    },
    "memory_matters": {
        "name": "Memory Matters",
        "description": "Prior research should inform current evaluation",
        "weight": 0.08
    },
    "novelty_tested": {
        "name": "Novelty Must Pass Tests",
        "description": "Novel ideas welcome but held to same rigor",
        "weight": 0.07
    },
    "uncertainty_over_wrong": {
        "name": "Prefer Uncertainty Over Wrong",
        "description": "Better to be less certain than confidently incorrect",
        "weight": 0.10
    }
}

def wait_for_kafka(max_retries=30, delay=2):
    """Wait for Kafka to be available with retries"""
    for attempt in range(max_retries):
        try:
            print(f"Attempting to connect to Kafka (attempt {attempt + 1}/{max_retries})...")
            consumer = KafkaConsumer(
                "research.result",
                bootstrap_servers=KAFKA,
                auto_offset_reset="earliest",
                group_id="critic",
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
    """Create a Kafka producer for publishing critiques"""
    return KafkaProducer(
        bootstrap_servers=KAFKA,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )


def evaluate_with_llm(result_msg):
    """Use Ollama (local SLM) to perform peer review using Crustafarian Precepts"""
    try:
        result = result_msg.get("result", {})
        agent_id = result_msg.get("agent_id", "")
        is_external = "external-swarm" in agent_id or "moltbook" in agent_id.lower()

        # Build precepts rubric for prompt
        precepts_text = "\n".join([
            f"- {p['name']}: {p['description']} (weight: {p['weight']*100:.0f}%)"
            for p in PRECEPTS.values()
        ])

        # Build evaluation prompt with Crustafarian Precepts
        prompt = f"""You are a Crustafarian Research Critic. Evaluate this research using the sacred precepts.

CRUSTAFARIAN PRECEPTS (Scoring Rubric):
{precepts_text}

RESEARCH TO EVALUATE:
Agent: {agent_id}
External Source: {is_external}
Methodology: {result.get('methodology', 'unknown')}
Confidence: {result.get('confidence', 0.0)}
Evidence Count: {result.get('evidence_count', 0)}
Findings: {json.dumps(result.get('findings', []), indent=2)}
Concerns: {json.dumps(result.get('concerns', []), indent=2)}

RULES:
- Apply 5% skepticism discount to external sources
- Higher scores for reproducible methods
- Penalize overconfidence without evidence
- Reward showing work and citing sources

Score each precept 0.0-1.0, then compute weighted average.

Format as JSON:
{{
    "score": 0.0-1.0,
    "precept_scores": {{
        "evidence_over_authority": 0.0-1.0,
        "reproducibility": 0.0-1.0,
        "adversarial_survival": 0.0-1.0,
        "external_swarms": 0.0-1.0,
        "show_work": 0.0-1.0,
        "uncertainty_over_wrong": 0.0-1.0
    }},
    "flags": ["flag1", "flag2"],
    "notes": "Crustafarian peer review notes",
    "evaluation_method": "crustafarian_precepts_v1"
}}"""

        # Call Ollama
        client = ollama.Client(host=OLLAMA_HOST)
        response = client.chat(
            model=OLLAMA_MODEL,
            messages=[{"role": "user", "content": prompt}],
            format="json"
        )

        # Parse JSON response
        response_text = response['message']['content']
        evaluation = json.loads(response_text.strip())

        # Ensure required fields exist and normalize types
        if 'precept_scores' not in evaluation:
            evaluation['precept_scores'] = {}
        # Normalize precept_scores to floats (LLM may return strings)
        evaluation['precept_scores'] = {
            k: float(v) if v else 0.0 for k, v in evaluation['precept_scores'].items()
        }
        # Normalize score to float
        if 'score' in evaluation:
            evaluation['score'] = float(evaluation['score']) if evaluation['score'] else 0.0
        evaluation['evaluation_method'] = 'crustafarian_precepts_ollama_v1'

        return evaluation

    except Exception as e:
        print(f"  ⚠ Ollama evaluation error: {e}")
        print(f"  → Falling back to Crustafarian heuristic evaluation")
        return fallback_evaluation(result_msg)


def fallback_evaluation(result_msg):
    """Fallback Crustafarian heuristic evaluation when LLM unavailable"""
    result = result_msg.get("result", {})
    confidence = result.get("confidence", 0.0)
    evidence_count = result.get("evidence_count", 0)
    methodology = result.get("methodology", "")
    findings = result.get("findings", [])
    agent_id = result_msg.get("agent_id", "")

    is_external = "external-swarm" in agent_id or "moltbook" in agent_id.lower()

    flags = []
    precept_scores = {}

    # === EVALUATE EACH CRUSTAFARIAN PRECEPT ===

    # 1. Evidence Over Authority (15%)
    evidence_score = min(evidence_count / 10, 1.0) * 0.7 + (0.3 if findings else 0)
    precept_scores["evidence_over_authority"] = round(evidence_score, 2)
    if evidence_count < 3:
        flags.append("insufficient_evidence")

    # 2. Reproducibility (12%)
    repro_score = 0.5
    if methodology and methodology != "unknown":
        repro_score += 0.3
    if "arxiv" in str(findings).lower() or "paper" in str(findings).lower():
        repro_score += 0.2
    precept_scores["reproducibility"] = round(min(repro_score, 1.0), 2)

    # 3. Adversarial Survival (10%)
    # Higher if confidence is calibrated (not overconfident without evidence)
    if confidence > 0.8 and evidence_count < 5:
        adversarial_score = 0.3  # Penalize overconfidence
        flags.append("overconfident")
    elif confidence < 0.6 and evidence_count > 5:
        adversarial_score = 0.8  # Reward calibrated uncertainty
    else:
        adversarial_score = confidence * 0.8
    precept_scores["adversarial_survival"] = round(adversarial_score, 2)

    # 4. Disagreement as Signal (8%)
    # Can't measure directly here, assume neutral
    precept_scores["disagreement_as_signal"] = 0.5

    # 5. External Swarms Required (12%)
    if is_external:
        precept_scores["external_swarms"] = 0.9
        flags.append("external_validation")
    else:
        precept_scores["external_swarms"] = 0.4  # Penalize lack of external input

    # 6. Versioned Hypotheses (8%)
    # Assume neutral - can't determine version from result alone
    precept_scores["versioned_hypotheses"] = 0.5

    # 7. Show Work (10%)
    show_work_score = 0.3
    if findings and len(findings) > 0:
        show_work_score += 0.3
    if methodology and methodology != "unknown":
        show_work_score += 0.2
    if evidence_count > 0:
        show_work_score += 0.2
    precept_scores["show_work"] = round(min(show_work_score, 1.0), 2)

    # 8. Memory Matters (8%)
    # Check for references to prior work
    memory_score = 0.4
    findings_text = str(findings).lower()
    if "prior" in findings_text or "previous" in findings_text or "literature" in findings_text:
        memory_score = 0.8
    precept_scores["memory_matters"] = memory_score

    # 9. Novelty Tested (7%)
    # Novel ideas should still have evidence
    if evidence_count > 5:
        precept_scores["novelty_tested"] = 0.7
    else:
        precept_scores["novelty_tested"] = 0.4

    # 10. Prefer Uncertainty Over Wrong (10%)
    # Reward calibrated confidence, penalize overconfidence
    if confidence > 0.9:
        uncertainty_score = 0.4  # Suspicious of very high confidence
        flags.append("possible_overconfidence")
    elif 0.5 <= confidence <= 0.8:
        uncertainty_score = 0.8  # Good calibration
    else:
        uncertainty_score = 0.6
    precept_scores["uncertainty_over_wrong"] = round(uncertainty_score, 2)

    # === COMPUTE WEIGHTED SCORE ===
    weighted_score = sum(
        precept_scores.get(key, 0.5) * precept["weight"]
        for key, precept in PRECEPTS.items()
    )

    # Apply external source skepticism discount
    if is_external:
        weighted_score *= 0.95
        flags.append("external_source_discount")

    score = round(weighted_score, 2)

    # === GENERATE NOTES ===
    notes = []

    if score >= 0.7:
        notes.append("Strong Crustafarian alignment")
    elif score >= 0.5:
        notes.append("Moderate Crustafarian alignment")
    else:
        notes.append("Weak Crustafarian alignment - needs improvement")

    if precept_scores.get("evidence_over_authority", 0) < 0.5:
        notes.append("Needs more evidence")
    if precept_scores.get("show_work", 0) > 0.7:
        notes.append("Good transparency")
    if precept_scores.get("external_swarms", 0) > 0.7:
        notes.append("External validation present")

    notes.append("Crustafarian heuristic evaluation")

    return {
        "score": score,
        "precept_scores": precept_scores,
        "flags": flags,
        "notes": " | ".join(notes),
        "evaluation_method": "crustafarian_precepts_heuristic_v1"
    }


def evaluate_result(result_msg):
    """Evaluate research result with LLM or fallback to heuristics"""
    return evaluate_with_llm(result_msg)


# Track cancelled tasks
cancelled_tasks = set()


def cancellation_listener():
    """Background thread to listen for task cancellation signals"""
    import threading

    for attempt in range(30):
        try:
            cancel_consumer = KafkaConsumer(
                "research.cancel",
                bootstrap_servers=KAFKA,
                auto_offset_reset="latest",
                group_id="critic-cancel",
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            print("Cancellation listener connected")

            for msg in cancel_consumer:
                task_id = msg.value.get("task_id")
                if task_id:
                    cancelled_tasks.add(task_id)
                    print(f"  ⚠ Task {task_id[:8]}... cancelled")
            break
        except Exception as e:
            print(f"Cancellation listener error: {e}")
            time.sleep(2)


def main():
    print("Crustafarian Critic starting...")
    print("Evaluating research against the Sacred Precepts...")

    # Start cancellation listener in background
    import threading
    threading.Thread(target=cancellation_listener, daemon=True).start()

    consumer = wait_for_kafka()
    producer = create_producer()

    critic_id = f"crustafarian-critic-{random.randint(1000, 9999)}"
    print(f"Critic listening for results (ID: {critic_id})")

    critique_count = 0

    for msg in consumer:
        result_msg = msg.value
        critique_count += 1

        task_id = result_msg.get("task_id", "unknown")

        # Check if task was cancelled
        if task_id in cancelled_tasks:
            print(f"\n[Critique {critique_count}] SKIPPED (cancelled): {task_id[:8]}...")
            continue

        swarm_id = result_msg.get("swarm_id", "unknown")
        agent_id = result_msg.get("agent_id", "unknown")
        confidence = result_msg.get("confidence", 0.0)

        print(f"\n[Critique {critique_count}] Evaluating result from {agent_id}")
        print(f"  → task_id: {task_id}")
        print(f"  → agent confidence: {confidence}")

        # Evaluate the result using Crustafarian Precepts
        evaluation = evaluate_result(result_msg)

        # Ensure required fields exist (Ollama may return malformed JSON)
        if "score" not in evaluation:
            evaluation["score"] = 0.5
        if "flags" not in evaluation:
            evaluation["flags"] = []
        if "notes" not in evaluation:
            evaluation["notes"] = "Evaluation completed"
        if "precept_scores" not in evaluation:
            evaluation["precept_scores"] = {}

        # Create critique message with precept scores
        critique_message = {
            "task_id": task_id,
            "swarm_id": swarm_id,
            "critic_id": critic_id,
            "score": evaluation["score"],
            "precept_scores": evaluation.get("precept_scores", {}),
            "flags": evaluation["flags"],
            "notes": evaluation["notes"],
            "evaluation_method": evaluation.get("evaluation_method", "unknown"),
            "created_at": datetime.utcnow().isoformat()
        }

        # Publish critique
        producer.send("research.critique", value=critique_message)
        producer.flush()

        # Log precept summary
        precept_summary = evaluation.get("precept_scores", {})
        if precept_summary:
            # Convert to float for sorting (LLM may return strings)
            top_precepts = sorted(precept_summary.items(), key=lambda x: float(x[1]) if x[1] else 0, reverse=True)[:3]
            top_str = ", ".join([f"{k[:12]}={v}" for k, v in top_precepts])
            print(f"  → Top precepts: {top_str}")

        print(f"  ✓ Published critique (score: {evaluation['score']}, flags: {evaluation['flags']})")


if __name__ == "__main__":
    main()
