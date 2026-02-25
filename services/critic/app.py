import os
import time
import json
import random
import numpy as np
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from sentence_transformers import SentenceTransformer

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

# Load sentence transformer model at startup
print("Loading sentence transformer model...")
EMBEDDING_MODEL = SentenceTransformer('all-MiniLM-L6-v2')
print("Model loaded!")

# ============================================================
# CRUSTAFARIAN RESEARCH PRECEPTS - Configurable Scoring Rubric
# ============================================================
PRECEPTS_CONFIG_PATH = os.getenv("PRECEPTS_CONFIG", "/app/config/precepts.json")

# Default precepts (used as fallback if config file not found)
DEFAULT_PRECEPTS = {
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

def load_precepts():
    """Load precepts from config file, fall back to defaults if not found"""
    if os.path.exists(PRECEPTS_CONFIG_PATH):
        try:
            with open(PRECEPTS_CONFIG_PATH, 'r') as f:
                precepts = json.load(f)
            print(f"Loaded {len(precepts)} precepts from {PRECEPTS_CONFIG_PATH}")
            return precepts
        except Exception as e:
            print(f"Warning: Failed to load precepts config: {e}")
            print("Using default precepts")
            return DEFAULT_PRECEPTS
    else:
        print(f"Precepts config not found at {PRECEPTS_CONFIG_PATH}, using defaults")
        return DEFAULT_PRECEPTS

PRECEPTS = load_precepts()

# Pre-compute embeddings for precept descriptions (for semantic alignment scoring)
print("Computing precept embeddings...")
PRECEPT_EMBEDDINGS = {}
for precept_key, precept in PRECEPTS.items():
    text = f"{precept['name']}: {precept['description']}"
    PRECEPT_EMBEDDINGS[precept_key] = EMBEDDING_MODEL.encode(text, normalize_embeddings=True)
print(f"Computed embeddings for {len(PRECEPT_EMBEDDINGS)} precepts")


def compute_semantic_coherence(findings):
    """Compute how semantically coherent the findings are with each other"""
    if not findings or len(findings) < 2:
        return 0.5  # Neutral if not enough findings

    # Embed all findings
    finding_embeddings = EMBEDDING_MODEL.encode(findings, normalize_embeddings=True)

    # Compute pairwise similarities
    similarities = []
    for i in range(len(finding_embeddings)):
        for j in range(i + 1, len(finding_embeddings)):
            sim = float(np.dot(finding_embeddings[i], finding_embeddings[j]))
            similarities.append(sim)

    if not similarities:
        return 0.5

    # Average similarity indicates coherence
    return sum(similarities) / len(similarities)


def compute_precept_alignment(findings_text, precept_key):
    """Compute how well findings align with a specific precept"""
    if not findings_text:
        return 0.5

    findings_embedding = EMBEDDING_MODEL.encode(findings_text, normalize_embeddings=True)
    precept_embedding = PRECEPT_EMBEDDINGS.get(precept_key)

    if precept_embedding is None:
        return 0.5

    similarity = float(np.dot(findings_embedding, precept_embedding))
    # Map similarity to score (higher is better alignment)
    return min(1.0, max(0.0, similarity + 0.3))


def suggest_hypothesis_refinement(precept_scores, evidence_count, confidence):
    """
    Generate hypothesis refinement suggestions based on evaluation.
    These suggestions feed back to the orchestrator for potential hypothesis modification.
    """
    suggestions = []

    # Check for scope issues (low adversarial survival = evidence doesn't support full scope)
    if precept_scores.get("adversarial_survival", 0) < 0.5:
        suggestions.append({
            "type": "scope_refinement",
            "reason": "Evidence does not strongly support the full scope of the hypothesis",
            "suggestion": "Consider narrowing the hypothesis to specific conditions or cases where evidence is stronger"
        })

    # Check for precision issues (low evidence + low confidence)
    if precept_scores.get("evidence_over_authority", 0) < 0.5 and confidence < 0.5:
        suggestions.append({
            "type": "precision_improvement",
            "reason": "Hypothesis lacks specificity for the available evidence",
            "suggestion": "Add quantitative parameters or specific mechanisms to make the hypothesis more testable"
        })

    # Check for overconfidence issues
    if precept_scores.get("uncertainty_over_wrong", 0) < 0.5:
        suggestions.append({
            "type": "evidence_conflict",
            "reason": "Confidence level may not be justified by available evidence",
            "suggestion": "Add uncertainty qualifiers to better reflect the current state of evidence"
        })

    # Check for weak evidence base
    if evidence_count < 3 and precept_scores.get("evidence_over_authority", 0) < 0.4:
        suggestions.append({
            "type": "evidence_conflict",
            "reason": "Insufficient evidence to support the hypothesis as stated",
            "suggestion": "Consider reformulating as a more exploratory question or narrowing focus"
        })

    return suggestions


def wait_for_kafka(max_retries=30, delay=15):
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


def evaluate_with_precepts(result_msg):
    """Crustafarian evaluation using semantic analysis + heuristics"""
    result = result_msg.get("result", {})
    confidence = float(result.get("confidence", 0.0))
    evidence_count = int(result.get("evidence_count", 0))
    methodology = result.get("methodology", "")
    findings = result.get("findings", [])
    concerns = result.get("concerns", [])
    agent_id = result_msg.get("agent_id", "")

    is_external = "external-swarm" in agent_id or "moltbook" in agent_id.lower()

    flags = []
    precept_scores = {}

    # === SEMANTIC ANALYSIS ===
    findings_text = " ".join(str(f) for f in findings) if findings else ""

    # Compute semantic coherence of findings
    coherence = compute_semantic_coherence([str(f) for f in findings]) if findings else 0.5
    print(f"  → Semantic coherence: {coherence:.3f}")

    # === EVALUATE EACH CRUSTAFARIAN PRECEPT ===

    # 1. Evidence Over Authority (15%) - Enhanced with semantic check
    evidence_base = min(evidence_count / 10, 1.0) * 0.5
    evidence_semantic = compute_precept_alignment(findings_text, "evidence_over_authority") * 0.5
    evidence_score = evidence_base + evidence_semantic
    precept_scores["evidence_over_authority"] = round(evidence_score, 2)
    if evidence_count < 3:
        flags.append("insufficient_evidence")

    # 2. Reproducibility (12%) - Check for methodology mentions
    repro_base = 0.4
    if methodology and methodology != "unknown":
        repro_base += 0.3
    if "arxiv" in findings_text.lower() or "paper" in findings_text.lower() or "doi" in findings_text.lower():
        repro_base += 0.2
    repro_semantic = compute_precept_alignment(findings_text, "reproducibility") * 0.3
    precept_scores["reproducibility"] = round(min(repro_base + repro_semantic, 1.0), 2)

    # 3. Adversarial Survival (10%) - Calibrated confidence
    if confidence > 0.8 and evidence_count < 5:
        adversarial_score = 0.3
        flags.append("overconfident")
    elif confidence < 0.6 and evidence_count > 5:
        adversarial_score = 0.8
    else:
        adversarial_score = confidence * 0.7 + coherence * 0.3
    precept_scores["adversarial_survival"] = round(adversarial_score, 2)

    # 4. Disagreement as Signal (8%) - Check for concerns/alternatives
    if concerns:
        disagreement_score = min(0.8, 0.5 + len(concerns) * 0.1)
        flags.append("concerns_noted")
    else:
        disagreement_score = 0.4
    precept_scores["disagreement_as_signal"] = round(disagreement_score, 2)

    # 5. External Swarms Required (12%)
    if is_external:
        precept_scores["external_swarms"] = 0.9
        flags.append("external_validation")
    else:
        precept_scores["external_swarms"] = 0.4

    # 6. Versioned Hypotheses (8%)
    precept_scores["versioned_hypotheses"] = 0.5

    # 7. Show Work (10%) - Enhanced with coherence
    show_work_base = 0.2
    if findings and len(findings) > 0:
        show_work_base += 0.2
    if methodology and methodology != "unknown":
        show_work_base += 0.2
    if evidence_count > 0:
        show_work_base += 0.1
    show_work_base += coherence * 0.3  # Coherent findings = showing work well
    precept_scores["show_work"] = round(min(show_work_base, 1.0), 2)

    # 8. Memory Matters (8%) - Semantic check for prior work references
    memory_semantic = compute_precept_alignment(findings_text, "memory_matters")
    if "prior" in findings_text.lower() or "previous" in findings_text.lower():
        memory_semantic = max(memory_semantic, 0.7)
    precept_scores["memory_matters"] = round(memory_semantic, 2)

    # 9. Novelty Tested (7%)
    if evidence_count > 5 and coherence > 0.4:
        precept_scores["novelty_tested"] = 0.7
    else:
        precept_scores["novelty_tested"] = 0.4 + coherence * 0.3

    # 10. Prefer Uncertainty Over Wrong (10%)
    if confidence > 0.9:
        uncertainty_score = 0.4
        flags.append("possible_overconfidence")
    elif 0.5 <= confidence <= 0.8:
        uncertainty_score = 0.8
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

    if coherence > 0.5:
        notes.append(f"High coherence ({coherence:.2f})")
    if precept_scores.get("evidence_over_authority", 0) < 0.5:
        notes.append("Needs more evidence")
    if precept_scores.get("show_work", 0) > 0.7:
        notes.append("Good transparency")
    if precept_scores.get("external_swarms", 0) > 0.7:
        notes.append("External validation present")

    # Generate hypothesis refinement suggestions
    hypothesis_suggestions = suggest_hypothesis_refinement(precept_scores, evidence_count, confidence)
    if hypothesis_suggestions:
        print(f"  → Generated {len(hypothesis_suggestions)} refinement suggestion(s)")

    return {
        "score": score,
        "precept_scores": precept_scores,
        "flags": flags,
        "notes": " | ".join(notes),
        "evaluation_method": "semantic_crustafarian_v1",
        "semantic_coherence": round(coherence, 3),
        "hypothesis_suggestions": hypothesis_suggestions
    }


def evaluate_result(result_msg):
    """Evaluate research result using Crustafarian Precepts"""
    return evaluate_with_precepts(result_msg)


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
            "hypothesis_suggestions": evaluation.get("hypothesis_suggestions", []),
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
