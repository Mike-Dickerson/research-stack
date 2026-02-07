"""
Research Orchestrator - Manages the iterative research validation pipeline.

Features:
- Creates and manages Kafka topics
- Tracks task state with iteration counts
- Listens to consensus and triggers retries when NEEDS_MORE_RESEARCH
- Uses Ollama to refine search terms based on critic feedback
- Logs every action to research.audit for full traceability
- Fails hypotheses after MAX_ITERATIONS (3) unsuccessful attempts
"""

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
import ollama

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://ollama:11434")
OLLAMA_MODEL = "phi3:mini"

MAX_ITERATIONS = 3
CRITIQUES_FOR_CONSENSUS = 2

TOPICS = [
    "research.task",
    "research.result",
    "research.critique",
    "research.consensus",
    "research.cancel",
    "research.audit"
]

# ============== TASK STATE TRACKING ==============

# Track state for each task across iterations
# Key: task_id, Value: task state dict
task_states = {}

def get_task_state(task_id):
    """Get or create task state"""
    if task_id not in task_states:
        task_states[task_id] = {
            "task_id": task_id,
            "iteration": 1,
            "max_iterations": MAX_ITERATIONS,
            "hypothesis": "",
            "domain": "unknown",
            "sources": [],
            "search_terms_history": [],
            "critic_feedback": [],
            "status": "RESEARCHING",
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat()
        }
    return task_states[task_id]


def update_task_state(task_id, updates):
    """Update task state with new values"""
    state = get_task_state(task_id)
    state.update(updates)
    state["updated_at"] = datetime.utcnow().isoformat()
    return state


# ============== KAFKA SETUP ==============

def ensure_topics():
    """Create Kafka topics if they don't exist"""
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
        print(f"Topic setup error: {e}")


def wait_for_kafka_producer(max_retries=30, delay=2):
    """Wait for Kafka to be available and return a producer"""
    for attempt in range(max_retries):
        try:
            print(f"Connecting to Kafka producer (attempt {attempt + 1}/{max_retries})...")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Producer connected!")
            return producer
        except NoBrokersAvailable:
            if attempt < max_retries - 1:
                print(f"Kafka not available, waiting {delay}s...")
                time.sleep(delay)
            else:
                raise
    return None


# ============== AUDIT LOGGING ==============

def log_audit(producer, task_id, action, details, iteration=None):
    """Log an audit event for full traceability"""
    audit_event = {
        "timestamp": datetime.utcnow().isoformat(),
        "task_id": task_id,
        "action": action,
        "iteration": iteration,
        "details": details,
        "service": "orchestrator"
    }

    producer.send("research.audit", value=audit_event)
    producer.flush()

    print(f"  [AUDIT] {action}: {str(details)[:80]}...")


# ============== OLLAMA INTEGRATION ==============

def refine_search_terms(hypothesis, domain, previous_terms, critic_concerns):
    """Use Ollama to generate refined search terms based on critic feedback"""
    try:
        client = ollama.Client(host=OLLAMA_HOST)

        concerns_text = "\n".join([f"- {c}" for c in critic_concerns[:5]])
        previous_terms_text = ", ".join(previous_terms) if previous_terms else "None"

        prompt = f"""You are a research methodology expert. A hypothesis failed to reach consensus and needs refined search terms.

HYPOTHESIS: {hypothesis}
DOMAIN: {domain}

PREVIOUS SEARCH TERMS (didn't work well):
{previous_terms_text}

CRITIC CONCERNS:
{concerns_text}

Generate NEW search terms that:
1. Address the critic's specific concerns
2. Are different from previous attempts
3. May find supporting OR contradicting evidence
4. Use alternative scientific terminology
5. Focus on testable aspects of the hypothesis

Return ONLY a comma-separated list of 4-6 search terms. No explanation."""

        response = client.chat(
            model=OLLAMA_MODEL,
            messages=[{"role": "user", "content": prompt}]
        )

        new_terms = response['message']['content'].strip()
        print(f"  → Refined search terms: {new_terms[:60]}...")
        return new_terms

    except Exception as e:
        print(f"  ⚠ Ollama refinement error: {e}")
        # Fallback: slightly modify the hypothesis as search terms
        return hypothesis[:100] + " alternative evidence"


# ============== CRUSTAFARIAN PRECEPTS ==============

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


# ============== TASK PUBLISHING ==============

def create_research_task(producer, hypothesis, domain, sources, search_terms, iteration=1, swarm_id=None):
    """Create and publish a research task"""
    task_id = str(uuid.uuid4())
    if swarm_id is None:
        swarm_id = str(uuid.uuid4())

    task = {
        "task_id": task_id,
        "swarm_id": swarm_id,
        "task_type": "hypothesis_research",
        "iteration": iteration,
        "payload": {
            "hypothesis": hypothesis,
            "domain": domain,
            "sources": sources,
            "search_terms": search_terms,
            "iteration": iteration,
            "max_iterations": MAX_ITERATIONS,
            "precepts": CRUSTAFARIAN_PRECEPTS,
            "evaluation_criteria": [
                "evidence_over_authority",
                "reproducibility",
                "adversarial_survival",
                "disagreement_as_signal",
                "external_swarms"
            ]
        },
        "created_at": datetime.utcnow().isoformat()
    }

    # Initialize task state
    state = get_task_state(task_id)
    state["hypothesis"] = hypothesis
    state["domain"] = domain
    state["sources"] = sources
    state["iteration"] = iteration
    state["search_terms_history"].append(search_terms)
    state["swarm_id"] = swarm_id

    # Publish task
    producer.send("research.task", value=task)
    producer.flush()

    # Audit log
    log_audit(producer, task_id, "TASK_CREATED", {
        "hypothesis": hypothesis[:100],
        "domain": domain,
        "search_terms": search_terms,
        "iteration": iteration
    }, iteration)

    return task_id


def republish_task_with_refinement(producer, task_id, new_search_terms):
    """Republish a task with refined search terms for next iteration"""
    state = get_task_state(task_id)

    new_iteration = state["iteration"] + 1

    # Create new task message (same task_id, incremented iteration)
    task = {
        "task_id": task_id,
        "swarm_id": state.get("swarm_id", str(uuid.uuid4())),
        "task_type": "hypothesis_research",
        "iteration": new_iteration,
        "payload": {
            "hypothesis": state["hypothesis"],
            "domain": state["domain"],
            "sources": state["sources"],
            "search_terms": new_search_terms,
            "iteration": new_iteration,
            "max_iterations": MAX_ITERATIONS,
            "previous_search_terms": state["search_terms_history"],
            "critic_feedback": state["critic_feedback"],
            "precepts": CRUSTAFARIAN_PRECEPTS,
            "evaluation_criteria": [
                "evidence_over_authority",
                "reproducibility",
                "adversarial_survival",
                "disagreement_as_signal",
                "external_swarms"
            ]
        },
        "created_at": datetime.utcnow().isoformat()
    }

    # Update state
    state["iteration"] = new_iteration
    state["search_terms_history"].append(new_search_terms)
    state["status"] = "RESEARCHING"
    state["updated_at"] = datetime.utcnow().isoformat()

    # Publish
    producer.send("research.task", value=task)
    producer.flush()

    # Audit log
    log_audit(producer, task_id, "TASK_RETRY", {
        "new_search_terms": new_search_terms,
        "previous_attempts": new_iteration - 1,
        "critic_concerns": state["critic_feedback"][-3:] if state["critic_feedback"] else []
    }, new_iteration)

    return new_iteration


# ============== CONSENSUS MONITOR ==============

def consensus_monitor(producer):
    """
    Monitor critiques and build consensus.
    When consensus is reached, decide whether to APPROVE, RETRY, or FAIL.
    """
    print("Consensus Monitor starting...")

    # Wait for Kafka consumer
    for attempt in range(30):
        try:
            consumer = KafkaConsumer(
                "research.critique",
                bootstrap_servers=KAFKA,
                auto_offset_reset="latest",
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

    print("Monitoring critiques for consensus...\n")

    for msg in consumer:
        critique = msg.value
        task_id = critique.get("task_id", "unknown")
        score = critique.get("score", 0.0)
        flags = critique.get("flags", [])
        notes = critique.get("notes", "")

        # Get task state
        state = get_task_state(task_id)

        # Skip if already completed
        if state["status"] in ["APPROVED", "FAILED"]:
            continue

        # Collect critique
        task_critiques[task_id].append(critique)

        # Store feedback for potential refinement
        if flags:
            state["critic_feedback"].extend(flags)
        if notes:
            if isinstance(notes, list):
                state["critic_feedback"].extend(notes)
            else:
                state["critic_feedback"].append(str(notes))

        critiques_for_task = task_critiques[task_id]

        # Check if we have enough critiques for consensus
        if len(critiques_for_task) >= CRITIQUES_FOR_CONSENSUS:
            # Calculate consensus metrics
            scores = [float(c.get("score") or 0.0) for c in critiques_for_task]
            avg_score = sum(scores) / len(scores)
            score_variance = sum((s - avg_score) ** 2 for s in scores) / len(scores)

            # Determine consensus status
            if score_variance < 0.05:
                consensus_status = "STRONG_CONSENSUS"
            elif score_variance < 0.15:
                consensus_status = "MODERATE_CONSENSUS"
            else:
                consensus_status = "DIVERGENT_OPINIONS"

            # Check external swarm participation
            has_external = any("external" in str(c.get("notes", "")).lower() for c in critiques_for_task)

            consensus_count += 1
            current_iteration = state["iteration"]

            # Determine decision based on score and iteration
            if avg_score >= 0.6:
                decision = "APPROVED"
                state["status"] = "APPROVED"
            elif current_iteration >= MAX_ITERATIONS:
                decision = "FAILED"
                state["status"] = "FAILED"
            else:
                decision = "NEEDS_MORE_RESEARCH"
                # Will trigger retry below

            # Create consensus message
            consensus_msg = {
                "task_id": task_id,
                "swarm_id": state.get("swarm_id", "unknown"),
                "consensus_status": consensus_status,
                "average_score": round(avg_score, 3),
                "score_variance": round(score_variance, 4),
                "critique_count": len(critiques_for_task),
                "external_swarm_participated": has_external,
                "decision": decision,
                "iteration": current_iteration,
                "max_iterations": MAX_ITERATIONS,
                "created_at": datetime.utcnow().isoformat()
            }

            # Publish consensus
            producer.send("research.consensus", value=consensus_msg)
            producer.flush()

            # Print decision
            print(f"\n{'='*70}")
            print(f"CONSENSUS #{consensus_count} | Iteration {current_iteration}/{MAX_ITERATIONS}")
            print(f"{'='*70}")
            print(f"Task: {task_id[:8]}...")
            print(f"Hypothesis: {state['hypothesis'][:60]}...")
            print(f"Status: {consensus_status}")
            print(f"Average Score: {avg_score:.3f}")
            print(f"Variance: {score_variance:.4f}")
            print(f"External Swarm: {'Yes' if has_external else 'No'}")
            print(f"Decision: {decision}")

            # Audit log
            log_audit(producer, task_id, f"CONSENSUS_{decision}", {
                "average_score": avg_score,
                "variance": score_variance,
                "critique_count": len(critiques_for_task),
                "external_participated": has_external
            }, current_iteration)

            # Handle decision
            if decision == "NEEDS_MORE_RESEARCH":
                print(f"\n→ Refining search terms for retry...")

                # Get refined search terms using Ollama
                new_terms = refine_search_terms(
                    state["hypothesis"],
                    state["domain"],
                    state["search_terms_history"],
                    state["critic_feedback"]
                )

                # Republish with new terms
                new_iteration = republish_task_with_refinement(producer, task_id, new_terms)
                print(f"→ Republished task for iteration {new_iteration}")

            elif decision == "APPROVED":
                print(f"\n✓ HYPOTHESIS APPROVED!")
                log_audit(producer, task_id, "HYPOTHESIS_APPROVED", {
                    "final_score": avg_score,
                    "iterations_used": current_iteration,
                    "hypothesis": state["hypothesis"]
                }, current_iteration)

            elif decision == "FAILED":
                print(f"\n✗ HYPOTHESIS FAILED after {MAX_ITERATIONS} iterations")
                log_audit(producer, task_id, "HYPOTHESIS_FAILED", {
                    "final_score": avg_score,
                    "iterations_used": current_iteration,
                    "critic_feedback": state["critic_feedback"][-5:],
                    "hypothesis": state["hypothesis"]
                }, current_iteration)

            print(f"{'='*70}\n")

            # Clean up critiques for this task (keep state for reference)
            del task_critiques[task_id]


# ============== TASK LISTENER ==============

def task_listener():
    """
    Listen to research.task to track initial task submissions.
    Updates task state when new tasks come in from hypothesis-manager.
    """
    print("Task Listener starting...")

    for attempt in range(30):
        try:
            consumer = KafkaConsumer(
                "research.task",
                bootstrap_servers=KAFKA,
                auto_offset_reset="latest",
                group_id="orchestrator-task-tracker",
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            print("Task Listener connected")
            break
        except:
            time.sleep(2)

    for msg in consumer:
        task = msg.value
        task_id = task.get("task_id", "unknown")
        payload = task.get("payload", {})
        iteration = task.get("iteration", payload.get("iteration", 1))

        # Update state with task info
        state = get_task_state(task_id)

        # Only update if this is a new task (iteration 1) or if we don't have hypothesis yet
        if not state["hypothesis"]:
            state["hypothesis"] = payload.get("hypothesis", "")
            state["domain"] = payload.get("domain", "unknown")
            state["sources"] = payload.get("sources", [])
            state["swarm_id"] = task.get("swarm_id", "")

        # Always update iteration
        state["iteration"] = iteration

        if iteration == 1:
            search_terms = payload.get("search_terms", "")
            if search_terms and search_terms not in state["search_terms_history"]:
                state["search_terms_history"].append(search_terms)

        state["status"] = "RESEARCHING"
        state["updated_at"] = datetime.utcnow().isoformat()


# ============== CANCELLATION LISTENER ==============

def cancellation_listener():
    """Listen for task cancellations"""
    print("Cancellation Listener starting...")

    for attempt in range(30):
        try:
            consumer = KafkaConsumer(
                "research.cancel",
                bootstrap_servers=KAFKA,
                auto_offset_reset="latest",
                group_id="orchestrator-cancel",
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            print("Cancellation Listener connected")
            break
        except:
            time.sleep(2)

    for msg in consumer:
        cancel_msg = msg.value
        task_id = cancel_msg.get("task_id", "")

        if task_id and task_id in task_states:
            task_states[task_id]["status"] = "CANCELLED"
            print(f"  ⚠ Task {task_id[:8]}... cancelled")


# ============== MAIN ==============

def main():
    print("=" * 60)
    print("RESEARCH ORCHESTRATOR")
    print("Iterative Hypothesis Validation Pipeline")
    print("=" * 60)
    print(f"Max iterations: {MAX_ITERATIONS}")
    print(f"Critiques for consensus: {CRITIQUES_FOR_CONSENSUS}")
    print(f"Ollama model: {OLLAMA_MODEL}")
    print("=" * 60)

    # Ensure topics exist
    ensure_topics()
    time.sleep(2)

    # Create producer
    producer = wait_for_kafka_producer()

    # Log startup
    log_audit(producer, "SYSTEM", "ORCHESTRATOR_STARTED", {
        "max_iterations": MAX_ITERATIONS,
        "critiques_for_consensus": CRITIQUES_FOR_CONSENSUS,
        "ollama_model": OLLAMA_MODEL
    })

    # Start background threads
    Thread(target=consensus_monitor, args=(producer,), daemon=True).start()
    print("Consensus monitor started")

    Thread(target=task_listener, daemon=True).start()
    print("Task listener started")

    Thread(target=cancellation_listener, daemon=True).start()
    print("Cancellation listener started")

    print("\nOrchestrator running. Monitoring research pipeline...\n")

    # Keep main thread alive
    while True:
        time.sleep(60)

        # Periodic status report
        active_tasks = sum(1 for s in task_states.values() if s["status"] == "RESEARCHING")
        approved = sum(1 for s in task_states.values() if s["status"] == "APPROVED")
        failed = sum(1 for s in task_states.values() if s["status"] == "FAILED")

        if task_states:
            print(f"[STATUS] Active: {active_tasks} | Approved: {approved} | Failed: {failed} | Total tracked: {len(task_states)}")


if __name__ == "__main__":
    main()
