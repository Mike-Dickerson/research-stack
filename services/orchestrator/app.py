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
import re

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

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
            "hypothesis_suggestions": [],
            "status": "RESEARCHING",
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
            "hypothesis_evolution": {
                "original": "",
                "current": "",
                "versions": []
            }
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


def wait_for_kafka_producer(max_retries=30, delay=15):
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


# ============== SEARCH TERM REFINEMENT ==============

# Common stop words to filter out
STOP_WORDS = {
    "the", "a", "an", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had",
    "do", "does", "did", "will", "would", "could", "should", "may", "might", "must", "shall",
    "can", "need", "to", "of", "in", "for", "on", "with", "at", "by", "from", "as", "into",
    "through", "during", "before", "after", "above", "below", "between", "under", "again",
    "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "each",
    "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same",
    "so", "than", "too", "very", "just", "and", "but", "if", "or", "because", "until", "while",
    "this", "that", "these", "those", "it", "its", "they", "their", "them", "we", "our", "you",
    "your", "he", "she", "him", "her", "his", "what", "which", "who", "whom", "whose", "whether",
    "both", "either", "neither", "also", "any", "many", "much", "hypothesis", "research", "study"
}

# Domain-specific alternative terms
DOMAIN_ALTERNATIVES = {
    "biology": ["molecular", "cellular", "genomic", "proteomic", "evolutionary", "ecological"],
    "medical": ["clinical", "therapeutic", "pathological", "epidemiological", "diagnostic"],
    "physics": ["quantum", "relativistic", "thermodynamic", "electromagnetic", "cosmological"],
    "chemistry": ["molecular", "organic", "inorganic", "catalytic", "synthetic"],
    "computer_science": ["computational", "algorithmic", "neural", "statistical", "distributed"],
    "economics": ["macroeconomic", "microeconomic", "monetary", "fiscal", "behavioral"],
    "engineering": ["mechanical", "electrical", "structural", "thermal", "systems"],
    "earth_science": ["geological", "atmospheric", "oceanic", "climatic", "environmental"],
    "mathematics": ["theoretical", "computational", "probabilistic", "statistical", "topological"]
}


def refine_search_terms(hypothesis, domain, previous_terms, critic_concerns):
    """Generate refined search terms based on critic feedback using heuristics"""

    # Extract words from hypothesis
    text = re.sub(r'[^\w\s\-]', ' ', hypothesis)
    words = text.split()

    # Get all previously used terms as a set
    used_terms = set()
    for prev in previous_terms:
        for term in prev.lower().split(","):
            used_terms.add(term.strip())

    # Find candidate terms not yet used
    candidates = []
    for word in words:
        word_lower = word.lower()
        if word_lower not in STOP_WORDS and len(word) > 4 and word_lower not in used_terms:
            candidates.append(word_lower)

    # Add domain-specific alternative terms
    domain_alts = DOMAIN_ALTERNATIVES.get(domain, [])
    for alt in domain_alts:
        if alt not in used_terms:
            candidates.append(alt)

    # Add terms based on critic concerns
    concern_keywords = []
    for concern in critic_concerns[:5]:
        concern_text = str(concern).lower()
        if "evidence" in concern_text:
            concern_keywords.extend(["meta-analysis", "systematic review", "empirical"])
        if "reproducib" in concern_text:
            concern_keywords.extend(["replication", "methodology", "protocol"])
        if "external" in concern_text:
            concern_keywords.extend(["independent", "validation", "verification"])
        if "overconfiden" in concern_text:
            concern_keywords.extend(["limitations", "uncertainty", "caveats"])

    # Remove duplicates and already used
    for kw in concern_keywords:
        if kw not in used_terms:
            candidates.append(kw)

    # Remove duplicates while preserving order
    seen = set()
    unique_candidates = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            unique_candidates.append(c)

    # Take top 5 candidates
    new_terms = ", ".join(unique_candidates[:5])

    # Fallback if we couldn't generate anything new
    if not new_terms:
        new_terms = f"{domain} alternative evidence contradicting supporting"

    print(f"  → Refined search terms: {new_terms[:60]}...")
    return new_terms


# ============== HYPOTHESIS EVOLUTION ==============

def should_modify_hypothesis(task_state, critiques, avg_score, score_variance):
    """
    Determine if hypothesis should be modified vs just search refinement.
    Returns (should_modify: bool, trigger: str or None, suggestions: list)
    """
    iteration = task_state.get("iteration", 1)
    suggestions = task_state.get("hypothesis_suggestions", [])

    # Collect suggestions from critiques
    for critique in critiques:
        critique_suggestions = critique.get("hypothesis_suggestions", [])
        suggestions.extend(critique_suggestions)

    # Decision logic for when to modify hypothesis
    # 1. First iteration with very low score = evidence conflict
    if iteration == 1 and avg_score < 0.4:
        return True, "evidence_conflict", suggestions

    # 2. High variance = divergent opinions = scope too broad
    if score_variance > 0.15:
        return True, "scope_refinement", suggestions

    # 3. Multiple iterations with middling scores = needs precision
    if iteration >= 2 and avg_score < 0.5:
        return True, "precision_improvement", suggestions

    return False, None, suggestions


def generate_modified_hypothesis(current_hypothesis, trigger, suggestions):
    """
    Generate a modified hypothesis based on trigger type.
    Uses heuristic rules (no LLM required).
    Returns (modified_hypothesis: str, reasoning: str)
    """
    modified = current_hypothesis

    if trigger == "evidence_conflict":
        # Add uncertainty qualifiers
        replacements = [
            ("causes ", "may be associated with "),
            ("proves ", "suggests "),
            ("demonstrates ", "indicates "),
            ("shows that ", "provides evidence that "),
            (" is ", " may be "),
            (" are ", " may be "),
        ]
        for old, new in replacements:
            if old in modified.lower():
                # Case-insensitive replacement
                import re
                modified = re.sub(re.escape(old), new, modified, flags=re.IGNORECASE, count=1)
                break
        else:
            # If no replacement made, add qualifier at start
            if not modified.lower().startswith(("it is possible", "evidence suggests", "it may be")):
                modified = "Evidence suggests that " + modified[0].lower() + modified[1:]

        reasoning = "Modified due to evidence conflict: Added uncertainty qualifiers to better reflect available evidence"

    elif trigger == "scope_refinement":
        # Narrow the scope
        replacements = [
            ("all ", "some "),
            ("always ", "often "),
            ("never ", "rarely "),
            ("every ", "many "),
            ("universal", "context-dependent"),
            ("generally ", "in certain cases, "),
        ]
        for old, new in replacements:
            if old in modified.lower():
                import re
                modified = re.sub(re.escape(old), new, modified, flags=re.IGNORECASE, count=1)
                break
        else:
            # Add scope limitation at end
            if not modified.rstrip('.').endswith(("in certain conditions", "in some cases", "under specific circumstances")):
                modified = modified.rstrip('.') + ", under specific conditions."

        reasoning = "Modified due to divergent evaluator opinions: Narrowed scope to reduce ambiguity"

    elif trigger == "precision_improvement":
        # Add precision qualifier
        if not modified.rstrip('.').endswith(("when certain parameters are met", "given specific criteria", "under defined conditions")):
            modified = modified.rstrip('.') + ", when certain parameters are met."

        reasoning = "Modified to improve precision: Added specific conditions to make hypothesis more testable"

    else:
        reasoning = "No modification applied"

    return modified, reasoning


def initialize_hypothesis_evolution(state, hypothesis):
    """Initialize hypothesis evolution tracking for a new task"""
    if not state["hypothesis_evolution"]["original"]:
        state["hypothesis_evolution"] = {
            "original": hypothesis,
            "current": hypothesis,
            "versions": [{
                "version": 1,
                "text": hypothesis,
                "timestamp": datetime.utcnow().isoformat(),
                "trigger": "initial",
                "reasoning": "Original hypothesis as submitted",
                "evidence_refs": []
            }]
        }


def add_hypothesis_version(state, new_hypothesis, trigger, reasoning, evidence_refs=None):
    """Add a new version to hypothesis evolution"""
    versions = state["hypothesis_evolution"]["versions"]
    new_version = len(versions) + 1

    state["hypothesis_evolution"]["current"] = new_hypothesis
    state["hypothesis_evolution"]["versions"].append({
        "version": new_version,
        "text": new_hypothesis,
        "timestamp": datetime.utcnow().isoformat(),
        "trigger": trigger,
        "reasoning": reasoning,
        "evidence_refs": evidence_refs or []
    })

    # Also update the main hypothesis field
    state["hypothesis"] = new_hypothesis

    return new_version


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
            "hypothesis_evolution": state["hypothesis_evolution"],
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

        # Store hypothesis suggestions from critic
        hypothesis_suggestions = critique.get("hypothesis_suggestions", [])
        if hypothesis_suggestions:
            state["hypothesis_suggestions"].extend(hypothesis_suggestions)

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
                "hypothesis_evolution": state.get("hypothesis_evolution", {}),
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
                print(f"\n→ Analyzing whether to modify hypothesis or refine search...")

                # Check if hypothesis should be modified
                should_modify, trigger, suggestions = should_modify_hypothesis(
                    state, critiques_for_task, avg_score, score_variance
                )

                if should_modify and trigger:
                    print(f"  → Hypothesis modification triggered: {trigger}")
                    current_hyp = state["hypothesis_evolution"]["current"]
                    modified_hyp, reasoning = generate_modified_hypothesis(current_hyp, trigger, suggestions)

                    if modified_hyp != current_hyp:
                        new_ver = add_hypothesis_version(state, modified_hyp, trigger, reasoning)
                        print(f"  → Created hypothesis version {new_ver}")
                        print(f"  → Reasoning: {reasoning[:60]}...")

                        log_audit(producer, task_id, "HYPOTHESIS_MODIFIED", {
                            "trigger": trigger,
                            "reasoning": reasoning,
                            "old_hypothesis": current_hyp[:100],
                            "new_hypothesis": modified_hyp[:100],
                            "version": new_ver
                        }, current_iteration)

                # Get refined search terms
                print(f"  → Refining search terms...")
                new_terms = refine_search_terms(
                    state["hypothesis"],
                    state["domain"],
                    state["search_terms_history"],
                    state["critic_feedback"]
                )

                # Republish with new terms (and potentially modified hypothesis)
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
            hypothesis = payload.get("hypothesis", "")
            state["hypothesis"] = hypothesis
            state["domain"] = payload.get("domain", "unknown")
            state["sources"] = payload.get("sources", [])
            state["swarm_id"] = task.get("swarm_id", "")
            # Initialize hypothesis evolution tracking
            initialize_hypothesis_evolution(state, hypothesis)

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
    print("=" * 60)

    # Ensure topics exist
    ensure_topics()
    time.sleep(2)

    # Create producer
    producer = wait_for_kafka_producer()

    # Log startup
    log_audit(producer, "SYSTEM", "ORCHESTRATOR_STARTED", {
        "max_iterations": MAX_ITERATIONS,
        "critiques_for_consensus": CRITIQUES_FOR_CONSENSUS
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
