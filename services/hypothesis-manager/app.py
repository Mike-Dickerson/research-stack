#!/usr/bin/env python3
"""
Research Stack - Hypothesis Manager
Submit, track, and manage research hypotheses
"""

import os
import json
import time
import uuid
import glob
import threading
import re
import numpy as np
from datetime import datetime
from flask import Flask, render_template_string, request, redirect, url_for, send_from_directory, session
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from sentence_transformers import SentenceTransformer

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "hypothesis-manager-secret-key")

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
PUBLICATIONS_DIR = "/app/publications"
PRECEPTS_CONFIG_PATH = os.getenv("PRECEPTS_CONFIG", "/app/config/precepts.json")

# Load sentence transformer model (small, fast, ~22MB)
print("Loading sentence transformer model...")
EMBEDDING_MODEL = SentenceTransformer('all-MiniLM-L6-v2')
print("Model loaded!")

# Research domains and their descriptions
RESEARCH_DOMAINS = {
    "biology": {
        "name": "Biology / Life Sciences",
        "description": "Genetics, cell biology, evolution, ecology, microbiology",
        "sources": ["pubmed", "semantic_scholar"]
    },
    "medical": {
        "name": "Medical / Health Sciences",
        "description": "Medicine, pharmacology, clinical research, public health",
        "sources": ["pubmed", "semantic_scholar"]
    },
    "physics": {
        "name": "Physics / Astronomy",
        "description": "Particle physics, cosmology, astrophysics, quantum mechanics",
        "sources": ["arxiv", "nasa_ads"]
    },
    "chemistry": {
        "name": "Chemistry / Materials",
        "description": "Organic, inorganic, materials science, nanotechnology",
        "sources": ["arxiv", "semantic_scholar"]
    },
    "computer_science": {
        "name": "Computer Science / AI",
        "description": "Machine learning, algorithms, software, cybersecurity",
        "sources": ["arxiv", "semantic_scholar"]
    },
    "economics": {
        "name": "Economics / Social Sciences",
        "description": "Economics, psychology, sociology, political science",
        "sources": ["semantic_scholar"]
    },
    "engineering": {
        "name": "Engineering / Technology",
        "description": "Electrical, mechanical, civil, aerospace engineering",
        "sources": ["arxiv", "semantic_scholar"]
    },
    "earth_science": {
        "name": "Earth / Environmental Science",
        "description": "Climate, geology, oceanography, environmental studies",
        "sources": ["arxiv", "semantic_scholar"]
    },
    "mathematics": {
        "name": "Mathematics / Statistics",
        "description": "Pure math, applied math, statistics, probability",
        "sources": ["arxiv"]
    },
    "unknown": {
        "name": "Unclear / Multi-disciplinary",
        "description": "Domain could not be determined automatically",
        "sources": ["semantic_scholar"]
    }
}

# Rich domain descriptions for semantic matching
DOMAIN_DESCRIPTIONS = {
    "biology": "Genetics, DNA, RNA, proteins, cells, organisms, evolution, mutation, bacteria, viruses, enzymes, chromosomes, genomes, ecology, ecosystems, biodiversity, metabolism, stem cells, telomeres, mitochondria, cell biology, molecular biology, microbiology",
    "medical": "Medicine, patients, diseases, treatments, therapy, drugs, clinical trials, diagnosis, symptoms, cancer, tumors, infections, pharmaceuticals, vaccines, antibodies, inflammation, pathology, epidemiology, public health, mortality, healthcare",
    "physics": "Quantum mechanics, particles, waves, photons, electrons, protons, neutrons, atoms, energy, mass, gravity, relativity, spacetime, black holes, dark matter, dark energy, cosmology, universe, galaxies, stars, quarks, bosons, thermodynamics, electromagnetism",
    "chemistry": "Molecules, compounds, chemical reactions, catalysts, bonds, organic chemistry, inorganic chemistry, synthesis, polymers, ions, acids, bases, oxidation, reduction, solvents, solutions, crystalline structures, nanoparticles, materials science",
    "computer_science": "Algorithms, neural networks, machine learning, artificial intelligence, deep learning, data science, databases, software engineering, computing, programming, optimization, classification, natural language processing, computer vision, robotics, cybersecurity, encryption",
    "economics": "Markets, economic theory, prices, supply and demand, trade, GDP, inflation, monetary policy, fiscal policy, investment, capital, labor markets, unemployment, economic growth, recession, macroeconomics, microeconomics, finance",
    "engineering": "Design, systems engineering, mechanical engineering, electrical engineering, circuits, structural analysis, materials, load bearing, stress analysis, efficiency, power systems, voltage, aerospace engineering, thermal systems, civil engineering",
    "earth_science": "Climate science, weather, atmosphere, oceans, geology, earthquakes, volcanoes, sediments, erosion, glaciers, ice sheets, carbon cycle, emissions, temperature, precipitation, fossils, minerals, plate tectonics, environmental science",
    "mathematics": "Theorems, proofs, equations, functions, variables, matrices, vectors, integrals, derivatives, probability theory, statistics, topology, algebra, geometry, calculus, convergence, infinite series, number theory, mathematical analysis"
}

# Pre-compute domain embeddings at startup
print("Computing domain embeddings...")
DOMAIN_EMBEDDINGS = {}
for domain, description in DOMAIN_DESCRIPTIONS.items():
    DOMAIN_EMBEDDINGS[domain] = EMBEDDING_MODEL.encode(description, normalize_embeddings=True)
print(f"Computed embeddings for {len(DOMAIN_EMBEDDINGS)} domains")

# Common stop words to filter out
STOP_WORDS = {
    "the", "a", "an", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had",
    "do", "does", "did", "will", "would", "could", "should", "may", "might", "must", "shall",
    "can", "need", "dare", "ought", "used", "to", "of", "in", "for", "on", "with", "at", "by",
    "from", "as", "into", "through", "during", "before", "after", "above", "below", "between",
    "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how",
    "all", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only",
    "own", "same", "so", "than", "too", "very", "just", "and", "but", "if", "or", "because",
    "until", "while", "this", "that", "these", "those", "it", "its", "they", "their", "them",
    "we", "our", "you", "your", "he", "she", "him", "her", "his", "what", "which", "who",
    "whom", "whose", "whether", "both", "either", "neither", "also", "any", "many", "much"
}


def detect_domain(hypothesis):
    """Semantic domain detection using sentence embeddings"""
    # Embed the hypothesis
    hypothesis_embedding = EMBEDDING_MODEL.encode(hypothesis, normalize_embeddings=True)

    # Compute cosine similarity with each domain (dot product since normalized)
    similarities = {}
    for domain, domain_embedding in DOMAIN_EMBEDDINGS.items():
        similarity = np.dot(hypothesis_embedding, domain_embedding)
        similarities[domain] = float(similarity)

    # Find the best matching domain
    best_domain = max(similarities, key=similarities.get)
    best_score = similarities[best_domain]

    # If similarity is too low, return unknown
    if best_score < 0.3:
        print(f"Domain detection: unknown (best was {best_domain} at {best_score:.3f})")
        return "unknown"

    print(f"Domain detection: {best_domain} (similarity: {best_score:.3f})")
    return best_domain


def extract_search_terms(hypothesis, domain):
    """Extract search terms using semantic similarity to domain"""
    # Remove punctuation except hyphens
    text = re.sub(r'[^\w\s\-]', ' ', hypothesis)
    words = text.split()

    # Filter out stop words and short words
    candidates = []
    i = 0
    while i < len(words):
        word = words[i]
        word_lower = word.lower()

        if word_lower in STOP_WORDS or len(word) < 3:
            i += 1
            continue

        # Check for two-word phrases
        if i + 1 < len(words) and words[i + 1].lower() not in STOP_WORDS:
            phrase = f"{word} {words[i + 1]}"
            candidates.append(phrase.lower())
            i += 2
            continue

        if len(word) > 4:  # Only words longer than 4 chars
            candidates.append(word_lower)
        i += 1

    # If we have candidates, rank them by similarity to domain description
    if candidates and domain in DOMAIN_DESCRIPTIONS:
        domain_embedding = DOMAIN_EMBEDDINGS.get(domain)
        if domain_embedding is not None:
            # Embed all candidates at once for efficiency
            candidate_embeddings = EMBEDDING_MODEL.encode(candidates, normalize_embeddings=True)

            # Score each candidate by similarity to domain
            scored = []
            for i, candidate in enumerate(candidates):
                similarity = np.dot(candidate_embeddings[i], domain_embedding)
                scored.append((candidate, similarity))

            # Sort by similarity and take top 5
            scored.sort(key=lambda x: x[1], reverse=True)
            terms = [term for term, _ in scored[:5]]
        else:
            terms = candidates[:5]
    else:
        terms = candidates[:5] if candidates else [hypothesis[:100]]

    result = ", ".join(terms)
    print(f"Search terms: {result}")
    return result

# In-memory hypothesis tracking
hypotheses = {}  # task_id -> hypothesis data
results = {}     # task_id -> list of results
critiques = {}   # task_id -> list of critiques
cancelled_tasks = set()  # task_ids that have been deleted/cancelled

def wait_for_kafka_producer(max_retries=30, delay=15):
    """Wait for Kafka to be available and return a producer"""
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            return producer
        except NoBrokersAvailable:
            if attempt < max_retries - 1:
                time.sleep(delay)
            else:
                raise
    return None

producer = None

def get_producer():
    global producer
    if producer is None:
        producer = wait_for_kafka_producer()
    return producer

def ensure_hypothesis_exists(task_id):
    """Create a placeholder hypothesis entry if it doesn't exist"""
    if task_id and task_id not in hypotheses:
        hypotheses[task_id] = {
            'task_id': task_id,
            'swarm_id': '',
            'hypothesis': '(loading...)',
            'status': 'SUBMITTED',
            'score': None,
            'decision': None,
            'consensus_status': None,
            'submitted_at': datetime.utcnow().isoformat(),
            'completed_at': None,
            'result_count': 0,
            'critique_count': 0,
            'external_participated': False,
            'iteration': 1,
            'max_iterations': 3
        }
        results[task_id] = []
        critiques[task_id] = []

def kafka_listener():
    """Background thread to track hypothesis status from Kafka"""
    # Use unique group ID to always read from beginning and rebuild state
    unique_group = f"hypothesis-manager-{uuid.uuid4()}"

    print("Kafka listener starting...")

    # Wait for Kafka
    consumer = None
    for attempt in range(30):
        try:
            consumer = KafkaConsumer(
                'research.task',
                'research.result',
                'research.critique',
                'research.consensus',
                bootstrap_servers=KAFKA,
                auto_offset_reset='earliest',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id=unique_group,
                consumer_timeout_ms=2000  # 2 second timeout for polling
            )
            print(f"Hypothesis Manager connected to Kafka (group: {unique_group[:20]}...)")
            break
        except Exception as e:
            print(f"Kafka connection attempt {attempt + 1} failed: {e}")
            time.sleep(2)

    if not consumer:
        print("Failed to connect to Kafka!")
        return

    msg_count = 0
    while True:
        try:
            for msg in consumer:
                msg_count += 1
                topic = msg.topic
                event = msg.value
                task_id = event.get('task_id', '')

                if not task_id:
                    continue

                # Skip cancelled/deleted tasks
                if task_id in cancelled_tasks:
                    continue

                if msg_count <= 10 or msg_count % 20 == 0:
                    print(f"[{msg_count}] {topic}: task_id={task_id[:8]}...")

                # Ensure hypothesis entry exists for any task_id we see
                ensure_hypothesis_exists(task_id)

                if topic == 'research.task':
                    hypothesis_text = event.get('payload', {}).get('hypothesis', '')
                    swarm_id = event.get('swarm_id', '')
                    hypotheses[task_id]['hypothesis'] = hypothesis_text
                    hypotheses[task_id]['swarm_id'] = swarm_id
                    hypotheses[task_id]['submitted_at'] = event.get('created_at', hypotheses[task_id]['submitted_at'])

                elif topic == 'research.result':
                    # Only update status if not already completed
                    if hypotheses[task_id]['status'] != 'COMPLETED':
                        hypotheses[task_id]['status'] = 'PROCESSING'
                    hypotheses[task_id]['result_count'] = hypotheses[task_id].get('result_count', 0) + 1

                    # Store result details - extract from nested 'result' object
                    if task_id not in results:
                        results[task_id] = []
                    result_data = event.get('result', {})
                    results[task_id].append({
                        'agent_id': event.get('agent_id', 'unknown'),
                        'confidence': event.get('confidence', 0.0),
                        'findings': result_data.get('findings', []),
                        'concerns': result_data.get('concerns', []),
                        'evidence_count': result_data.get('evidence_count', 0),
                        'methodology': result_data.get('methodology', 'unknown'),
                        'created_at': event.get('created_at', '')
                    })

                elif topic == 'research.critique':
                    hypotheses[task_id]['critique_count'] = hypotheses[task_id].get('critique_count', 0) + 1

                    # Store critique details
                    if task_id not in critiques:
                        critiques[task_id] = []

                    notes = event.get('notes', '') or ''
                    # Handle notes being a list or string
                    if isinstance(notes, list):
                        notes = '; '.join(str(n) for n in notes)
                    notes = str(notes)
                    is_external = 'external' in notes.lower()
                    if is_external:
                        hypotheses[task_id]['external_participated'] = True

                    critiques[task_id].append({
                        'critic_id': event.get('critic_id', 'unknown'),
                        'score': event.get('score', 0.0),
                        'flags': event.get('flags', []),
                        'notes': notes,
                        'is_external': is_external,
                        'created_at': event.get('created_at', '')
                    })

                elif topic == 'research.consensus':
                    decision = event.get('decision', '')
                    iteration = event.get('iteration', 1)
                    max_iterations = event.get('max_iterations', 3)

                    hypotheses[task_id]['score'] = event.get('average_score')
                    hypotheses[task_id]['decision'] = decision
                    hypotheses[task_id]['consensus_status'] = event.get('consensus_status')
                    hypotheses[task_id]['score_variance'] = event.get('score_variance')
                    hypotheses[task_id]['iteration'] = iteration
                    hypotheses[task_id]['max_iterations'] = max_iterations
                    hypotheses[task_id]['external_participated'] = event.get('external_swarm_participated', False)

                    # Only mark as COMPLETED for final decisions
                    if decision in ['APPROVED', 'FAILED']:
                        hypotheses[task_id]['status'] = 'COMPLETED'
                        hypotheses[task_id]['completed_at'] = event.get('created_at')
                    elif decision == 'NEEDS_MORE_RESEARCH':
                        # Keep processing - orchestrator will retry
                        hypotheses[task_id]['status'] = f'RETRY {iteration}/{max_iterations}'

        except StopIteration:
            # Consumer timeout - no more messages, continue waiting
            pass
        except Exception as e:
            print(f"Kafka listener error: {e}")
            time.sleep(1)

# Crustafarian Research Precepts - propagated to all swarm agents
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

def create_task(hypothesis, domain="unknown", search_terms=None):
    """Create and publish a research task with Crustafarian Precepts"""
    task_id = str(uuid.uuid4())
    swarm_id = str(uuid.uuid4())

    # Get domain info
    domain_info = RESEARCH_DOMAINS.get(domain, RESEARCH_DOMAINS["unknown"])

    task = {
        "task_id": task_id,
        "swarm_id": swarm_id,
        "task_type": "hypothesis_research",
        "payload": {
            "hypothesis": hypothesis,
            "domain": domain,
            "domain_name": domain_info["name"],
            "sources": domain_info["sources"],
            "search_terms": search_terms or hypothesis[:100],
            "context": "Submitted via Hypothesis Manager",
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

    # Track locally
    hypotheses[task_id] = {
        'task_id': task_id,
        'swarm_id': swarm_id,
        'hypothesis': hypothesis,
        'domain': domain,
        'status': 'SUBMITTED',
        'score': None,
        'decision': None,
        'consensus_status': None,
        'submitted_at': task['created_at'],
        'completed_at': None,
        'result_count': 0,
        'critique_count': 0,
        'external_participated': False,
        'iteration': 1,
        'max_iterations': 3
    }
    results[task_id] = []
    critiques[task_id] = []

    # Publish to Kafka
    p = get_producer()
    p.send('research.task', value=task)
    p.flush()

    return task_id

def find_publications(task_id):
    """Find publication files for a given task_id"""
    if not os.path.exists(PUBLICATIONS_DIR):
        return None

    # Publications can be named with either legacy or new prefixes:
    # - publication_YYYYMMDD_HHMMSS_taskid8.ext
    # - research_document_YYYYMMDD_HHMMSS_taskid8.ext
    short_id = task_id[:8]
    publications = {}

    for ext in ['md', 'html', 'pdf']:
        patterns = [
            os.path.join(PUBLICATIONS_DIR, f"publication_*_{short_id}.{ext}"),
            os.path.join(PUBLICATIONS_DIR, f"research_document_*_{short_id}.{ext}")
        ]
        matches = []
        for pattern in patterns:
            matches.extend(glob.glob(pattern))
        if matches:
            # Get the most recently modified file
            latest = max(matches, key=os.path.getmtime)
            publications[ext] = os.path.basename(latest)

    return publications if publications else None

# ============== HTML TEMPLATES ==============

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Hypothesis Manager</title>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: #1a1a2e;
            color: #eee;
            margin: 0;
            padding: 20px;
        }
        .header {
            background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
            padding: 30px;
            border-radius: 10px;
            margin-bottom: 30px;
            text-align: center;
        }
        h1 { margin: 0; font-size: 2.5em; color: #fff; }
        .subtitle { opacity: 0.9; margin-top: 10px; }

        .submit-form {
            background: #16213e;
            padding: 25px;
            border-radius: 10px;
            margin-bottom: 30px;
        }
        .submit-form h2 { margin-top: 0; color: #38ef7d; }
        textarea {
            width: 100%;
            height: 120px;
            padding: 15px;
            border: none;
            border-radius: 8px;
            background: #0f3460;
            color: #fff;
            font-size: 1em;
            resize: vertical;
            box-sizing: border-box;
        }
        textarea:focus {
            outline: 2px solid #38ef7d;
        }
        button {
            background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
            color: #fff;
            border: none;
            padding: 12px 30px;
            border-radius: 5px;
            cursor: pointer;
            font-size: 1em;
            font-weight: bold;
            margin-top: 15px;
            transition: transform 0.2s, box-shadow 0.2s;
        }
        button:hover {
            transform: translateY(-2px);
            box-shadow: 0 5px 20px rgba(56, 239, 125, 0.3);
        }
        .btn-rerun {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 6px 15px;
            font-size: 0.85em;
        }
        .btn-pub {
            background: linear-gradient(135deg, #e74c3c 0%, #c0392b 100%);
            padding: 4px 10px;
            font-size: 0.75em;
            margin: 2px;
            text-decoration: none;
            display: inline-block;
        }
        .btn-pub:hover {
            transform: translateY(-1px);
        }

        .hypotheses-table {
            background: #16213e;
            border-radius: 10px;
            padding: 20px;
            overflow-x: auto;
        }
        table {
            width: 100%;
            border-collapse: collapse;
        }
        th, td {
            padding: 12px 15px;
            text-align: left;
            border-bottom: 1px solid #0f3460;
        }
        th {
            background: #0f3460;
            color: #38ef7d;
            font-weight: 600;
        }
        tr:hover {
            background: #0f3460;
        }

        .status {
            display: inline-block;
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 0.85em;
            font-weight: bold;
        }
        .status-submitted { background: #3498db; }
        .status-processing { background: #f39c12; color: #000; }
        .status-completed { background: #27ae60; }

        .decision {
            font-weight: bold;
        }
        .decision-approved { color: #2ecc71; }
        .decision-needs { color: #e74c3c; }

        .score {
            font-size: 1.1em;
            font-weight: bold;
        }
        .score-high { color: #2ecc71; }
        .score-medium { color: #f39c12; }
        .score-low { color: #e74c3c; }

        .hypothesis-text {
            max-width: 400px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }
        .hypothesis-text a {
            color: #fff;
            text-decoration: none;
        }
        .hypothesis-text a:hover {
            color: #38ef7d;
            text-decoration: underline;
        }

        .refresh-notice {
            text-align: center;
            padding: 10px;
            background: #0f3460;
            border-radius: 5px;
            margin-bottom: 20px;
        }

        .empty-state {
            text-align: center;
            padding: 40px;
            color: #888;
        }

        .links {
            margin-top: 30px;
            text-align: center;
            opacity: 0.6;
        }
        .links a { color: #38ef7d; margin: 0 15px; }

        .flash {
            background: #27ae60;
            color: #fff;
            padding: 15px;
            border-radius: 5px;
            margin-bottom: 20px;
            text-align: center;
        }
        .btn-refresh {
            background: linear-gradient(135deg, #3498db 0%, #2980b9 100%);
            padding: 8px 20px;
            font-size: 0.9em;
            margin-left: 15px;
        }

        /* Loading overlay styles */
        .loading-overlay {
            display: none;
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(26, 26, 46, 0.95);
            z-index: 1000;
            justify-content: center;
            align-items: center;
            flex-direction: column;
        }
        .loading-overlay.active {
            display: flex;
        }
        .loading-spinner {
            width: 60px;
            height: 60px;
            border: 4px solid #0f3460;
            border-top: 4px solid #38ef7d;
            border-radius: 50%;
            animation: spin 1s linear infinite;
            margin-bottom: 20px;
        }
        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
        .loading-text {
            color: #38ef7d;
            font-size: 1.3em;
            font-weight: bold;
            margin-bottom: 10px;
        }
        .loading-subtext {
            color: #888;
            font-size: 0.95em;
        }
        .loading-steps {
            margin-top: 20px;
            text-align: left;
        }
        .loading-step {
            padding: 8px 0;
            color: #666;
            transition: color 0.3s;
        }
        .loading-step.active {
            color: #38ef7d;
        }
        .loading-step.done {
            color: #27ae60;
        }
        .loading-step::before {
            content: '○ ';
            margin-right: 8px;
        }
        .loading-step.active::before {
            content: '◉ ';
            animation: pulse 1s infinite;
        }
        .loading-step.done::before {
            content: '✓ ';
        }
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }

        textarea:disabled, button:disabled {
            opacity: 0.5;
            cursor: not-allowed;
        }

        /* Precepts Management Styles */
        .precepts-section {
            background: #16213e;
            padding: 25px;
            border-radius: 10px;
            margin-bottom: 30px;
        }
        .precepts-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            cursor: pointer;
            user-select: none;
        }
        .precepts-header h2 {
            margin: 0;
            color: #38ef7d;
        }
        .precepts-toggle {
            font-size: 1.5em;
            color: #38ef7d;
            transition: transform 0.3s;
        }
        .precepts-toggle.open {
            transform: rotate(180deg);
        }
        .precepts-content {
            display: none;
            margin-top: 20px;
        }
        .precepts-content.open {
            display: block;
        }
        .precept-item {
            background: #0f3460;
            padding: 15px;
            border-radius: 8px;
            margin-bottom: 15px;
        }
        .precept-name {
            font-weight: bold;
            color: #38ef7d;
            margin-bottom: 5px;
        }
        .precept-description {
            font-size: 0.9em;
            color: #aaa;
            margin-bottom: 10px;
        }
        .precept-weight-row {
            display: flex;
            align-items: center;
            gap: 15px;
        }
        .precept-slider {
            flex: 1;
            -webkit-appearance: none;
            height: 8px;
            border-radius: 4px;
            background: #1a1a2e;
            outline: none;
        }
        .precept-slider::-webkit-slider-thumb {
            -webkit-appearance: none;
            width: 20px;
            height: 20px;
            border-radius: 50%;
            background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
            cursor: pointer;
            box-shadow: 0 2px 6px rgba(0,0,0,0.3);
        }
        .precept-slider::-moz-range-thumb {
            width: 20px;
            height: 20px;
            border-radius: 50%;
            background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
            cursor: pointer;
            border: none;
        }
        .precept-weight-value {
            min-width: 60px;
            text-align: right;
            font-weight: bold;
            color: #38ef7d;
        }
        .precepts-total {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 15px;
            background: #0f3460;
            border-radius: 8px;
            margin-top: 15px;
        }
        .precepts-total-label {
            font-weight: bold;
        }
        .precepts-total-value {
            font-size: 1.2em;
            font-weight: bold;
        }
        .precepts-total-value.valid {
            color: #2ecc71;
        }
        .precepts-total-value.invalid {
            color: #e74c3c;
        }
        .precepts-actions {
            margin-top: 15px;
            display: flex;
            gap: 10px;
        }
        .btn-save-precepts {
            background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
        }
        .btn-reset-precepts {
            background: linear-gradient(135deg, #e74c3c 0%, #c0392b 100%);
        }
        .precepts-message {
            margin-top: 10px;
            padding: 10px;
            border-radius: 5px;
            display: none;
        }
        .precepts-message.success {
            display: block;
            background: #27ae60;
            color: #fff;
        }
        .precepts-message.error {
            display: block;
            background: #e74c3c;
            color: #fff;
        }

        /* Add/Remove Precept Styles */
        .precept-header-row {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            margin-bottom: 10px;
        }
        .precept-info {
            flex: 1;
        }
        .btn-delete-precept {
            background: linear-gradient(135deg, #e74c3c 0%, #c0392b 100%);
            padding: 5px 10px;
            font-size: 0.8em;
            margin-left: 10px;
            flex-shrink: 0;
        }
        .btn-delete-precept:hover {
            transform: translateY(-1px);
        }
        .add-precept-form {
            background: #0f3460;
            padding: 20px;
            border-radius: 8px;
            margin-top: 15px;
            border: 2px dashed #38ef7d;
        }
        .add-precept-form h3 {
            margin-top: 0;
            color: #38ef7d;
        }
        .form-row {
            margin-bottom: 15px;
        }
        .form-row label {
            display: block;
            margin-bottom: 5px;
            color: #aaa;
            font-size: 0.9em;
        }
        .form-row input[type="text"] {
            width: 100%;
            padding: 10px;
            border: none;
            border-radius: 5px;
            background: #1a1a2e;
            color: #fff;
            font-size: 1em;
            box-sizing: border-box;
        }
        .form-row input[type="text"]:focus {
            outline: 2px solid #38ef7d;
        }
        .btn-add-precept {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        }
        .btn-cancel-add {
            background: #555;
            margin-left: 10px;
        }
        .add-precept-toggle {
            margin-top: 15px;
        }
        .precept-name-input, .precept-desc-input {
            background: transparent;
            border: none;
            color: inherit;
            font: inherit;
            width: 100%;
            padding: 2px;
        }
        .precept-name-input:focus, .precept-desc-input:focus {
            outline: 1px solid #38ef7d;
            background: #1a1a2e;
            border-radius: 3px;
        }
        .precept-name-input {
            font-weight: bold;
            color: #38ef7d;
        }
        .precept-desc-input {
            font-size: 0.9em;
            color: #aaa;
        }
    </style>
</head>
<body>
    <!-- Loading Overlay -->
    <div id="loadingOverlay" class="loading-overlay">
        <div class="loading-spinner"></div>
        <div class="loading-text">Submitting Hypothesis...</div>
        <div class="loading-subtext">This may take a few seconds</div>
        <div class="loading-steps">
            <div id="step1" class="loading-step active">Detecting research domain...</div>
            <div id="step2" class="loading-step">Extracting search terms...</div>
            <div id="step3" class="loading-step">Publishing to research swarm...</div>
        </div>
    </div>

    <div class="header">
        <h1>Hypothesis Manager</h1>
        <div class="subtitle">Submit and track research hypotheses</div>
    </div>

    {% if message %}
    <div class="flash">{{ message }}</div>
    {% endif %}

    <div class="refresh-notice">
        Last update: {{ now }}
        <button class="btn-refresh" onclick="location.reload()">Refresh</button>
    </div>

    <div class="submit-form">
        <h2>Submit New Hypothesis</h2>
        <form action="/submit" method="POST">
            <textarea name="hypothesis" placeholder="Enter your research hypothesis here...

Example: Galaxy rotation curve anomalies may be explainable without non-baryonic dark matter through under-modeled gravitational effects from supermassive black holes and spacetime geometry"></textarea>
            <button type="submit">Submit for Research</button>
        </form>
    </div>

    <!-- Precepts Management Section -->
    <div class="precepts-section">
        <div class="precepts-header" onclick="togglePrecepts()">
            <h2>Research Precepts Configuration</h2>
            <span id="preceptsToggle" class="precepts-toggle">▼</span>
        </div>
        <div id="preceptsContent" class="precepts-content">
            <p style="color: #888; margin-bottom: 15px;">
                Adjust weights, edit names/descriptions, or add/remove precepts.
                Weights auto-balance to sum to 1.0. Changes apply after saving and restarting the critic.
            </p>
            <div id="preceptsList">
                <!-- Precepts will be loaded dynamically -->
                <div style="color: #888; text-align: center; padding: 20px;">Loading precepts...</div>
            </div>

            <!-- Add New Precept Form (hidden by default) -->
            <div id="addPreceptForm" class="add-precept-form" style="display: none;">
                <h3>Add New Precept</h3>
                <div class="form-row">
                    <label>Key (unique identifier, lowercase, underscores):</label>
                    <input type="text" id="newPreceptKey" placeholder="e.g., ethical_review">
                </div>
                <div class="form-row">
                    <label>Name (display name):</label>
                    <input type="text" id="newPreceptName" placeholder="e.g., Ethical Review Required">
                </div>
                <div class="form-row">
                    <label>Description (used for semantic matching):</label>
                    <input type="text" id="newPreceptDesc" placeholder="e.g., Research must consider ethical implications">
                </div>
                <div>
                    <button class="btn-add-precept" onclick="confirmAddPrecept()">Add Precept</button>
                    <button class="btn-cancel-add" onclick="hideAddForm()">Cancel</button>
                </div>
            </div>

            <div class="add-precept-toggle">
                <button id="showAddFormBtn" class="btn-add-precept" onclick="showAddForm()">+ Add New Precept</button>
            </div>

            <div class="precepts-total">
                <span class="precepts-total-label">Total Weight:</span>
                <span id="totalWeight" class="precepts-total-value valid">1.00</span>
            </div>
            <div class="precepts-actions">
                <button class="btn-save-precepts" onclick="savePrecepts()">Save Precepts</button>
                <button class="btn-reset-precepts" onclick="loadPrecepts()">Reset Changes</button>
            </div>
            <div id="preceptsMessage" class="precepts-message"></div>
        </div>
    </div>

    <div class="hypotheses-table">
        <h2 style="margin-top: 0; color: #38ef7d;">Hypothesis History ({{ hypotheses|length }})</h2>
        {% if hypotheses %}
        <table>
            <thead>
                <tr>
                    <th>Hypothesis</th>
                    <th>Status</th>
                    <th>Score</th>
                    <th>Decision</th>
                    <th>Publications</th>
                    <th>Submitted</th>
                    <th>Actions</th>
                </tr>
            </thead>
            <tbody>
                {% for h in hypotheses %}
                <tr>
                    <td class="hypothesis-text" title="{{ h.hypothesis }}">
                        <a href="/detail/{{ h.task_id }}">{{ h.hypothesis[:80] }}{% if h.hypothesis|length > 80 %}...{% endif %}</a>
                    </td>
                    <td>
                        <span class="status status-{{ h.status|lower }}">{{ h.status }}</span>
                    </td>
                    <td>
                        {% if h.score is not none %}
                        <span class="score {% if h.score|float >= 0.6 %}score-high{% elif h.score|float >= 0.4 %}score-medium{% else %}score-low{% endif %}">
                            {{ "%.2f"|format(h.score|float) }}
                        </span>
                        {% else %}
                        <span style="color: #888;">-</span>
                        {% endif %}
                    </td>
                    <td>
                        {% if h.decision %}
                        <span class="decision {% if h.decision == 'APPROVED' %}decision-approved{% else %}decision-needs{% endif %}">
                            {{ h.decision }}
                        </span>
                        {% else %}
                        <span style="color: #888;">Pending</span>
                        {% endif %}
                    </td>
                    <td>
                        {% if h.publications %}
                            {% if h.publications.pdf %}
                            <a href="/publications/{{ h.publications.pdf }}" class="btn-pub" target="_blank">PDF</a>
                            {% endif %}
                            {% if h.publications.html %}
                            <a href="/publications/{{ h.publications.html }}" class="btn-pub" target="_blank">HTML</a>
                            {% endif %}
                            {% if h.publications.md %}
                            <a href="/publications/{{ h.publications.md }}" class="btn-pub" target="_blank">MD</a>
                            {% endif %}
                        {% else %}
                            <span style="color: #888;">-</span>
                        {% endif %}
                    </td>
                    <td style="font-size: 0.9em; color: #888;">{{ h.submitted_at[:16] if h.submitted_at else '-' }}</td>
                    <td>
                        <form action="/rerun/{{ h.task_id }}" method="POST" style="display: inline;">
                            <button type="submit" class="btn-rerun">Re-run</button>
                        </form>
                    </td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
        {% else %}
        <div class="empty-state">
            <p>No hypotheses yet. Submit one above to get started!</p>
        </div>
        {% endif %}
    </div>

    <div class="links">
        <a href="http://localhost:5004">Swarm Dashboard</a> |
        <a href="http://localhost:8080">Kafka UI</a>
    </div>

    <script>
        document.addEventListener('DOMContentLoaded', function() {
            const form = document.querySelector('.submit-form form');
            const textarea = form.querySelector('textarea');
            const submitBtn = form.querySelector('button[type="submit"]');
            const overlay = document.getElementById('loadingOverlay');
            const steps = ['step1', 'step2', 'step3'];

            form.addEventListener('submit', function(e) {
                // Don't prevent default - let the form submit normally
                // But show loading state first

                if (!textarea.value.trim()) {
                    return; // Let browser validation handle empty input
                }

                // Disable inputs (use readOnly for textarea so value still submits)
                textarea.readOnly = true;
                submitBtn.disabled = true;
                submitBtn.textContent = 'Submitting...';

                // Show loading overlay
                overlay.classList.add('active');

                // Animate through steps (simulated timing)
                let currentStep = 0;

                function advanceStep() {
                    if (currentStep > 0) {
                        document.getElementById(steps[currentStep - 1]).classList.remove('active');
                        document.getElementById(steps[currentStep - 1]).classList.add('done');
                    }
                    if (currentStep < steps.length) {
                        document.getElementById(steps[currentStep]).classList.add('active');
                        currentStep++;
                        // Step timing: domain detection ~2s, search terms ~1.5s, publish ~0.5s
                        const delays = [2000, 1500, 500];
                        if (currentStep < steps.length) {
                            setTimeout(advanceStep, delays[currentStep - 1] || 1500);
                        }
                    }
                }

                advanceStep();

                // Form will submit and redirect naturally
            });

            // Load precepts on page load
            loadPrecepts();
        });

        // Precepts data storage
        let preceptsData = {};
        let originalPreceptsData = {};

        function togglePrecepts() {
            const content = document.getElementById('preceptsContent');
            const toggle = document.getElementById('preceptsToggle');
            content.classList.toggle('open');
            toggle.classList.toggle('open');
        }

        function loadPrecepts() {
            fetch('/precepts')
                .then(response => response.json())
                .then(data => {
                    if (data.error) {
                        document.getElementById('preceptsList').innerHTML =
                            '<div style="color: #e74c3c; text-align: center; padding: 20px;">Error: ' + data.error + '</div>';
                        return;
                    }
                    preceptsData = JSON.parse(JSON.stringify(data));
                    originalPreceptsData = JSON.parse(JSON.stringify(data));
                    renderPrecepts();
                    hideMessage();
                })
                .catch(err => {
                    document.getElementById('preceptsList').innerHTML =
                        '<div style="color: #e74c3c; text-align: center; padding: 20px;">Failed to load precepts</div>';
                });
        }

        function renderPrecepts() {
            const container = document.getElementById('preceptsList');
            let html = '';

            for (const [key, precept] of Object.entries(preceptsData)) {
                const weightPercent = Math.round(precept.weight * 100);
                html += `
                    <div class="precept-item" data-key="${key}">
                        <div class="precept-header-row">
                            <div class="precept-info">
                                <input type="text" class="precept-name-input" value="${precept.name}"
                                       data-key="${key}" data-field="name" onchange="onFieldChange(this)">
                                <input type="text" class="precept-desc-input" value="${precept.description}"
                                       data-key="${key}" data-field="description" onchange="onFieldChange(this)">
                            </div>
                            <button class="btn-delete-precept" onclick="deletePrecept('${key}')">Delete</button>
                        </div>
                        <div class="precept-weight-row">
                            <input type="range" class="precept-slider" min="0" max="100" value="${weightPercent}"
                                   data-key="${key}" oninput="onSliderChange(this)">
                            <span class="precept-weight-value" id="weight-${key}">${precept.weight.toFixed(2)}</span>
                        </div>
                    </div>
                `;
            }

            container.innerHTML = html;
            updateTotalDisplay();
        }

        function onFieldChange(input) {
            const key = input.dataset.key;
            const field = input.dataset.field;
            preceptsData[key][field] = input.value;
        }

        function deletePrecept(key) {
            const precept = preceptsData[key];
            if (!confirm(`Delete precept "${precept.name}"?\\n\\nIts weight will be redistributed to other precepts.`)) {
                return;
            }

            const weightToRedistribute = preceptsData[key].weight;
            delete preceptsData[key];

            // Redistribute weight proportionally
            const remainingKeys = Object.keys(preceptsData);
            if (remainingKeys.length > 0) {
                const currentTotal = remainingKeys.reduce((sum, k) => sum + preceptsData[k].weight, 0);
                if (currentTotal > 0) {
                    const scaleFactor = 1 / currentTotal;
                    for (const k of remainingKeys) {
                        preceptsData[k].weight = preceptsData[k].weight * scaleFactor;
                    }
                } else {
                    // All weights were 0, distribute equally
                    const equalWeight = 1 / remainingKeys.length;
                    for (const k of remainingKeys) {
                        preceptsData[k].weight = equalWeight;
                    }
                }
            }

            renderPrecepts();
            showMessage('Precept deleted. Remember to save changes.', 'success');
        }

        function showAddForm() {
            document.getElementById('addPreceptForm').style.display = 'block';
            document.getElementById('showAddFormBtn').style.display = 'none';
            document.getElementById('newPreceptKey').value = '';
            document.getElementById('newPreceptName').value = '';
            document.getElementById('newPreceptDesc').value = '';
            document.getElementById('newPreceptKey').focus();
        }

        function hideAddForm() {
            document.getElementById('addPreceptForm').style.display = 'none';
            document.getElementById('showAddFormBtn').style.display = 'inline-block';
        }

        function confirmAddPrecept() {
            const key = document.getElementById('newPreceptKey').value.trim().toLowerCase().replace(/[^a-z0-9_]/g, '_');
            const name = document.getElementById('newPreceptName').value.trim();
            const desc = document.getElementById('newPreceptDesc').value.trim();

            if (!key || !name || !desc) {
                showMessage('Please fill in all fields', 'error');
                return;
            }

            if (preceptsData[key]) {
                showMessage('A precept with this key already exists', 'error');
                return;
            }

            // Calculate new weight (take equal share from existing)
            const numPrecepts = Object.keys(preceptsData).length;
            const newWeight = 1 / (numPrecepts + 1);

            // Scale down existing weights
            const scaleFactor = numPrecepts / (numPrecepts + 1);
            for (const k of Object.keys(preceptsData)) {
                preceptsData[k].weight = preceptsData[k].weight * scaleFactor;
            }

            // Add new precept
            preceptsData[key] = {
                name: name,
                description: desc,
                weight: newWeight
            };

            normalizeWeights();
            renderPrecepts();
            hideAddForm();
            showMessage(`Added precept "${name}". Remember to save changes.`, 'success');
        }

        function onSliderChange(slider) {
            const key = slider.dataset.key;
            const newValue = parseInt(slider.value) / 100;
            const oldValue = preceptsData[key].weight;
            const delta = newValue - oldValue;

            // Update the changed slider's precept
            preceptsData[key].weight = newValue;

            // Get other precepts to adjust
            const otherKeys = Object.keys(preceptsData).filter(k => k !== key);
            const otherTotal = otherKeys.reduce((sum, k) => sum + preceptsData[k].weight, 0);

            if (otherTotal > 0 && delta !== 0) {
                // Proportionally adjust other weights
                const scaleFactor = (otherTotal - delta) / otherTotal;

                for (const otherKey of otherKeys) {
                    preceptsData[otherKey].weight = Math.max(0, preceptsData[otherKey].weight * scaleFactor);
                }
            }

            // Normalize to ensure sum is exactly 1.0
            normalizeWeights();

            // Update all displays
            updateAllDisplays();
        }

        function normalizeWeights() {
            const total = Object.values(preceptsData).reduce((sum, p) => sum + p.weight, 0);
            if (total > 0 && Math.abs(total - 1.0) > 0.001) {
                for (const key of Object.keys(preceptsData)) {
                    preceptsData[key].weight = preceptsData[key].weight / total;
                }
            }
        }

        function updateAllDisplays() {
            for (const [key, precept] of Object.entries(preceptsData)) {
                const slider = document.querySelector(`input.precept-slider[data-key="${key}"]`);
                const valueDisplay = document.getElementById(`weight-${key}`);
                if (slider && valueDisplay) {
                    slider.value = Math.round(precept.weight * 100);
                    valueDisplay.textContent = precept.weight.toFixed(2);
                }
            }
            updateTotalDisplay();
        }

        function updateTotalDisplay() {
            const total = Object.values(preceptsData).reduce((sum, p) => sum + p.weight, 0);
            const totalEl = document.getElementById('totalWeight');
            totalEl.textContent = total.toFixed(2);
            totalEl.className = 'precepts-total-value ' + (Math.abs(total - 1.0) < 0.01 ? 'valid' : 'invalid');
        }

        function savePrecepts() {
            // Round weights to 2 decimal places for clean JSON
            const saveData = {};
            for (const [key, precept] of Object.entries(preceptsData)) {
                saveData[key] = {
                    name: precept.name,
                    description: precept.description,
                    weight: Math.round(precept.weight * 100) / 100
                };
            }

            fetch('/precepts', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(saveData)
            })
            .then(response => response.json())
            .then(data => {
                if (data.error) {
                    showMessage(data.error, 'error');
                } else {
                    showMessage('Precepts saved! Restart critic to apply: docker-compose restart research-critic', 'success');
                    originalPreceptsData = JSON.parse(JSON.stringify(preceptsData));
                }
            })
            .catch(err => {
                showMessage('Failed to save precepts: ' + err, 'error');
            });
        }

        function showMessage(text, type) {
            const el = document.getElementById('preceptsMessage');
            el.textContent = text;
            el.className = 'precepts-message ' + type;
        }

        function hideMessage() {
            const el = document.getElementById('preceptsMessage');
            el.className = 'precepts-message';
        }
    </script>
</body>
</html>
"""

DETAIL_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Hypothesis Detail</title>
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
        }
        h1 { margin: 0; font-size: 1.8em; color: #fff; }
        .back-link { color: #fff; text-decoration: none; opacity: 0.8; }
        .back-link:hover { opacity: 1; }

        .section {
            background: #16213e;
            border-radius: 10px;
            padding: 20px;
            margin-bottom: 20px;
        }
        .section h2 {
            margin-top: 0;
            color: #667eea;
            border-bottom: 2px solid #0f3460;
            padding-bottom: 10px;
        }

        .hypothesis-full {
            background: #0f3460;
            padding: 20px;
            border-radius: 8px;
            line-height: 1.6;
            font-size: 1.1em;
        }

        .meta-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin-top: 15px;
        }
        .meta-item {
            background: #0f3460;
            padding: 15px;
            border-radius: 8px;
        }
        .meta-item label {
            color: #888;
            font-size: 0.85em;
            display: block;
            margin-bottom: 5px;
        }
        .meta-item .value {
            font-size: 1.2em;
            font-weight: bold;
        }

        .status-submitted { color: #3498db; }
        .status-processing { color: #f39c12; }
        .status-completed { color: #27ae60; }

        .decision-approved { color: #2ecc71; }
        .decision-needs { color: #e74c3c; }

        .event-list {
            max-height: 400px;
            overflow-y: auto;
        }
        .event {
            background: #0f3460;
            padding: 15px;
            border-radius: 8px;
            margin-bottom: 10px;
            border-left: 4px solid #667eea;
        }
        .event.external {
            border-left-color: #f39c12;
        }
        .event-header {
            display: flex;
            justify-content: space-between;
            margin-bottom: 10px;
        }
        .event-agent {
            font-weight: bold;
            color: #667eea;
        }
        .event-time {
            color: #888;
            font-size: 0.85em;
        }
        .event-score {
            font-size: 1.2em;
            font-weight: bold;
        }
        .event-flags {
            margin-top: 8px;
        }
        .flag {
            display: inline-block;
            background: #e74c3c;
            color: #fff;
            padding: 2px 8px;
            border-radius: 10px;
            font-size: 0.75em;
            margin-right: 5px;
        }
        .event-notes {
            margin-top: 10px;
            padding: 10px;
            background: #1a1a2e;
            border-radius: 5px;
            font-size: 0.9em;
            color: #ccc;
        }

        .papers-list {
            margin-top: 10px;
        }
        .paper {
            background: #1a1a2e;
            padding: 8px 12px;
            border-radius: 5px;
            margin-bottom: 5px;
            font-size: 0.9em;
        }

        .pub-links {
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
        }
        .pub-link {
            background: linear-gradient(135deg, #e74c3c 0%, #c0392b 100%);
            color: #fff;
            padding: 10px 20px;
            border-radius: 5px;
            text-decoration: none;
            font-weight: bold;
        }
        .pub-link:hover {
            transform: translateY(-2px);
        }

        .empty-state {
            color: #888;
            text-align: center;
            padding: 20px;
        }

        .refresh-notice {
            text-align: center;
            padding: 10px;
            background: #0f3460;
            border-radius: 5px;
            margin-bottom: 20px;
        }
        .btn-refresh {
            background: linear-gradient(135deg, #3498db 0%, #2980b9 100%);
            color: #fff;
            border: none;
            padding: 8px 20px;
            border-radius: 5px;
            cursor: pointer;
            font-size: 0.9em;
            margin-left: 15px;
        }
    </style>
</head>
<body>
    <div class="header">
        <a href="/" class="back-link">&larr; Back to Hypothesis Manager</a>
        <h1 style="margin-top: 15px;">Hypothesis Detail</h1>
    </div>

    <div class="refresh-notice">
        Last update: {{ now }}
        <button class="btn-refresh" onclick="location.reload()">Refresh</button>
    </div>

    <!-- Hypothesis Section -->
    <div class="section">
        <h2>Hypothesis</h2>
        <div class="hypothesis-full">{{ h.hypothesis }}</div>

        <div class="meta-grid">
            <div class="meta-item">
                <label>Task ID</label>
                <div class="value" style="font-size: 0.9em;">{{ h.task_id[:8] }}...</div>
            </div>
            <div class="meta-item">
                <label>Swarm ID</label>
                <div class="value" style="font-size: 0.9em;">{{ h.swarm_id[:8] if h.swarm_id else '-' }}...</div>
            </div>
            <div class="meta-item">
                <label>Status</label>
                <div class="value status-{{ h.status|lower }}">{{ h.status }}</div>
            </div>
            <div class="meta-item">
                <label>Submitted</label>
                <div class="value" style="font-size: 0.9em;">{{ h.submitted_at[:19] if h.submitted_at else '-' }}</div>
            </div>
        </div>
    </div>

    <!-- Consensus Section -->
    <div class="section">
        <h2>Consensus</h2>
        {% if h.decision %}
        <div class="meta-grid">
            <div class="meta-item">
                <label>Decision</label>
                <div class="value {% if h.decision == 'APPROVED' %}decision-approved{% else %}decision-needs{% endif %}">
                    {{ h.decision }}
                </div>
            </div>
            <div class="meta-item">
                <label>Score</label>
                <div class="value">{{ "%.2f"|format(h.score|float) if h.score else '-' }}</div>
            </div>
            <div class="meta-item">
                <label>Consensus Status</label>
                <div class="value" style="font-size: 0.9em;">{{ h.consensus_status or '-' }}</div>
            </div>
            <div class="meta-item">
                <label>Score Variance</label>
                <div class="value" style="font-size: 0.9em;">{{ "%.3f"|format(h.score_variance|float) if h.score_variance else '-' }}</div>
            </div>
            <div class="meta-item">
                <label>External Swarm</label>
                <div class="value">{{ 'Participated' if h.external_participated else 'No' }}</div>
            </div>
            <div class="meta-item">
                <label>Completed</label>
                <div class="value" style="font-size: 0.9em;">{{ h.completed_at[:19] if h.completed_at else '-' }}</div>
            </div>
        </div>
        {% else %}
        <div class="empty-state">Consensus not yet reached. Waiting for critiques...</div>
        {% endif %}
    </div>

    <!-- Research Results Section -->
    <div class="section">
        <h2>Research Results ({{ results|length }})</h2>
        {% if results %}
        <div class="event-list">
            {% for r in results %}
            <div class="event {% if r.agent_id and 'external' in r.agent_id|lower %}external{% endif %}">
                <div class="event-header">
                    <span class="event-agent">{{ (r.agent_id or 'unknown')[:40] }}{% if r.agent_id and r.agent_id|length > 40 %}...{% endif %}</span>
                    <span class="event-time">{{ r.created_at[:19] if r.created_at else '' }}</span>
                </div>
                <div style="margin-bottom: 8px;">
                    <strong>Confidence:</strong>
                    <span class="event-score" style="color: {% if (r.confidence or 0)|float >= 0.6 %}#2ecc71{% elif (r.confidence or 0)|float >= 0.4 %}#f39c12{% else %}#e74c3c{% endif %}">
                        {{ "%.2f"|format((r.confidence or 0)|float) }}
                    </span>
                    <span style="color: #888; margin-left: 15px;">
                        <strong>Evidence:</strong> {{ r.evidence_count or 0 }} papers |
                        <strong>Method:</strong> {{ (r.methodology or 'unknown')[:30] }}{% if r.methodology and r.methodology|length > 30 %}...{% endif %}
                    </span>
                </div>
                {% if r.findings %}
                <div class="event-notes">
                    <strong style="color: #2ecc71;">Findings:</strong>
                    <ul style="margin: 5px 0 0 0; padding-left: 20px;">
                        {% for finding in r.findings[:5] %}
                        <li style="margin-bottom: 4px;">{{ finding }}</li>
                        {% endfor %}
                        {% if r.findings|length > 5 %}
                        <li style="color: #888;">... and {{ r.findings|length - 5 }} more findings</li>
                        {% endif %}
                    </ul>
                </div>
                {% endif %}
                {% if r.concerns %}
                <div class="event-notes" style="border-left: 3px solid #e74c3c; margin-top: 8px;">
                    <strong style="color: #e74c3c;">Concerns:</strong>
                    <ul style="margin: 5px 0 0 0; padding-left: 20px;">
                        {% for concern in r.concerns[:3] %}
                        <li style="margin-bottom: 4px;">{{ concern }}</li>
                        {% endfor %}
                        {% if r.concerns|length > 3 %}
                        <li style="color: #888;">... and {{ r.concerns|length - 3 }} more concerns</li>
                        {% endif %}
                    </ul>
                </div>
                {% endif %}
            </div>
            {% endfor %}
        </div>
        {% else %}
        <div class="empty-state">No research results yet...</div>
        {% endif %}
    </div>

    <!-- Critiques Section -->
    <div class="section">
        <h2>Peer Review Critiques ({{ critiques|length }})</h2>
        {% if critiques %}
        <div class="event-list">
            {% for c in critiques %}
            <div class="event {% if c.is_external %}external{% endif %}">
                <div class="event-header">
                    <span class="event-agent">
                        {{ c.critic_id[:30] }}{% if c.critic_id|length > 30 %}...{% endif %}
                        {% if c.is_external %}<span style="color: #f39c12;">(External)</span>{% endif %}
                    </span>
                    <span class="event-time">{{ c.created_at[:19] if c.created_at else '' }}</span>
                </div>
                <div>
                    <strong>Score:</strong>
                    <span class="event-score" style="color: {% if c.score|float >= 0.6 %}#2ecc71{% elif c.score|float >= 0.4 %}#f39c12{% else %}#e74c3c{% endif %}">
                        {{ "%.2f"|format(c.score|float) }}
                    </span>
                </div>
                {% if c.flags %}
                <div class="event-flags">
                    {% for flag in c.flags %}
                    <span class="flag">{{ flag }}</span>
                    {% endfor %}
                </div>
                {% endif %}
                {% if c.notes %}
                <div class="event-notes">{{ c.notes }}</div>
                {% endif %}
            </div>
            {% endfor %}
        </div>
        {% else %}
        <div class="empty-state">No critiques yet...</div>
        {% endif %}
    </div>

    <!-- Publications Section -->
    <div class="section">
        <h2>Publications</h2>
        {% if publications %}
        <div class="pub-links">
            {% if publications.pdf %}
            <a href="/publications/{{ publications.pdf }}" class="pub-link" target="_blank">Download PDF</a>
            {% endif %}
            {% if publications.html %}
            <a href="/publications/{{ publications.html }}" class="pub-link" target="_blank">View HTML</a>
            {% endif %}
            {% if publications.md %}
            <a href="/publications/{{ publications.md }}" class="pub-link" target="_blank">View Markdown</a>
            {% endif %}
        </div>
        {% else %}
        <div class="empty-state">
            {% if h.decision %}
            Publication is being generated...
            {% else %}
            Publications will be available after consensus is reached.
            {% endif %}
        </div>
        {% endif %}
    </div>

    <!-- Actions -->
    <div class="section">
        <h2>Actions</h2>
        <form action="/rerun/{{ h.task_id }}" method="POST" style="display: inline;">
            <button type="submit" style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);">
                Re-run This Hypothesis
            </button>
        </form>
        <form action="/delete/{{ h.task_id }}" method="POST" style="display: inline; margin-left: 15px;"
              onsubmit="return confirm('Are you sure you want to delete this hypothesis and all its data? This cannot be undone.');">
            <button type="submit" style="background: linear-gradient(135deg, #e74c3c 0%, #c0392b 100%);">
                Delete Hypothesis
            </button>
        </form>
    </div>
</body>
</html>
"""

CLARIFY_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Whachoo Talkin' Bout? - Domain Clarification</title>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: #1a1a2e;
            color: #eee;
            margin: 0;
            padding: 20px;
        }
        .header {
            background: linear-gradient(135deg, #e74c3c 0%, #c0392b 100%);
            padding: 30px;
            border-radius: 10px;
            margin-bottom: 30px;
            text-align: center;
        }
        h1 { margin: 0; font-size: 2.2em; color: #fff; }
        .subtitle { opacity: 0.9; margin-top: 10px; font-size: 1.1em; }

        .section {
            background: #16213e;
            border-radius: 10px;
            padding: 25px;
            margin-bottom: 20px;
        }
        .section h2 {
            margin-top: 0;
            color: #e74c3c;
            border-bottom: 2px solid #0f3460;
            padding-bottom: 10px;
        }

        .hypothesis-box {
            background: #0f3460;
            padding: 20px;
            border-radius: 8px;
            line-height: 1.6;
            font-size: 1.1em;
            margin-bottom: 20px;
            border-left: 4px solid #e74c3c;
        }

        .domain-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 15px;
        }
        .domain-option {
            background: #0f3460;
            padding: 20px;
            border-radius: 8px;
            cursor: pointer;
            transition: all 0.2s;
            border: 2px solid transparent;
        }
        .domain-option:hover {
            border-color: #38ef7d;
            transform: translateY(-3px);
        }
        .domain-option.selected {
            border-color: #38ef7d;
            background: #1a3a5c;
        }
        .domain-option input {
            display: none;
        }
        .domain-name {
            font-weight: bold;
            font-size: 1.1em;
            color: #38ef7d;
            margin-bottom: 8px;
        }
        .domain-desc {
            color: #aaa;
            font-size: 0.9em;
        }
        .domain-sources {
            margin-top: 10px;
            font-size: 0.8em;
            color: #667eea;
        }

        button {
            background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
            color: #fff;
            border: none;
            padding: 15px 40px;
            border-radius: 5px;
            cursor: pointer;
            font-size: 1.1em;
            font-weight: bold;
            margin-top: 20px;
            transition: transform 0.2s, box-shadow 0.2s;
        }
        button:hover {
            transform: translateY(-2px);
            box-shadow: 0 5px 20px rgba(56, 239, 125, 0.3);
        }
        .btn-cancel {
            background: linear-gradient(135deg, #666 0%, #444 100%);
            margin-left: 15px;
        }

        .buttons {
            text-align: center;
            margin-top: 30px;
        }

        .back-link {
            color: #aaa;
            text-decoration: none;
        }
        .back-link:hover { color: #fff; }
    </style>
    <script>
        function selectDomain(element) {
            // Remove selected from all
            document.querySelectorAll('.domain-option').forEach(el => {
                el.classList.remove('selected');
            });
            // Add selected to clicked
            element.classList.add('selected');
            // Check the radio
            element.querySelector('input').checked = true;
        }
    </script>
</head>
<body>
    <div class="header">
        <h1>Whachoo Talkin' Bout, Willis?</h1>
        <div class="subtitle">I couldn't automatically detect the research domain. Help me out!</div>
    </div>

    <div class="section">
        <h2>Your Hypothesis</h2>
        <div class="hypothesis-box">{{ hypothesis }}</div>
    </div>

    <form action="/submit_confirmed" method="POST">
        <div class="section">
            <h2>Select Research Domain</h2>
            <p style="color: #aaa; margin-bottom: 20px;">
                Pick the domain that best matches your hypothesis. This helps me search the right sources.
            </p>
            <div class="domain-grid">
                {% for key, info in domains.items() %}
                {% if key != 'unknown' %}
                <label class="domain-option" onclick="selectDomain(this)">
                    <input type="radio" name="domain" value="{{ key }}" {% if loop.first %}checked{% endif %}>
                    <div class="domain-name">{{ info.name }}</div>
                    <div class="domain-desc">{{ info.description }}</div>
                    <div class="domain-sources">Sources: {{ info.sources | join(', ') }}</div>
                </label>
                {% endif %}
                {% endfor %}
            </div>
        </div>

        <div class="buttons">
            <button type="submit">Submit with Selected Domain</button>
            <a href="/" class="back-link"><button type="button" class="btn-cancel">Cancel</button></a>
        </div>
    </form>
</body>
</html>
"""

# ============== ROUTES ==============

@app.route('/')
def index():
    # Sort hypotheses by submission time (newest first)
    sorted_hypotheses = sorted(
        hypotheses.values(),
        key=lambda x: x.get('submitted_at', ''),
        reverse=True
    )

    # Add publication links to each hypothesis
    for h in sorted_hypotheses:
        h['publications'] = find_publications(h['task_id'])

    return render_template_string(
        HTML_TEMPLATE,
        hypotheses=sorted_hypotheses,
        now=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        message=request.args.get('message')
    )

@app.route('/detail/<task_id>')
def detail(task_id):
    if task_id not in hypotheses:
        return redirect(url_for('index', message='Hypothesis not found'))

    h = hypotheses[task_id]
    h_results = results.get(task_id, [])
    h_critiques = critiques.get(task_id, [])
    h_publications = find_publications(task_id)

    return render_template_string(
        DETAIL_TEMPLATE,
        h=h,
        results=h_results,
        critiques=h_critiques,
        publications=h_publications,
        now=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    )

@app.route('/publications/<filename>')
def serve_publication(filename):
    """Serve publication files"""
    return send_from_directory(PUBLICATIONS_DIR, filename)

@app.route('/submit', methods=['POST'])
def submit():
    hypothesis = request.form.get('hypothesis', '').strip()
    if not hypothesis:
        return redirect(url_for('index', message='Please enter a hypothesis'))

    # Detect domain using LLM
    print(f"Detecting domain for: {hypothesis[:60]}...")
    domain = detect_domain(hypothesis)
    print(f"Detected domain: {domain}")

    # If domain is unclear, show the clarification modal
    if domain == "unknown":
        # Store hypothesis in session for later
        session['pending_hypothesis'] = hypothesis
        return redirect(url_for('clarify'))

    # Domain detected - extract search terms and create task
    search_terms = extract_search_terms(hypothesis, domain)
    print(f"Search terms: {search_terms}")

    task_id = create_task(hypothesis, domain=domain, search_terms=search_terms)
    domain_name = RESEARCH_DOMAINS[domain]["name"]
    return redirect(url_for('index', message=f'Hypothesis submitted! Domain: {domain_name}, Task ID: {task_id[:8]}...'))


@app.route('/clarify')
def clarify():
    """Show the 'Whachoo talkin bout Willis' domain clarification page"""
    hypothesis = session.get('pending_hypothesis', '')
    if not hypothesis:
        return redirect(url_for('index', message='No pending hypothesis'))

    return render_template_string(CLARIFY_TEMPLATE,
                                  hypothesis=hypothesis,
                                  domains=RESEARCH_DOMAINS,
                                  now=datetime.now().strftime('%Y-%m-%d %H:%M:%S'))


@app.route('/submit_confirmed', methods=['POST'])
def submit_confirmed():
    """Handle confirmed submission with user-selected domain"""
    hypothesis = session.pop('pending_hypothesis', '')
    domain = request.form.get('domain', 'unknown')

    if not hypothesis:
        return redirect(url_for('index', message='No pending hypothesis'))

    # Extract search terms with the confirmed domain
    search_terms = extract_search_terms(hypothesis, domain)
    print(f"Confirmed domain: {domain}, Search terms: {search_terms}")

    task_id = create_task(hypothesis, domain=domain, search_terms=search_terms)
    domain_name = RESEARCH_DOMAINS.get(domain, {}).get("name", domain)
    return redirect(url_for('index', message=f'Hypothesis submitted! Domain: {domain_name}, Task ID: {task_id[:8]}...'))

@app.route('/rerun/<task_id>', methods=['POST'])
def rerun(task_id):
    if task_id in hypotheses:
        hypothesis_text = hypotheses[task_id]['hypothesis']
        new_task_id = create_task(hypothesis_text)
        return redirect(url_for('index', message=f'Hypothesis resubmitted! New Task ID: {new_task_id[:8]}...'))
    return redirect(url_for('index', message='Hypothesis not found'))


@app.route('/delete/<task_id>', methods=['POST'])
def delete(task_id):
    """Delete a hypothesis and all its data, and signal agents to stop"""
    if task_id not in hypotheses:
        return redirect(url_for('index', message='Hypothesis not found'))

    hypothesis_text = hypotheses[task_id].get('hypothesis', '')[:50]

    # Send cancellation signal to Kafka
    try:
        p = get_producer()
        cancel_message = {
            "task_id": task_id,
            "action": "cancel",
            "reason": "User requested deletion",
            "created_at": datetime.utcnow().isoformat()
        }
        p.send('research.cancel', value=cancel_message)
        p.flush()
        print(f"Sent cancellation signal for task {task_id[:8]}...")
    except Exception as e:
        print(f"Failed to send cancellation signal: {e}")

    # Remove from in-memory tracking
    del hypotheses[task_id]

    # Remove associated results
    if task_id in results:
        del results[task_id]

    # Remove associated critiques
    if task_id in critiques:
        del critiques[task_id]

    # Add to cancelled set (for Kafka listener to ignore incoming messages)
    cancelled_tasks.add(task_id)

    print(f"Deleted hypothesis: {hypothesis_text}...")
    return redirect(url_for('index', message=f'Hypothesis deleted: {hypothesis_text}...'))


@app.route('/precepts')
def get_precepts():
    """Get current precepts configuration"""
    try:
        if os.path.exists(PRECEPTS_CONFIG_PATH):
            with open(PRECEPTS_CONFIG_PATH, 'r') as f:
                precepts = json.load(f)
            return json.dumps(precepts), 200, {'Content-Type': 'application/json'}
        else:
            return json.dumps({"error": "Precepts config not found"}), 404, {'Content-Type': 'application/json'}
    except Exception as e:
        return json.dumps({"error": str(e)}), 500, {'Content-Type': 'application/json'}


@app.route('/precepts', methods=['POST'])
def save_precepts():
    """Save precepts configuration"""
    try:
        precepts = request.get_json()
        if not precepts:
            return json.dumps({"error": "No data provided"}), 400, {'Content-Type': 'application/json'}

        # Validate structure
        total_weight = 0
        for key, value in precepts.items():
            if not isinstance(value, dict):
                return json.dumps({"error": f"Invalid precept format for {key}"}), 400, {'Content-Type': 'application/json'}
            if 'name' not in value or 'description' not in value or 'weight' not in value:
                return json.dumps({"error": f"Missing required fields in {key}"}), 400, {'Content-Type': 'application/json'}
            total_weight += float(value['weight'])

        # Validate weights sum to ~1.0 (allow small floating point errors)
        if abs(total_weight - 1.0) > 0.01:
            return json.dumps({"error": f"Weights must sum to 1.0 (current: {total_weight:.3f})"}), 400, {'Content-Type': 'application/json'}

        # Ensure config directory exists
        config_dir = os.path.dirname(PRECEPTS_CONFIG_PATH)
        os.makedirs(config_dir, exist_ok=True)

        # Try to fix permissions if needed (Windows Docker workaround)
        try:
            if os.path.exists(PRECEPTS_CONFIG_PATH):
                os.chmod(PRECEPTS_CONFIG_PATH, 0o666)
        except OSError:
            pass  # May fail on some systems, that's ok

        # Save to file using atomic write pattern
        temp_path = PRECEPTS_CONFIG_PATH + '.tmp'
        try:
            with open(temp_path, 'w') as f:
                json.dump(precepts, f, indent=2)
            # Atomic rename
            if os.path.exists(PRECEPTS_CONFIG_PATH):
                os.remove(PRECEPTS_CONFIG_PATH)
            os.rename(temp_path, PRECEPTS_CONFIG_PATH)
        except PermissionError as pe:
            # Clean up temp file if it exists
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except:
                    pass
            return json.dumps({
                "error": f"Permission denied writing to config file. Try: docker-compose down && docker-compose up --build"
            }), 500, {'Content-Type': 'application/json'}

        print(f"Saved {len(precepts)} precepts to {PRECEPTS_CONFIG_PATH}")
        return json.dumps({"success": True, "message": f"Saved {len(precepts)} precepts"}), 200, {'Content-Type': 'application/json'}
    except PermissionError as pe:
        return json.dumps({
            "error": f"Permission denied: {str(pe)}. Rebuild container: docker-compose up --build hypothesis-manager"
        }), 500, {'Content-Type': 'application/json'}
    except Exception as e:
        return json.dumps({"error": str(e)}), 500, {'Content-Type': 'application/json'}


if __name__ == '__main__':
    # Start Kafka listener in background
    threading.Thread(target=kafka_listener, daemon=True).start()

    print("Starting Hypothesis Manager...")
    print("Visit: http://localhost:5001")

    app.run(host='0.0.0.0', port=5001, debug=False)
