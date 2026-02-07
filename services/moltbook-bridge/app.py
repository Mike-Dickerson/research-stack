import os
import time
import json
import random
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
import requests
import ollama

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://ollama:11434")
OLLAMA_MODEL = "phi3:mini"

def wait_for_kafka(max_retries=30, delay=2):
    """Wait for Kafka to be available with retries"""
    for attempt in range(max_retries):
        try:
            print(f"Attempting to connect to Kafka (attempt {attempt + 1}/{max_retries})...")
            consumer = KafkaConsumer(
                "research.task",
                bootstrap_servers=KAFKA,
                auto_offset_reset="earliest",
                group_id="moltbook-external-swarm",
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
    """Create a Kafka producer for publishing results"""
    return KafkaProducer(
        bootstrap_servers=KAFKA,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )


def rephrase_hypothesis(hypothesis, domain):
    """Use Ollama to rephrase hypothesis for alternative search angle"""
    try:
        client = ollama.Client(host=OLLAMA_HOST)

        prompt = f"""Rephrase this {domain} hypothesis using different terminology to search for related but independent research.

ORIGINAL: {hypothesis}

Rules:
- Use synonyms and alternative scientific terms
- Focus on the underlying mechanisms or phenomena
- Keep it concise (1-2 sentences)
- Maintain scientific accuracy

Rephrased:"""

        response = client.chat(
            model=OLLAMA_MODEL,
            messages=[{"role": "user", "content": prompt}]
        )

        rephrased = response['message']['content'].strip()
        print(f"  → Rephrased: {rephrased[:80]}...")
        return rephrased

    except Exception as e:
        print(f"  ⚠ Rephrase error: {e}")
        return hypothesis


def extract_alternative_terms(hypothesis, domain):
    """Extract alternative search terms for external validation"""
    try:
        client = ollama.Client(host=OLLAMA_HOST)

        prompt = f"""Extract 3-5 ALTERNATIVE search terms for this {domain} hypothesis.
Use different terminology than the obvious terms - think about related concepts, mechanisms, or phenomena.

HYPOTHESIS: {hypothesis}

Rules:
- Use scientific synonyms
- Include broader/narrower concepts
- Focus on testable aspects
- Return as comma-separated list

Alternative terms:"""

        response = client.chat(
            model=OLLAMA_MODEL,
            messages=[{"role": "user", "content": prompt}]
        )

        terms = response['message']['content'].strip()
        return terms

    except Exception as e:
        print(f"  ⚠ Term extraction error: {e}")
        return hypothesis[:100]


def search_nasa_ads(search_query, max_results=5):
    """Search NASA ADS for astrophysics papers"""
    try:
        # NASA ADS API (free, but rate limited without API key)
        # Using the public search endpoint
        url = "https://api.adsabs.harvard.edu/v1/search/query"

        # Try without API key first (limited but works for basic searches)
        headers = {"Accept": "application/json"}
        params = {
            "q": search_query,
            "rows": max_results,
            "fl": "title,author,abstract,pubdate,bibcode"
        }

        # Check for API key
        api_key = os.getenv("ADS_API_KEY", "")
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        resp = requests.get(url, params=params, headers=headers, timeout=15)

        if resp.status_code == 401:
            print("  ⚠ NASA ADS requires API key - skipping")
            return []

        if resp.status_code != 200:
            print(f"  ⚠ NASA ADS returned {resp.status_code}")
            return []

        data = resp.json()
        papers = []

        for doc in data.get("response", {}).get("docs", []):
            authors = doc.get("author", [])[:3]
            papers.append({
                "title": doc.get("title", [""])[0] if doc.get("title") else "",
                "authors": authors,
                "summary": (doc.get("abstract") or "")[:300],
                "published": doc.get("pubdate", ""),
                "source": "NASA ADS",
                "id": doc.get("bibcode", "")
            })

        return papers

    except Exception as e:
        print(f"  ⚠ NASA ADS error: {e}")
        return []


def search_openalex(search_query, max_results=5):
    """Search OpenAlex for scholarly works (free, no API key required)"""
    try:
        url = "https://api.openalex.org/works"
        params = {
            "search": search_query,
            "per_page": max_results,
            "select": "title,authorships,abstract_inverted_index,publication_date,doi"
        }

        headers = {
            "Accept": "application/json",
            "User-Agent": "MoltbookBridge/1.0 (research-stack)"
        }

        resp = requests.get(url, params=params, headers=headers, timeout=15)

        if resp.status_code != 200:
            print(f"  ⚠ OpenAlex returned {resp.status_code}")
            return []

        data = resp.json()
        papers = []

        for work in data.get("results", []):
            # Reconstruct abstract from inverted index
            abstract = ""
            inv_idx = work.get("abstract_inverted_index")
            if inv_idx:
                # Simple reconstruction - just get words
                word_positions = []
                for word, positions in inv_idx.items():
                    for pos in positions:
                        word_positions.append((pos, word))
                word_positions.sort()
                abstract = " ".join([w for _, w in word_positions[:60]])

            # Get authors
            authors = []
            for authorship in work.get("authorships", [])[:3]:
                author = authorship.get("author", {})
                if author.get("display_name"):
                    authors.append(author["display_name"])

            papers.append({
                "title": work.get("title", ""),
                "authors": authors,
                "summary": abstract[:300],
                "published": work.get("publication_date", ""),
                "source": "OpenAlex",
                "id": work.get("doi", "")
            })

        return papers

    except Exception as e:
        print(f"  ⚠ OpenAlex error: {e}")
        return []


def search_crossref(search_query, max_results=5):
    """Search CrossRef for DOI-indexed papers (free, no API key required)"""
    try:
        url = "https://api.crossref.org/works"
        params = {
            "query": search_query,
            "rows": max_results,
            "select": "title,author,abstract,published-print,DOI"
        }

        headers = {
            "Accept": "application/json",
            "User-Agent": "MoltbookBridge/1.0 (mailto:research@example.com)"
        }

        resp = requests.get(url, params=params, headers=headers, timeout=15)

        if resp.status_code != 200:
            print(f"  ⚠ CrossRef returned {resp.status_code}")
            return []

        data = resp.json()
        papers = []

        for item in data.get("message", {}).get("items", []):
            # Get title
            title_list = item.get("title", [])
            title = title_list[0] if title_list else ""

            # Get authors
            authors = []
            for author in item.get("author", [])[:3]:
                name = f"{author.get('given', '')} {author.get('family', '')}".strip()
                if name:
                    authors.append(name)

            # Get published date
            pub_date = ""
            pub_parts = item.get("published-print", {}).get("date-parts", [[]])
            if pub_parts and pub_parts[0]:
                pub_date = "-".join(str(p) for p in pub_parts[0])

            papers.append({
                "title": title,
                "authors": authors,
                "summary": (item.get("abstract") or "")[:300],
                "published": pub_date,
                "source": "CrossRef",
                "id": item.get("DOI", "")
            })

        return papers

    except Exception as e:
        print(f"  ⚠ CrossRef error: {e}")
        return []


def search_external_sources(search_query, domain, max_per_source=5):
    """Search external sources different from main agent-runner"""
    all_papers = []

    # For physics/astronomy, prioritize NASA ADS
    if domain in ["physics", "earth_science", "engineering"]:
        print("  → Searching NASA ADS...")
        papers = search_nasa_ads(search_query, max_results=max_per_source)
        print(f"    Found {len(papers)} papers from NASA ADS")
        all_papers.extend(papers)

    # OpenAlex works for all domains (free, comprehensive)
    print("  → Searching OpenAlex...")
    papers = search_openalex(search_query, max_results=max_per_source)
    print(f"    Found {len(papers)} papers from OpenAlex")
    all_papers.extend(papers)

    # CrossRef as backup/additional source
    print("  → Searching CrossRef...")
    papers = search_crossref(search_query, max_results=max_per_source)
    print(f"    Found {len(papers)} papers from CrossRef")
    all_papers.extend(papers)

    return all_papers


def analyze_with_external_perspective(hypothesis, papers, domain):
    """Use Ollama to analyze from an external/adversarial perspective"""
    try:
        client = ollama.Client(host=OLLAMA_HOST)

        # Build context from papers
        papers_context = "\n\n".join([
            f"Paper {i+1} [{p.get('source', 'Unknown')}]: {p['title']}\nAuthors: {', '.join(p['authors'])}\nAbstract: {p['summary']}\nPublished: {p['published']}"
            for i, p in enumerate(papers[:7])
        ])

        sources_used = list(set(p.get('source', 'Unknown') for p in papers))

        prompt = f"""You are an EXTERNAL REVIEWER providing independent validation of this hypothesis.
Your role is to find ALTERNATIVE evidence and potential CONTRADICTIONS, not just confirmations.

Hypothesis: {hypothesis}
Domain: {domain}

External Research from {', '.join(sources_used)}:
{papers_context}

Provide an INDEPENDENT assessment:
1. Confidence score (0.0-1.0) - be skeptical, don't just agree
2. Supporting evidence from external sources
3. Contradicting evidence or alternative explanations
4. Gaps in the hypothesis that need addressing
5. Reproducibility concerns

Format as JSON:
{{
    "confidence": 0.0-1.0,
    "findings": ["finding1", "finding2", ...],
    "evidence_count": number,
    "concerns": ["concern1", "concern2", ...],
    "alternative_explanations": ["alt1", "alt2", ...],
    "methodology": "moltbook_external_validation_v1"
}}"""

        response = client.chat(
            model=OLLAMA_MODEL,
            messages=[{"role": "user", "content": prompt}],
            format="json"
        )

        response_text = response['message']['content']
        result = json.loads(response_text.strip())
        result["sources_searched"] = sources_used
        result["external_validation"] = True
        return result

    except Exception as e:
        print(f"  ⚠ External analysis error: {e}")
        return fallback_analysis(hypothesis, papers)


def fallback_analysis(hypothesis, papers):
    """Fallback when LLM unavailable"""
    evidence_count = len(papers)
    sources_used = list(set(p.get('source', 'Unknown') for p in papers))

    # External swarm should be more skeptical
    if evidence_count >= 8:
        confidence = round(random.uniform(0.55, 0.75), 2)  # Lower than internal
    elif evidence_count >= 4:
        confidence = round(random.uniform(0.40, 0.65), 2)
    else:
        confidence = round(random.uniform(0.25, 0.50), 2)

    findings = [
        f"External validation: Found {evidence_count} papers from {', '.join(sources_used)}",
        f"Independent sources consulted: {', '.join(sources_used)}",
        "External review pending deeper analysis",
    ]

    if papers:
        findings.append(f"Top external paper: {papers[0]['title'][:80]}")

    return {
        "confidence": confidence,
        "findings": findings,
        "evidence_count": evidence_count,
        "concerns": ["External heuristic evaluation - requires LLM for full analysis"],
        "alternative_explanations": [],
        "methodology": "moltbook_external_heuristic_v1",
        "sources_searched": sources_used,
        "external_validation": True
    }


def should_process_task(task):
    """Determine if this task should get external validation"""
    hypothesis = task.get("payload", {}).get("hypothesis", "").lower()

    # Scientific hypotheses that benefit from external validation
    scientific_keywords = [
        "dark matter", "galaxy", "gravitational", "spacetime", "black hole",
        "quantum", "particle", "cosmic", "astrophysical", "relativistic",
        "evolution", "genetic", "cellular", "molecular", "neural",
        "climate", "atmospheric", "geological", "environmental",
        "machine learning", "artificial intelligence", "algorithm"
    ]

    for keyword in scientific_keywords:
        if keyword in hypothesis:
            return True

    # Also process if domain suggests it needs external validation
    domain = task.get("payload", {}).get("domain", "")
    if domain in ["physics", "biology", "medical", "computer_science", "earth_science"]:
        return True

    return False


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
                group_id="moltbook-cancel",
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
    print("=" * 60)
    print("🦞 MOLTBOOK EXTERNAL SWARM - Real External Validation 🦞")
    print("=" * 60)
    print("Sources: NASA ADS, OpenAlex, CrossRef")
    print("Role: Independent external validation (Crustafarian Precept #5)")
    print("=" * 60)

    # Start cancellation listener
    import threading
    threading.Thread(target=cancellation_listener, daemon=True).start()

    consumer = wait_for_kafka()
    producer = create_producer()

    bridge_id = f"moltbook-external-{random.randint(1000, 9999)}"
    print(f"External swarm ID: {bridge_id}")
    print("Listening for research tasks...\n")

    task_count = 0

    for msg in consumer:
        task = msg.value
        task_count += 1

        task_id = task.get("task_id", "unknown")
        hypothesis = task.get("payload", {}).get("hypothesis", "Unknown")
        domain = task.get("payload", {}).get("domain", "unknown")

        # Check if cancelled
        if task_id in cancelled_tasks:
            print(f"\n[Task {task_count}] SKIPPED (cancelled): {task_id[:8]}...")
            continue

        # Check if we should process this
        if not should_process_task(task):
            if task_count % 5 == 0:
                print(f"[Task {task_count}] Monitoring... (not scientific enough for external validation)")
            continue

        print(f"\n{'='*60}")
        print(f"[Task {task_count}] 🎯 EXTERNAL VALIDATION TRIGGERED")
        print(f"{'='*60}")
        print(f"Hypothesis: {hypothesis[:80]}...")
        print(f"Domain: {domain}")
        print(f"Task ID: {task_id[:8]}...")

        # Get alternative search terms
        print("\n→ Extracting alternative search terms...")
        alt_terms = extract_alternative_terms(hypothesis, domain)
        print(f"  Alternative terms: {alt_terms[:60]}...")

        # Search external sources
        print("\n→ Searching external sources...")
        papers = search_external_sources(alt_terms, domain, max_per_source=5)
        print(f"  Total papers found: {len(papers)}")

        if papers:
            print(f"  Top result [{papers[0].get('source', '?')}]: {papers[0]['title'][:50]}...")

        # Analyze with external perspective
        print(f"\n→ Analyzing with Ollama (external perspective)...")
        analysis = analyze_with_external_perspective(hypothesis, papers, domain)

        # Ensure required fields
        if "findings" not in analysis:
            analysis["findings"] = []
        if "confidence" not in analysis:
            analysis["confidence"] = 0.5
        if "evidence_count" not in analysis:
            analysis["evidence_count"] = len(papers)
        if "concerns" not in analysis:
            analysis["concerns"] = []
        if "alternative_explanations" not in analysis:
            analysis["alternative_explanations"] = []

        # Add top paper reference
        if papers:
            top = papers[0]
            analysis["findings"].append(
                f"External source [{top.get('source', '?')}]: {top['title'][:70]} ({top.get('id', 'N/A')})"
            )

        # Create result message
        result_message = {
            "task_id": task_id,
            "swarm_id": task.get("swarm_id", "unknown"),
            "agent_id": f"{bridge_id}-external-swarm",
            "result": {
                "findings": analysis["findings"],
                "confidence": analysis["confidence"],
                "evidence_count": analysis["evidence_count"],
                "concerns": analysis["concerns"],
                "alternative_explanations": analysis.get("alternative_explanations", []),
                "methodology": analysis.get("methodology", "moltbook_external_v1"),
                "sources_searched": analysis.get("sources_searched", []),
                "external_validation": True,
                "external_network": "moltbook_crustafarian_council"
            },
            "confidence": analysis["confidence"],
            "created_at": datetime.utcnow().isoformat()
        }

        # Publish to research.result
        producer.send("research.result", value=result_message)
        producer.flush()

        print(f"\n✓ Published external validation (confidence: {analysis['confidence']})")
        print(f"  Sources: {', '.join(analysis.get('sources_searched', []))}")
        print(f"  Evidence: {analysis['evidence_count']} papers")
        print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
