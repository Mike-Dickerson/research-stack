import os
import time
import json
import random
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
import arxiv
import requests
import ollama

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://ollama:11434")
OLLAMA_MODEL = "phi3:mini"  # Small, fast 3.8B parameter model

def wait_for_kafka(max_retries=30, delay=2):
    """Wait for Kafka to be available with retries"""
    for attempt in range(max_retries):
        try:
            print(f"Attempting to connect to Kafka (attempt {attempt + 1}/{max_retries})...")
            consumer = KafkaConsumer(
                "research.task",
                bootstrap_servers=KAFKA,
                auto_offset_reset="earliest",
                group_id="agent-runner",
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


def search_arxiv(search_query, max_results=10):
    """Search arXiv for relevant papers"""
    try:
        search = arxiv.Search(
            query=search_query,
            max_results=max_results,
            sort_by=arxiv.SortCriterion.Relevance
        )

        papers = []
        for result in search.results():
            papers.append({
                "title": result.title,
                "authors": [author.name for author in result.authors][:3],
                "summary": result.summary[:300],
                "published": result.published.strftime("%Y-%m-%d"),
                "source": "arXiv",
                "id": result.entry_id.split('/')[-1]
            })

        return papers
    except Exception as e:
        print(f"  ⚠ arXiv search error: {e}")
        return []


def search_pubmed(search_query, max_results=10):
    """Search PubMed for biomedical papers"""
    try:
        # PubMed E-utilities API (free, no API key required for low volume)
        base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"

        # Search for paper IDs
        search_url = f"{base_url}/esearch.fcgi"
        search_params = {
            "db": "pubmed",
            "term": search_query,
            "retmax": max_results,
            "retmode": "json",
            "sort": "relevance"
        }

        search_resp = requests.get(search_url, params=search_params, timeout=10)
        search_data = search_resp.json()

        ids = search_data.get("esearchresult", {}).get("idlist", [])
        if not ids:
            return []

        # Fetch paper details
        fetch_url = f"{base_url}/esummary.fcgi"
        fetch_params = {
            "db": "pubmed",
            "id": ",".join(ids),
            "retmode": "json"
        }

        fetch_resp = requests.get(fetch_url, params=fetch_params, timeout=10)
        fetch_data = fetch_resp.json()

        papers = []
        result_data = fetch_data.get("result", {})
        for pmid in ids:
            if pmid in result_data:
                paper = result_data[pmid]
                authors = paper.get("authors", [])
                author_names = [a.get("name", "") for a in authors[:3]]

                papers.append({
                    "title": paper.get("title", ""),
                    "authors": author_names,
                    "summary": paper.get("sorttitle", "")[:300],
                    "published": paper.get("pubdate", ""),
                    "source": "PubMed",
                    "id": f"PMID:{pmid}"
                })

        return papers
    except Exception as e:
        print(f"  ⚠ PubMed search error: {e}")
        return []


def search_semantic_scholar(search_query, max_results=10):
    """Search Semantic Scholar for academic papers (multi-domain)"""
    try:
        # Semantic Scholar API (free, rate limited)
        url = "https://api.semanticscholar.org/graph/v1/paper/search"
        params = {
            "query": search_query,
            "limit": max_results,
            "fields": "title,authors,abstract,year,externalIds"
        }

        headers = {"Accept": "application/json"}
        resp = requests.get(url, params=params, headers=headers, timeout=10)

        if resp.status_code != 200:
            print(f"  ⚠ Semantic Scholar API returned {resp.status_code}")
            return []

        data = resp.json()
        papers = []

        for paper in data.get("data", []):
            authors = paper.get("authors", [])
            author_names = [a.get("name", "") for a in authors[:3]]

            # Get external ID (prefer DOI)
            ext_ids = paper.get("externalIds", {})
            paper_id = ext_ids.get("DOI") or ext_ids.get("ArXiv") or ext_ids.get("PubMed") or ""

            papers.append({
                "title": paper.get("title", ""),
                "authors": author_names,
                "summary": (paper.get("abstract") or "")[:300],
                "published": str(paper.get("year", "")),
                "source": "Semantic Scholar",
                "id": paper_id
            })

        return papers
    except Exception as e:
        print(f"  ⚠ Semantic Scholar search error: {e}")
        return []


def search_papers(search_query, sources, max_per_source=5):
    """Search multiple sources based on domain configuration"""
    all_papers = []

    for source in sources:
        print(f"  → Searching {source}...")

        if source == "arxiv":
            papers = search_arxiv(search_query, max_results=max_per_source)
        elif source == "pubmed":
            papers = search_pubmed(search_query, max_results=max_per_source)
        elif source == "semantic_scholar":
            papers = search_semantic_scholar(search_query, max_results=max_per_source)
        else:
            print(f"  ⚠ Unknown source: {source}")
            papers = []

        print(f"    Found {len(papers)} papers from {source}")
        all_papers.extend(papers)

    return all_papers


def analyze_with_llm(hypothesis, papers, domain="unknown"):
    """Use Ollama (local SLM) to analyze hypothesis against research papers"""
    try:
        # Build context from papers with source info
        papers_context = "\n\n".join([
            f"Paper {i+1} [{p.get('source', 'Unknown')}]: {p['title']}\nAuthors: {', '.join(p['authors'])}\nAbstract: {p['summary']}\nPublished: {p['published']}"
            for i, p in enumerate(papers[:7])  # Use top 7 papers
        ])

        # Get sources used
        sources_used = list(set(p.get('source', 'Unknown') for p in papers))

        prompt = f"""You are a scientific research analyst specializing in {domain}. Analyze this hypothesis against the provided research papers.

IMPORTANT: Only discuss findings that are DIRECTLY relevant to the hypothesis. Do not mention unrelated topics.

Hypothesis: {hypothesis}
Research Domain: {domain}

Research Papers from {', '.join(sources_used)}:
{papers_context}

Provide:
1. A confidence score (0.0-1.0) for how well the hypothesis is supported by existing research
2. Key findings (3-5 bullet points) - ONLY findings directly relevant to the hypothesis
3. Evidence count (number of relevant supporting papers found)
4. Any concerns or contradictions with the hypothesis specifically

Format your response as JSON:
{{
    "confidence": 0.0-1.0,
    "findings": ["finding1", "finding2", ...],
    "evidence_count": number,
    "concerns": ["concern1", "concern2", ...],
    "methodology": "ollama_phi3_multi_source_v1"
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
        result = json.loads(response_text.strip())
        result["sources_searched"] = sources_used
        return result

    except Exception as e:
        print(f"  ⚠ Ollama analysis error: {e}")
        print(f"  → Falling back to heuristic analysis")
        return fallback_analysis(hypothesis, papers)


def fallback_analysis(hypothesis, papers):
    """Fallback analysis when LLM is unavailable"""
    evidence_count = len(papers)
    sources_used = list(set(p.get('source', 'Unknown') for p in papers))

    # Base confidence on number of papers found
    if evidence_count >= 8:
        confidence = round(random.uniform(0.65, 0.85), 2)
    elif evidence_count >= 4:
        confidence = round(random.uniform(0.50, 0.75), 2)
    else:
        confidence = round(random.uniform(0.35, 0.60), 2)

    findings = [
        f"Found {evidence_count} relevant papers from {', '.join(sources_used)}",
        f"Most recent research published: {papers[0]['published'] if papers else 'N/A'}",
        "Further analysis needed to determine hypothesis validity",
        "Requires additional empirical data for validation"
    ]

    return {
        "confidence": confidence,
        "findings": findings,
        "evidence_count": evidence_count,
        "methodology": "heuristic_multi_source_v1",
        "concerns": ["Fallback analysis - Ollama unavailable"],
        "sources_searched": sources_used
    }


def conduct_research(task):
    """
    Research function - searches domain-appropriate sources and analyzes with LLM
    """
    payload = task.get("payload", {})
    hypothesis = payload.get("hypothesis", "Unknown hypothesis")
    domain = payload.get("domain", "unknown")
    search_terms = payload.get("search_terms", hypothesis[:100])
    sources = payload.get("sources", ["semantic_scholar"])  # Default fallback

    print(f"  → Domain: {domain}")
    print(f"  → Search terms: {search_terms[:60]}...")
    print(f"  → Sources to search: {sources}")

    # Search papers from domain-appropriate sources
    papers = search_papers(search_terms, sources, max_per_source=5)

    print(f"  → Total papers found: {len(papers)}")
    if papers:
        print(f"  → Top result [{papers[0].get('source', '?')}]: {papers[0]['title'][:50]}...")

    # Analyze with Ollama
    print(f"  → Analyzing with Ollama ({OLLAMA_MODEL})...")
    analysis = analyze_with_llm(hypothesis, papers, domain)

    # Ensure required fields exist (Ollama may return malformed JSON)
    if "findings" not in analysis:
        analysis["findings"] = []
    if "confidence" not in analysis:
        analysis["confidence"] = 0.5
    if "evidence_count" not in analysis:
        analysis["evidence_count"] = len(papers)
    if "concerns" not in analysis:
        analysis["concerns"] = []
    if "methodology" not in analysis:
        analysis["methodology"] = "unknown"
    if "sources_searched" not in analysis:
        analysis["sources_searched"] = sources

    # Add top paper reference to findings
    if papers:
        top = papers[0]
        analysis["findings"].append(f"Top paper [{top.get('source', '?')}]: {top['title'][:80]} ({top.get('id', 'N/A')})")

    # Add domain info
    analysis["domain"] = domain

    return analysis


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
                auto_offset_reset="latest",  # Only get new cancellations
                group_id="agent-runner-cancel",
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
    print("Agent Runner starting...")

    # Start cancellation listener in background
    import threading
    threading.Thread(target=cancellation_listener, daemon=True).start()

    consumer = wait_for_kafka()
    producer = create_producer()

    agent_id = f"agent-{random.randint(1000, 9999)}"
    print(f"Agent Runner listening for tasks (ID: {agent_id})")

    task_count = 0

    for msg in consumer:
        task = msg.value
        task_count += 1

        task_id = task.get("task_id", "unknown")
        swarm_id = task.get("swarm_id", "unknown")
        hypothesis = task.get("payload", {}).get("hypothesis", "No hypothesis")

        # Check if task was cancelled
        if task_id in cancelled_tasks:
            print(f"\n[Task {task_count}] SKIPPED (cancelled): {task_id[:8]}...")
            continue

        print(f"\n[Task {task_count}] Received: {hypothesis[:60]}...")
        print(f"  → task_id: {task_id}")
        print(f"  → Conducting research...")

        # Do the research
        research_result = conduct_research(task)

        # Create result message
        result_message = {
            "task_id": task_id,
            "swarm_id": swarm_id,
            "agent_id": agent_id,
            "result": research_result,
            "confidence": research_result["confidence"],
            "created_at": datetime.utcnow().isoformat()
        }

        # Publish result
        producer.send("research.result", value=result_message)
        producer.flush()

        print(f"  ✓ Published result (confidence: {research_result['confidence']})")


if __name__ == "__main__":
    main()
