import os
import time
import json
import random
import re
import numpy as np
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from sentence_transformers import SentenceTransformer
import arxiv
import requests
from minio_client import store_papers_batch

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

# Load sentence transformer model at startup
print("Loading sentence transformer model...")
EMBEDDING_MODEL = SentenceTransformer('all-MiniLM-L6-v2')
print("Model loaded!")

def wait_for_kafka(max_retries=30, delay=15):
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
        client = arxiv.Client()
        search = arxiv.Search(
            query=search_query,
            max_results=max_results,
            sort_by=arxiv.SortCriterion.Relevance
        )

        papers = []
        for result in client.results(search):
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


def extract_quotes_from_papers(hypothesis, papers, embedding_model):
    """
    Extract relevant phrases from paper abstracts.
    Since we only have 300-char abstracts, we extract the most relevant sentences.
    """
    all_quotes = []

    if not papers or not hypothesis:
        return all_quotes

    # Embed the hypothesis
    hypothesis_emb = embedding_model.encode(hypothesis, normalize_embeddings=True)

    for paper in papers:
        abstract = paper.get("summary", "")
        if not abstract or len(abstract) < 50:
            continue

        # Split abstract into sentences (by . ; or ?)
        sentences = re.split(r'[.;?]', abstract)
        sentences = [s.strip() for s in sentences if len(s.strip()) > 20]

        if not sentences:
            continue

        # Embed all sentences
        sentence_embs = embedding_model.encode(sentences, normalize_embeddings=True)

        # Find the most relevant sentence
        best_idx, best_score = -1, 0.0
        for i, emb in enumerate(sentence_embs):
            sim = float(np.dot(hypothesis_emb, emb))
            if sim > best_score and sim > 0.3:  # Minimum relevance threshold
                best_score = sim
                best_idx = i

        if best_idx >= 0:
            # Determine context type based on similarity
            if best_score > 0.5:
                context_type = "supporting"
            elif best_score > 0.4:
                context_type = "related"
            else:
                context_type = "tangential"

            all_quotes.append({
                "text": sentences[best_idx].strip(),
                "paper_ref": "",  # Will be filled after MinIO storage
                "paper_title": paper.get("title", ""),
                "paper_id": paper.get("id", ""),
                "paper_source": paper.get("source", ""),
                "paper_authors": paper.get("authors", []),
                "paper_published": paper.get("published", ""),
                "relevance_score": round(best_score, 3),
                "context_type": context_type
            })

    # Sort by relevance and return top quotes
    all_quotes.sort(key=lambda x: x["relevance_score"], reverse=True)
    return all_quotes


def analyze_papers(hypothesis, papers):
    """Semantic analysis of papers against hypothesis using embeddings"""
    evidence_count = len(papers)
    sources_used = list(set(p.get('source', 'Unknown') for p in papers))

    if not papers:
        return {
            "confidence": 0.25,
            "findings": ["No relevant papers found"],
            "evidence_count": 0,
            "methodology": "semantic_analysis_v1",
            "concerns": ["Insufficient evidence for hypothesis"],
            "sources_searched": sources_used
        }

    # Embed the hypothesis
    hypothesis_embedding = EMBEDDING_MODEL.encode(hypothesis, normalize_embeddings=True)

    # Embed each paper (title + summary) and compute similarity
    paper_texts = [f"{p['title']}. {p['summary']}" for p in papers]
    paper_embeddings = EMBEDDING_MODEL.encode(paper_texts, normalize_embeddings=True)

    # Compute cosine similarities (dot product since normalized)
    similarities = [float(np.dot(hypothesis_embedding, emb)) for emb in paper_embeddings]

    # Rank papers by relevance
    ranked_papers = sorted(zip(papers, similarities), key=lambda x: x[1], reverse=True)

    # Compute confidence based on top paper similarities
    top_similarities = [s for _, s in ranked_papers[:5]]
    avg_similarity = sum(top_similarities) / len(top_similarities)

    # Map similarity to confidence (0.3-0.7 similarity -> 0.4-0.85 confidence)
    confidence = min(0.85, max(0.4, avg_similarity * 1.2))
    confidence = round(confidence, 2)

    # Generate findings from top relevant papers
    findings = []
    findings.append(f"Analyzed {evidence_count} papers from {', '.join(sources_used)}")

    # Add top 3 most relevant papers
    for i, (paper, sim) in enumerate(ranked_papers[:3]):
        relevance = "highly relevant" if sim > 0.5 else "moderately relevant" if sim > 0.35 else "tangentially relevant"
        findings.append(f"[{paper['source']}] {paper['title'][:70]}... ({relevance}, {sim:.2f})")

    # Check for potential contradictions (papers with moderate but not high similarity)
    concerns = []
    low_sim_papers = [p for p, s in ranked_papers if 0.2 < s < 0.35]
    if low_sim_papers:
        concerns.append(f"{len(low_sim_papers)} papers found with tangential relevance - may contain alternative perspectives")

    if avg_similarity < 0.35:
        concerns.append("Low semantic alignment between hypothesis and existing research")

    print(f"  → Semantic analysis: avg_similarity={avg_similarity:.3f}, confidence={confidence}")

    # Extract quotes from paper abstracts
    extracted_quotes = extract_quotes_from_papers(hypothesis, papers, EMBEDDING_MODEL)
    print(f"  → Extracted {len(extracted_quotes)} quotes from papers")

    return {
        "confidence": confidence,
        "findings": findings,
        "evidence_count": evidence_count,
        "methodology": "semantic_analysis_v1",
        "concerns": concerns,
        "sources_searched": sources_used,
        "top_relevance_score": round(ranked_papers[0][1], 3) if ranked_papers else 0,
        "extracted_quotes": extracted_quotes
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

    # Store papers to MinIO
    task_id = task.get("task_id", "unknown")
    paper_refs = []
    if papers:
        print(f"  → Storing {len(papers)} papers to MinIO...")
        paper_refs = store_papers_batch(task_id, papers)
        print(f"  → Stored {len(paper_refs)} papers")

    # Analyze papers
    print(f"  → Analyzing papers...")
    analysis = analyze_papers(hypothesis, papers)

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
    if "extracted_quotes" not in analysis:
        analysis["extracted_quotes"] = []

    # Backfill paper_ref in extracted quotes using MinIO keys
    # Create a mapping from paper_id to MinIO ref
    paper_id_to_ref = {}
    for i, paper in enumerate(papers):
        if i < len(paper_refs):
            paper_id_to_ref[paper.get("id", "")] = paper_refs[i]

    for quote in analysis.get("extracted_quotes", []):
        paper_id = quote.get("paper_id", "")
        if paper_id and paper_id in paper_id_to_ref:
            quote["paper_ref"] = paper_id_to_ref[paper_id]

    # Add top paper reference to findings
    if papers:
        top = papers[0]
        analysis["findings"].append(f"Top paper [{top.get('source', '?')}]: {top['title'][:80]} ({top.get('id', 'N/A')})")

    # Add domain info
    analysis["domain"] = domain

    # Add paper references
    analysis["paper_refs"] = paper_refs

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
            "confidence": float(research_result["confidence"]),
            "created_at": datetime.utcnow().isoformat()
        }

        # Publish result
        producer.send("research.result", value=result_message)
        producer.flush()

        print(f"  ✓ Published result (confidence: {research_result['confidence']})")


if __name__ == "__main__":
    main()
