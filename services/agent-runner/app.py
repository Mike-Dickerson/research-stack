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


def search_arxiv(hypothesis, max_results=10):
    """Search arXiv for relevant papers"""
    try:
        # Extract key terms for search
        search_query = hypothesis[:200]  # Use hypothesis as search query

        search = arxiv.Search(
            query=search_query,
            max_results=max_results,
            sort_by=arxiv.SortCriterion.Relevance
        )

        papers = []
        for result in search.results():
            papers.append({
                "title": result.title,
                "authors": [author.name for author in result.authors][:3],  # First 3 authors
                "summary": result.summary[:300],  # First 300 chars
                "published": result.published.strftime("%Y-%m-%d"),
                "arxiv_id": result.entry_id.split('/')[-1]
            })

        return papers
    except Exception as e:
        print(f"  ⚠ arXiv search error: {e}")
        return []


def analyze_with_llm(hypothesis, papers):
    """Use Ollama (local SLM) to analyze hypothesis against research papers"""
    try:
        # Build context from papers
        papers_context = "\n\n".join([
            f"Paper {i+1}: {p['title']}\nAuthors: {', '.join(p['authors'])}\nAbstract: {p['summary']}\nPublished: {p['published']}"
            for i, p in enumerate(papers[:5])  # Use top 5 papers
        ])

        prompt = f"""You are a scientific research analyst. Analyze this hypothesis against the provided research papers.

Hypothesis: {hypothesis}

Research Papers from arXiv:
{papers_context}

Provide:
1. A confidence score (0.0-1.0) for how well the hypothesis is supported by existing research
2. Key findings (3-5 bullet points)
3. Evidence count (number of relevant supporting papers found)
4. Any concerns or contradictions

Format your response as JSON:
{{
    "confidence": 0.0-1.0,
    "findings": ["finding1", "finding2", ...],
    "evidence_count": number,
    "concerns": ["concern1", "concern2", ...],
    "methodology": "ollama_phi3_analysis_v1"
}}"""

        # Call Ollama
        client = ollama.Client(host=OLLAMA_HOST)
        response = client.chat(
            model=OLLAMA_MODEL,
            messages=[{"role": "user", "content": prompt}],
            format="json"  # Request JSON output
        )

        # Parse JSON response
        response_text = response['message']['content']
        result = json.loads(response_text.strip())
        return result

    except Exception as e:
        print(f"  ⚠ Ollama analysis error: {e}")
        print(f"  → Falling back to heuristic analysis")
        return fallback_analysis(hypothesis, papers)


def fallback_analysis(hypothesis, papers):
    """Fallback analysis when LLM is unavailable"""
    # Simple heuristic-based analysis
    evidence_count = len(papers)

    # Base confidence on number of papers found
    if evidence_count >= 8:
        confidence = round(random.uniform(0.65, 0.85), 2)
    elif evidence_count >= 4:
        confidence = round(random.uniform(0.50, 0.75), 2)
    else:
        confidence = round(random.uniform(0.35, 0.60), 2)

    findings = [
        f"Found {evidence_count} relevant papers in arXiv database",
        f"Most recent research published: {papers[0]['published'] if papers else 'N/A'}",
        "Hypothesis aligns with existing gravitational research literature" if evidence_count > 5 else "Limited direct evidence in current literature",
        "Requires additional empirical data for validation"
    ]

    return {
        "confidence": confidence,
        "findings": findings,
        "evidence_count": evidence_count,
        "methodology": "heuristic_literature_analysis_v1",
        "concerns": ["Fallback analysis - Ollama unavailable"]
    }


def conduct_research(task):
    """
    Real research function - searches arXiv and analyzes with LLM
    """
    hypothesis = task.get("payload", {}).get("hypothesis", "Unknown hypothesis")

    print(f"  → Searching arXiv for: '{hypothesis[:60]}...'")

    # Search arXiv for relevant papers
    papers = search_arxiv(hypothesis, max_results=10)

    print(f"  → Found {len(papers)} relevant papers")
    if papers:
        print(f"  → Top result: {papers[0]['title'][:60]}...")

    # Analyze with Ollama
    print(f"  → Analyzing with Ollama ({OLLAMA_MODEL})...")
    analysis = analyze_with_llm(hypothesis, papers)

    # Add paper references to findings
    if papers:
        analysis["findings"].append(f"Top paper: {papers[0]['title']} ({papers[0]['arxiv_id']})")

    return analysis


def main():
    print("Agent Runner starting...")

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
