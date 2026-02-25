import os
import json
import time
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from jinja2 import Template
import markdown
from weasyprint import HTML, CSS
from minio_client import get_papers_batch

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
OUTPUT_DIR = "/app/publications"

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)


def wait_for_kafka(max_retries=30, delay=15):
    """Wait for Kafka to be available"""
    for attempt in range(max_retries):
        try:
            print(f"Attempting to connect to Kafka (attempt {attempt + 1}/{max_retries})...")
            consumer = KafkaConsumer(
                "research.consensus",
                bootstrap_servers=KAFKA,
                auto_offset_reset="earliest",
                group_id="document-generator",
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


# New Research Document Template
RESEARCH_DOCUMENT_TEMPLATE = """
# {{ title }}

**Research Document**
**Generated**: {{ generated_date }}
**Document ID**: `{{ task_id }}`

---

## Abstract

{{ abstract }}

---

## 1. Original Hypothesis

{{ original_hypothesis }}

---

## 2. Literature Review

### 2.1 Sources Summary

| Source | Papers Found | Key Topic |
|--------|-------------|-----------|
{% for source in sources_summary %}
| {{ source.name }} | {{ source.count }} | {{ source.key_topic }} |
{% endfor %}

**Total Papers Analyzed**: {{ total_papers }}
**Total Quotes Extracted**: {{ total_quotes }}

### 2.2 Key Evidence

{% for item in bibliography %}
#### {{ item.citation }}

> "{{ item.quote }}"

**Relevance Score**: {{ item.relevance }} | **Context**: {{ item.context_type }}

{% endfor %}

{% if not bibliography %}
*No quotes were extracted from the literature.*
{% endif %}

---

## 3. Hypothesis Evolution

{% if hypothesis_versions|length > 1 %}
The hypothesis underwent **{{ hypothesis_versions|length - 1 }} refinement(s)** based on evidence analysis.

{% for version in hypothesis_versions %}
### Version {{ version.version }}{% if version.version == 1 %} (Original){% endif %}

**Hypothesis**: {{ version.text }}

{% if version.version > 1 %}
**Modification Trigger**: {{ version.trigger }}

**Reasoning**: {{ version.reasoning }}

{% endif %}
{% endfor %}

### Final Hypothesis

{{ current_hypothesis }}

{% else %}
The hypothesis remained **unchanged** throughout the research process.

**Hypothesis**: {{ current_hypothesis }}
{% endif %}

---

## 4. Methodology

### 4.1 Research Process

This research was conducted using a distributed multi-agent validation system:

1. **Literature Search**: Parallel searches across multiple academic databases
   - Local agents: {{ local_sources }}
   - External validation: {{ external_sources }}

2. **Evidence Analysis**: Semantic similarity analysis using sentence-transformers
   - Papers analyzed: {{ total_papers }}
   - Quotes extracted: {{ total_quotes }}

3. **Peer Review**: {{ critique_count }} independent evaluations using Crustafarian Precepts

4. **Consensus Building**: Democratic aggregation with 0.6 threshold for approval

### 4.2 Iterations

| Iteration | Outcome | Score |
|-----------|---------|-------|
{% for iter in iterations %}
| {{ iter.number }} | {{ iter.outcome }} | {{ iter.score }} |
{% endfor %}

---

## 5. Results

### 5.1 Consensus Metrics

| Metric | Value |
|--------|-------|
| **Final Score** | {{ average_score }} |
| **Score Variance** | {{ score_variance }} |
| **Consensus Status** | {{ consensus_status }} |
| **External Validation** | {{ external_participation }} |

### 5.2 Precept Alignment Summary

{% for precept, score in precept_scores.items() %}
- **{{ precept }}**: {{ "%.2f"|format(score|float) }}{% if (score|float) >= 0.7 %} (Pass){% elif (score|float) >= 0.5 %} (Marginal){% else %} (Fail){% endif %}
{% endfor %}

---

## 6. Conclusion

### 6.1 Hypothesis Verdict

**Confidence Rating**: {{ confidence_rating }} ({{ confidence_text }})

**Verdict**: **{{ verdict }}**

{{ verdict_text }}

### 6.2 Summary of Findings

{{ conclusion_summary }}

{% if supporting_evidence %}
**Key Supporting Evidence:**
{% for evidence in supporting_evidence %}
- {{ evidence }}
{% endfor %}
{% endif %}

{% if concerns %}
**Concerns Noted:**
{% for concern in concerns %}
- {{ concern }}
{% endfor %}
{% endif %}

### 6.3 Recommendations

{% if verdict == "SUPPORTED" or verdict == "PARTIALLY SUPPORTED" %}
This hypothesis has sufficient evidence support for further investigation:

1. Conduct deeper literature review in specific sub-areas
2. Design experiments to test key predictions
3. Seek peer collaboration in relevant research communities
{% else %}
This hypothesis requires refinement before proceeding:

1. Address the concerns noted above
2. Gather additional evidence in weak areas
3. Consider narrowing or refining the hypothesis scope
4. Resubmit for re-evaluation after improvements
{% endif %}

---

## References

{% for ref in references %}
{{ ref }}

{% endfor %}

---

*Generated by Research Stack v2.0 - Distributed Multi-Agent Research System*
"""


def generate_score_interpretation(score):
    """Generate human-readable score interpretation"""
    if score >= 0.85:
        return "Exceptional hypothesis with strong literature support"
    elif score >= 0.70:
        return "Strong hypothesis worthy of significant research investment"
    elif score >= 0.60:
        return "Promising hypothesis with adequate foundation"
    elif score >= 0.40:
        return "Hypothesis needs substantial refinement"
    else:
        return "Hypothesis not currently viable based on evidence"


def generate_variance_interpretation(variance):
    """Generate variance interpretation"""
    if variance < 0.05:
        return "Very strong agreement among evaluators"
    elif variance < 0.15:
        return "Moderate agreement among evaluators"
    else:
        return "Significant disagreement among evaluators"


def generate_consensus_interpretation(status):
    """Generate consensus status interpretation"""
    interpretations = {
        "STRONG_CONSENSUS": "Independent evaluators strongly agree",
        "MODERATE_CONSENSUS": "Independent evaluators generally agree",
        "DIVERGENT_OPINIONS": "Evaluators have conflicting assessments"
    }
    return interpretations.get(status, "Unknown consensus status")


def generate_decision_interpretation(decision):
    """Generate decision interpretation"""
    if decision == "APPROVED":
        return "Hypothesis approved for publication and further research"
    else:
        return "Hypothesis requires refinement before publication"


def generate_apa_citation(paper):
    """Generate APA-style citation from paper metadata"""
    authors = paper.get("authors", [])
    title = paper.get("title", "Unknown Title")
    published = paper.get("published", "n.d.")
    source = paper.get("source", "")
    paper_id = paper.get("id", "")

    # Format authors (Last, F. M.)
    formatted_authors = []
    for author in authors[:3]:
        if not author:
            continue
        parts = author.split()
        if len(parts) >= 2:
            last = parts[-1]
            initials = ". ".join(p[0].upper() for p in parts[:-1] if p) + "."
            formatted_authors.append(f"{last}, {initials}")
        else:
            formatted_authors.append(author)

    if len(authors) > 3:
        author_str = ", ".join(formatted_authors) + ", et al."
    elif len(formatted_authors) > 1:
        author_str = ", & ".join(formatted_authors)
    elif formatted_authors:
        author_str = formatted_authors[0]
    else:
        author_str = "Unknown Author"

    # Extract year
    year = published[:4] if published and len(published) >= 4 else "n.d."

    # Format based on source
    if source == "arXiv":
        return f"{author_str} ({year}). {title}. *arXiv preprint* arXiv:{paper_id}."
    elif source == "PubMed":
        return f"{author_str} ({year}). {title}. *PubMed* {paper_id}."
    elif paper_id and ("10." in str(paper_id) or "doi" in str(paper_id).lower()):
        return f"{author_str} ({year}). {title}. https://doi.org/{paper_id}"
    else:
        return f"{author_str} ({year}). {title}. *{source}*."


def generate_conclusion(avg_score, findings, concerns, quotes):
    """Generate the hypothesis conclusion with verdict and confidence"""

    # Determine verdict based on score
    if avg_score >= 0.75:
        verdict = "SUPPORTED"
        verdict_text = "The evidence strongly supports this hypothesis. The literature review found consistent supporting evidence across multiple sources."
    elif avg_score >= 0.6:
        verdict = "PARTIALLY SUPPORTED"
        verdict_text = "The evidence provides partial support for this hypothesis. While some literature supports the core claims, there are areas that require further investigation."
    elif avg_score >= 0.4:
        verdict = "INCONCLUSIVE"
        verdict_text = "The available evidence is insufficient to draw a firm conclusion. The literature search found limited or mixed results."
    else:
        verdict = "NOT SUPPORTED"
        verdict_text = "The evidence does not support this hypothesis. The literature review found insufficient or contradicting evidence."

    # Confidence interpretation
    if avg_score >= 0.8:
        confidence_text = "High confidence - strong evidence alignment"
    elif avg_score >= 0.6:
        confidence_text = "Moderate confidence - reasonable evidence support"
    elif avg_score >= 0.4:
        confidence_text = "Low confidence - limited or mixed evidence"
    else:
        confidence_text = "Very low confidence - contradicting or absent evidence"

    # Build conclusion summary
    finding_count = len(findings) if findings else 0
    quote_count = len(quotes) if quotes else 0
    supporting_quotes = [q for q in (quotes or []) if q.get("context_type") == "supporting"]

    summary_parts = []
    summary_parts.append(f"This research analyzed {finding_count} findings from multiple academic sources.")

    if quote_count > 0:
        summary_parts.append(f"{quote_count} relevant excerpts were extracted from the literature.")

    if supporting_quotes:
        summary_parts.append(f"{len(supporting_quotes)} excerpts directly support the hypothesis.")

    if concerns:
        summary_parts.append(f"{len(concerns)} concerns were noted during the evaluation.")

    conclusion_summary = " ".join(summary_parts)

    # Format supporting evidence for display
    supporting_evidence = []
    for q in supporting_quotes[:3]:
        supporting_evidence.append(f'"{q.get("text", "")[:100]}..." - {q.get("paper_title", "Unknown")[:50]}')

    return {
        "confidence_rating": f"{avg_score:.2f}",
        "confidence_text": confidence_text,
        "verdict": verdict,
        "verdict_text": verdict_text,
        "conclusion_summary": conclusion_summary,
        "supporting_evidence": supporting_evidence,
        "concerns": concerns[:5] if concerns else []
    }


def collect_all_quotes(results):
    """Collect all extracted quotes from research results"""
    all_quotes = []
    for result in results:
        result_quotes = result.get("result", {}).get("extracted_quotes", [])
        all_quotes.extend(result_quotes)
    # Sort by relevance
    all_quotes.sort(key=lambda x: x.get("relevance_score", 0), reverse=True)
    return all_quotes


def build_bibliography(quotes, papers):
    """Build bibliography with quotes and APA citations"""
    bibliography = []

    # Create paper lookup by ref and id
    paper_lookup = {}
    for paper in papers:
        paper_id = paper.get("id", "")
        if paper_id:
            paper_lookup[paper_id] = paper

    # Process quotes
    for quote in quotes[:10]:  # Limit to top 10 quotes
        paper_id = quote.get("paper_id", "")
        paper_title = quote.get("paper_title", "")

        # Try to find the full paper info
        paper = paper_lookup.get(paper_id)
        if not paper:
            # Build minimal paper info from quote
            paper = {
                "title": paper_title,
                "authors": quote.get("paper_authors", []),
                "published": quote.get("paper_published", ""),
                "source": quote.get("paper_source", ""),
                "id": paper_id
            }

        citation = generate_apa_citation(paper)

        bibliography.append({
            "citation": citation,
            "quote": quote.get("text", "")[:200],
            "relevance": f"{quote.get('relevance_score', 0):.2f}",
            "context_type": quote.get("context_type", "related").title()
        })

    return bibliography


def build_references_list(quotes, papers):
    """Build a list of unique APA references"""
    seen_ids = set()
    references = []

    # Create paper lookup
    paper_lookup = {}
    for paper in papers:
        paper_id = paper.get("id", "")
        if paper_id:
            paper_lookup[paper_id] = paper

    # Add papers from quotes
    for quote in quotes:
        paper_id = quote.get("paper_id", "")
        if paper_id and paper_id not in seen_ids:
            paper = paper_lookup.get(paper_id)
            if not paper:
                paper = {
                    "title": quote.get("paper_title", ""),
                    "authors": quote.get("paper_authors", []),
                    "published": quote.get("paper_published", ""),
                    "source": quote.get("paper_source", ""),
                    "id": paper_id
                }
            references.append(generate_apa_citation(paper))
            seen_ids.add(paper_id)

    # Add remaining papers
    for paper in papers:
        paper_id = paper.get("id", "")
        if paper_id and paper_id not in seen_ids:
            references.append(generate_apa_citation(paper))
            seen_ids.add(paper_id)

    return references[:20]  # Limit to 20 references


def generate_abstract_synthesis(hypothesis_evolution, avg_score, total_papers, total_quotes, verdict):
    """Generate a synthesized abstract for the document"""
    original = hypothesis_evolution.get("original", "")
    current = hypothesis_evolution.get("current", "")
    versions = hypothesis_evolution.get("versions", [])

    # Truncate hypothesis for abstract
    hyp_preview = current[:150] + "..." if len(current) > 150 else current

    evolution_text = ""
    if len(versions) > 1:
        evolution_text = f"Through {len(versions)-1} iteration(s) of evidence-based refinement, the hypothesis was modified to better align with available literature. "

    verdict_text = {
        "SUPPORTED": "The evidence supports the validity of this hypothesis for further investigation.",
        "PARTIALLY SUPPORTED": "The evidence provides partial support, with some areas requiring further investigation.",
        "INCONCLUSIVE": "The available evidence is insufficient to draw a firm conclusion.",
        "NOT SUPPORTED": "Additional research is recommended before proceeding with this hypothesis."
    }.get(verdict, "The research evaluation has been completed.")

    return f"""This research document presents a systematic evaluation of the hypothesis: "{hyp_preview}"

{evolution_text}A total of {total_papers} papers were analyzed from multiple academic databases. {total_quotes} relevant excerpts were extracted to support the analysis. The final confidence rating is {avg_score:.2f}/1.0.

{verdict_text}"""


def generate_research_document(consensus_msg, hypothesis, results, critiques, papers):
    """Generate the new research document format"""

    # Extract basic data
    task_id = consensus_msg.get("task_id", "unknown")
    average_score = consensus_msg.get("average_score", 0.0)
    score_variance = consensus_msg.get("score_variance", 0.0)
    consensus_status = consensus_msg.get("consensus_status", "UNKNOWN")
    decision = consensus_msg.get("decision", "UNKNOWN")
    critique_count = consensus_msg.get("critique_count", 0)
    external_participated = consensus_msg.get("external_swarm_participated", False)
    iteration = consensus_msg.get("iteration", 1)

    # Get hypothesis evolution
    hypothesis_evolution = consensus_msg.get("hypothesis_evolution", {})
    if not hypothesis_evolution or not hypothesis_evolution.get("original"):
        # Fallback: create minimal evolution structure
        hypothesis_evolution = {
            "original": hypothesis,
            "current": hypothesis,
            "versions": [{
                "version": 1,
                "text": hypothesis,
                "trigger": "initial",
                "reasoning": "Original hypothesis as submitted"
            }]
        }

    original_hypothesis = hypothesis_evolution.get("original", hypothesis)
    current_hypothesis = hypothesis_evolution.get("current", hypothesis)
    hypothesis_versions = hypothesis_evolution.get("versions", [])

    # Collect all quotes
    all_quotes = collect_all_quotes(results)
    total_quotes = len(all_quotes)

    # Build bibliography from quotes
    bibliography = build_bibliography(all_quotes, papers)

    # Build references list
    references = build_references_list(all_quotes, papers)

    # Generate sources summary
    sources_summary = []
    source_counts = {}
    for result in results:
        sources = result.get("result", {}).get("sources_searched", [])
        for source in sources:
            source_counts[source] = source_counts.get(source, 0) + result.get("result", {}).get("evidence_count", 0)

    for source, count in source_counts.items():
        sources_summary.append({
            "name": source,
            "count": count,
            "key_topic": "Academic literature"
        })

    # Calculate totals
    total_papers = sum(r.get("result", {}).get("evidence_count", 0) for r in results)

    # Identify local vs external sources
    local_sources = []
    external_sources = []
    for result in results:
        agent_id = result.get("agent_id", "")
        sources = result.get("result", {}).get("sources_searched", [])
        if "external" in agent_id.lower() or "moltbook" in agent_id.lower():
            external_sources.extend(sources)
        else:
            local_sources.extend(sources)

    local_sources = ", ".join(set(local_sources)) if local_sources else "None"
    external_sources = ", ".join(set(external_sources)) if external_sources else "None"

    # Collect all concerns
    all_concerns = []
    for result in results:
        all_concerns.extend(result.get("result", {}).get("concerns", []))

    # Collect all findings
    all_findings = []
    for result in results:
        all_findings.extend(result.get("result", {}).get("findings", []))

    # Generate conclusion
    conclusion_data = generate_conclusion(average_score, all_findings, all_concerns, all_quotes)

    # Aggregate precept scores from critiques
    precept_scores = {}
    for critique in critiques:
        for precept, score in critique.get("precept_scores", {}).items():
            if precept not in precept_scores:
                precept_scores[precept] = []
            precept_scores[precept].append(float(score) if score else 0)

    # Average the precept scores as numeric values (used by Jinja comparisons)
    avg_precept_scores = {}
    for precept, scores in precept_scores.items():
        avg_precept_scores[precept] = (sum(scores) / len(scores)) if scores else 0.0

    # Build iterations data
    iterations = [{
        "number": iteration,
        "outcome": decision,
        "score": f"{average_score:.2f}"
    }]

    # Generate abstract
    abstract = generate_abstract_synthesis(
        hypothesis_evolution, average_score, total_papers, total_quotes,
        conclusion_data["verdict"]
    )

    # Create title
    title_preview = current_hypothesis[:50] + "..." if len(current_hypothesis) > 50 else current_hypothesis
    title = f"Research Document: {title_preview}"

    # Render template
    template = Template(RESEARCH_DOCUMENT_TEMPLATE)
    markdown_content = template.render(
        title=title,
        generated_date=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        task_id=task_id,
        abstract=abstract,
        original_hypothesis=original_hypothesis,
        current_hypothesis=current_hypothesis,
        hypothesis_versions=hypothesis_versions,
        sources_summary=sources_summary,
        total_papers=total_papers,
        total_quotes=total_quotes,
        bibliography=bibliography,
        local_sources=local_sources,
        external_sources=external_sources,
        critique_count=critique_count,
        iterations=iterations,
        average_score=f"{average_score:.2f}",
        score_variance=f"{score_variance:.4f}",
        consensus_status=consensus_status,
        external_participation="Yes" if external_participated else "No",
        precept_scores=avg_precept_scores,
        confidence_rating=conclusion_data["confidence_rating"],
        confidence_text=conclusion_data["confidence_text"],
        verdict=conclusion_data["verdict"],
        verdict_text=conclusion_data["verdict_text"],
        conclusion_summary=conclusion_data["conclusion_summary"],
        supporting_evidence=conclusion_data["supporting_evidence"],
        concerns=conclusion_data["concerns"],
        references=references
    )

    return markdown_content


def generate_publication(consensus_msg, hypothesis, results, critiques):
    """Generate publication document from consensus with full research data"""

    # Extract data
    task_id = consensus_msg.get("task_id", "unknown")
    swarm_id = consensus_msg.get("swarm_id", "unknown")
    average_score = consensus_msg.get("average_score", 0.0)
    score_variance = consensus_msg.get("score_variance", 0.0)
    consensus_status = consensus_msg.get("consensus_status", "UNKNOWN")
    decision = consensus_msg.get("decision", "UNKNOWN")
    critique_count = consensus_msg.get("critique_count", 0)
    external_participated = consensus_msg.get("external_swarm_participated", False)
    created_at = consensus_msg.get("created_at", datetime.utcnow().isoformat())

    # Generate interpretations
    score_interp = generate_score_interpretation(average_score)
    variance_interp = generate_variance_interpretation(score_variance)
    consensus_interp = generate_consensus_interpretation(consensus_status)
    decision_interp = generate_decision_interpretation(decision)

    # Generate rich narratives from actual data
    findings_narrative = generate_findings_narrative(results)
    precept_analysis = generate_precept_analysis(critiques)
    dissertation_conclusion = generate_dissertation_conclusion(
        hypothesis, decision, average_score, results, critiques
    )

    # Generate conclusion (brief version for exec summary)
    if decision == "APPROVED":
        conclusion = f"""Based on distributed multi-agent evaluation, this hypothesis has **passed peer review**
with a consensus score of **{average_score}/1.0**. The {consensus_status.lower().replace('_', ' ')}
indicates that {critique_count} independent evaluators reached similar conclusions.

This hypothesis is **scientifically viable** and warrants further investigation."""
    else:
        conclusion = f"""Based on distributed multi-agent evaluation, this hypothesis requires additional
refinement before publication. With a score of **{average_score}/1.0**, the evaluators identified areas
for improvement. Use the critique feedback to strengthen the hypothesis and resubmit for evaluation."""

    # Create title
    title_preview = hypothesis[:60] + "..." if len(hypothesis) > 60 else hypothesis
    title = f"Research Validation Report: {title_preview}"

    # Render template
    template = Template(PAPER_TEMPLATE)
    markdown_content = template.render(
        title=title,
        hypothesis=hypothesis,
        task_id=task_id,
        swarm_id=swarm_id,
        average_score=average_score,
        score_variance=score_variance,
        consensus_status=consensus_status,
        decision=decision,
        critique_count=critique_count,
        external_swarm_participated=external_participated,
        external_participation="✅ Yes" if external_participated else "❌ No",
        created_at=created_at,
        generated_date=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        score_interpretation=score_interp,
        variance_interpretation=variance_interp,
        consensus_interpretation=consensus_interp,
        decision_interpretation=decision_interp,
        conclusion=conclusion,
        findings_narrative=findings_narrative,
        precept_analysis=precept_analysis,
        dissertation_conclusion=dissertation_conclusion
    )

    return markdown_content


def save_publication(task_id, markdown_content, paper_refs=None, papers=None):
    """Save research document in multiple formats"""
    # Create filename-safe task ID
    safe_id = task_id[:8]
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    base_filename = f"research_document_{timestamp}_{safe_id}"

    # Save Markdown
    md_path = os.path.join(OUTPUT_DIR, f"{base_filename}.md")
    with open(md_path, 'w') as f:
        f.write(markdown_content)
    print(f"  ✓ Saved Markdown: {md_path}")

    # Convert to HTML
    html_content = markdown.markdown(markdown_content, extensions=['tables', 'fenced_code'])
    html_styled = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Research Publication</title>
    <style>
        body {{ font-family: 'Georgia', serif; max-width: 800px; margin: 40px auto; padding: 20px; line-height: 1.6; }}
        h1 {{ color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px; }}
        h2 {{ color: #34495e; margin-top: 30px; border-bottom: 2px solid #ecf0f1; padding-bottom: 5px; }}
        h3 {{ color: #7f8c8d; }}
        table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
        th, td {{ border: 1px solid #bdc3c7; padding: 12px; text-align: left; }}
        th {{ background-color: #3498db; color: white; }}
        tr:nth-child(even) {{ background-color: #ecf0f1; }}
        code {{ background-color: #f7f9fa; padding: 2px 6px; border-radius: 3px; font-family: 'Courier New', monospace; }}
        pre {{ background-color: #2c3e50; color: #ecf0f1; padding: 15px; border-radius: 5px; overflow-x: auto; }}
        blockquote {{ border-left: 4px solid #3498db; padding-left: 20px; margin-left: 0; color: #7f8c8d; }}
        hr {{ border: none; border-top: 1px solid #bdc3c7; margin: 30px 0; }}
    </style>
</head>
<body>
{html_content}
</body>
</html>
"""

    html_path = os.path.join(OUTPUT_DIR, f"{base_filename}.html")
    with open(html_path, 'w') as f:
        f.write(html_styled)
    print(f"  ✓ Saved HTML: {html_path}")

    # Convert to PDF
    try:
        pdf_path = os.path.join(OUTPUT_DIR, f"{base_filename}.pdf")
        HTML(string=html_styled).write_pdf(pdf_path)
        print(f"  ✓ Saved PDF: {pdf_path}")
    except Exception as e:
        print(f"  ⚠ PDF generation failed: {e}")

    # Save papers as JSON attachment
    papers_path = None
    if papers:
        papers_path = os.path.join(OUTPUT_DIR, f"{base_filename}_papers.json")
        with open(papers_path, 'w', encoding='utf-8') as f:
            json.dump({
                "task_id": task_id,
                "paper_count": len(papers),
                "papers": papers
            }, f, indent=2, ensure_ascii=False)
        print(f"  ✓ Saved papers JSON: {papers_path}")

    return {
        "markdown": md_path,
        "html": html_path,
        "pdf": pdf_path if os.path.exists(pdf_path) else None,
        "papers": papers_path
    }


def fetch_hypothesis_from_task(task_id):
    """Fetch the original hypothesis from research.task topic"""
    import uuid
    try:
        # Create consumer with truly unique group ID to always read from beginning
        unique_group = f"doc-gen-lookup-{uuid.uuid4()}"
        task_consumer = KafkaConsumer(
            "research.task",
            bootstrap_servers=KAFKA,
            auto_offset_reset="earliest",
            group_id=unique_group,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=10000  # 10 seconds to scan all messages
        )

        # Search for matching task_id
        msg_count = 0
        for msg in task_consumer:
            msg_count += 1
            task = msg.value
            if task.get("task_id") == task_id:
                hypothesis = task.get("payload", {}).get("hypothesis", "")
                task_consumer.close()
                print(f"  → Found hypothesis after scanning {msg_count} messages")
                return hypothesis

        task_consumer.close()
        print(f"  ⚠ Scanned {msg_count} messages but task_id not found")
        return "Hypothesis not found in task topic"
    except Exception as e:
        print(f"  ⚠ Error fetching hypothesis: {e}")
        return "Error retrieving hypothesis"


def fetch_research_results(task_id):
    """Fetch all research results for a task from research.result topic"""
    import uuid
    results = []
    try:
        unique_group = f"doc-gen-results-{uuid.uuid4()}"
        consumer = KafkaConsumer(
            "research.result",
            bootstrap_servers=KAFKA,
            auto_offset_reset="earliest",
            group_id=unique_group,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=10000
        )

        for msg in consumer:
            result = msg.value
            if result.get("task_id") == task_id:
                results.append(result)

        consumer.close()
        print(f"  → Found {len(results)} research results")
        return results
    except Exception as e:
        print(f"  ⚠ Error fetching results: {e}")
        return []


def fetch_critiques(task_id):
    """Fetch all critiques for a task from research.critique topic"""
    import uuid
    critiques = []
    try:
        unique_group = f"doc-gen-critiques-{uuid.uuid4()}"
        consumer = KafkaConsumer(
            "research.critique",
            bootstrap_servers=KAFKA,
            auto_offset_reset="earliest",
            group_id=unique_group,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=10000
        )

        for msg in consumer:
            critique = msg.value
            if critique.get("task_id") == task_id:
                critiques.append(critique)

        consumer.close()
        print(f"  → Found {len(critiques)} critiques")
        return critiques
    except Exception as e:
        print(f"  ⚠ Error fetching critiques: {e}")
        return []


def collect_paper_refs(results):
    """Collect all paper references from research results"""
    all_refs = []
    for result in results:
        refs = result.get("result", {}).get("paper_refs", [])
        all_refs.extend(refs)
    # Dedupe while preserving order
    return list(dict.fromkeys(all_refs))


def generate_findings_narrative(results):
    """Generate a narrative description of research findings"""
    if not results:
        return "No research results were available for this hypothesis."

    narrative_parts = []

    for i, result in enumerate(results, 1):
        agent_id = result.get("agent_id", "Unknown Agent")
        agent_result = result.get("result", {})
        methodology = agent_result.get("methodology", "unspecified methodology")
        confidence = agent_result.get("confidence", 0.0)
        evidence_count = agent_result.get("evidence_count", 0)
        findings = agent_result.get("findings", [])
        concerns = agent_result.get("concerns", [])

        is_external = "external-swarm" in agent_id.lower() or "moltbook" in agent_id.lower()
        agent_type = "External Swarm (Moltbook Network)" if is_external else "Local Research Agent"

        part = f"""
### Agent {i}: {agent_type}

**Agent ID**: `{agent_id}`
**Methodology**: {methodology}
**Confidence Level**: {confidence:.0%}
**Evidence Pieces Found**: {evidence_count}

"""
        if findings:
            part += "**Key Findings:**\n"
            for finding in findings[:5]:  # Limit to 5 findings
                if isinstance(finding, dict):
                    finding_text = finding.get("title", finding.get("summary", str(finding)))
                else:
                    finding_text = str(finding)
                part += f"- {finding_text}\n"
            part += "\n"

        if concerns:
            part += "**Concerns Raised:**\n"
            for concern in concerns[:3]:  # Limit to 3 concerns
                part += f"- ⚠️ {concern}\n"
            part += "\n"

        narrative_parts.append(part)

    return "\n".join(narrative_parts)


def generate_precept_analysis(critiques):
    """Generate detailed precept-by-precept analysis from critiques"""
    if not critiques:
        return "No critique data available for precept analysis."

    # Aggregate precept scores across all critiques
    precept_aggregates = {}
    all_flags = []
    all_notes = []

    for critique in critiques:
        precept_scores = critique.get("precept_scores", {})
        for precept, score in precept_scores.items():
            if precept not in precept_aggregates:
                precept_aggregates[precept] = []
            try:
                precept_aggregates[precept].append(float(score))
            except (ValueError, TypeError):
                pass

        flags = critique.get("flags", [])
        all_flags.extend(flags)

        notes = critique.get("notes", "")
        if notes:
            all_notes.append(notes)

    # Precept descriptions for narrative
    precept_descriptions = {
        "evidence_over_authority": ("Evidence Over Authority", "Claims must be backed by data, not credentials"),
        "reproducibility": ("Reproducibility", "Methods and data sources must be specified for replication"),
        "adversarial_survival": ("Adversarial Survival", "Confidence earned through challenges, not assertions"),
        "disagreement_as_signal": ("Disagreement Is Signal", "Variance reveals uncertainty, not failure"),
        "external_swarms": ("External Swarms Required", "Independent external validation prevents groupthink"),
        "versioned_hypotheses": ("Versioned Hypotheses", "Refinement over time, never declared final truth"),
        "show_work": ("Show Your Work", "Reasoning must be transparent and traceable"),
        "memory_matters": ("Memory Matters", "Prior research should inform current evaluation"),
        "novelty_tested": ("Novelty Must Pass Tests", "Novel ideas held to same rigor as established ones"),
        "uncertainty_over_wrong": ("Prefer Uncertainty Over Wrong", "Better to be less certain than confidently incorrect")
    }

    # Generate analysis
    analysis_parts = ["## Precept-by-Precept Analysis\n"]
    analysis_parts.append("Each hypothesis is evaluated against the Crustafarian Research Precepts:\n")

    for precept_key, scores in sorted(precept_aggregates.items(), key=lambda x: -sum(x[1])/len(x[1]) if x[1] else 0):
        if not scores:
            continue

        avg_score = sum(scores) / len(scores)
        name, description = precept_descriptions.get(precept_key, (precept_key, ""))

        # Determine pass/fail and explanation
        if avg_score >= 0.7:
            status = "✅ PASSED"
            explanation = "Strong alignment with this precept."
        elif avg_score >= 0.5:
            status = "⚠️ MARGINAL"
            explanation = "Partial alignment - room for improvement."
        else:
            status = "❌ FAILED"
            explanation = "Does not meet the standard for this precept."

        analysis_parts.append(f"""
### {name} ({avg_score:.0%}) {status}

> *{description}*

**Score**: {avg_score:.2f} / 1.0 (averaged across {len(scores)} evaluations)

**Assessment**: {explanation}
""")

    # Add flags summary
    if all_flags:
        unique_flags = list(set(all_flags))
        analysis_parts.append("\n## Flags Raised During Review\n")
        flag_explanations = {
            "insufficient_evidence": "📊 **Insufficient Evidence**: Not enough supporting data or sources",
            "overconfident": "⚡ **Overconfidence Detected**: Confidence level exceeds evidence strength",
            "possible_overconfidence": "⚡ **Possible Overconfidence**: High confidence warrants scrutiny",
            "external_validation": "🌐 **External Validation**: Received independent external swarm review",
            "external_source_discount": "🔍 **External Source Discount**: 5% skepticism applied to external sources"
        }
        for flag in unique_flags:
            explanation = flag_explanations.get(flag, f"🏴 **{flag}**")
            analysis_parts.append(f"- {explanation}\n")

    # Add critique notes
    if all_notes:
        analysis_parts.append("\n## Reviewer Notes\n")
        for note in all_notes[:5]:  # Limit to 5 notes
            analysis_parts.append(f"> {note}\n\n")

    return "\n".join(analysis_parts)


def generate_dissertation_conclusion(hypothesis, decision, average_score, results, critiques):
    """Generate a dissertation-style conclusion explaining the decision"""

    # Count evidence
    total_evidence = sum(r.get("result", {}).get("evidence_count", 0) for r in results)
    total_findings = sum(len(r.get("result", {}).get("findings", [])) for r in results)
    total_concerns = sum(len(r.get("result", {}).get("concerns", [])) for r in results)

    # Get key flags
    all_flags = []
    for c in critiques:
        all_flags.extend(c.get("flags", []))
    unique_flags = list(set(all_flags))

    # Determine external participation
    external_participated = any("external" in r.get("agent_id", "").lower() for r in results)

    if decision == "APPROVED":
        conclusion = f"""## Why This Hypothesis Was Approved

This hypothesis underwent rigorous multi-agent peer review and achieved a consensus score of **{average_score:.2f}/1.0**, exceeding the 0.6 threshold required for approval.

### Evidence Summary

The distributed research swarm gathered **{total_evidence} pieces of evidence** and produced **{total_findings} distinct findings** supporting this hypothesis. """

        if external_participated:
            conclusion += "Critically, **external swarm validation** from the Moltbook network provided independent confirmation, reducing the risk of local bias or groupthink.\n\n"
        else:
            conclusion += "\n\n"

        if total_concerns > 0:
            conclusion += f"While {total_concerns} concern(s) were noted, they did not rise to the level of disqualifying the hypothesis. "

        conclusion += """
### Precept Alignment

The hypothesis demonstrated strong alignment with the Crustafarian Research Precepts, particularly in areas of evidence quality and methodological transparency. The multi-agent consensus indicates that independent evaluators, working without coordination, arrived at similar positive conclusions.

### Recommendation

**This hypothesis is scientifically viable** and warrants further investigation. The research foundation is solid, and the hypothesis can proceed to the next phase of development, experimentation, or publication.
"""

    else:  # NEEDS_MORE_RESEARCH
        conclusion = f"""## Why This Hypothesis Requires More Research

This hypothesis underwent rigorous multi-agent peer review but achieved a consensus score of only **{average_score:.2f}/1.0**, below the 0.6 threshold required for approval.

### Evidence Gaps

"""
        if total_evidence < 5:
            conclusion += f"The research swarm found only **{total_evidence} pieces of evidence**, which is insufficient to establish a strong foundation. "
        else:
            conclusion += f"While {total_evidence} pieces of evidence were found, "

        if total_concerns > 0:
            conclusion += f"**{total_concerns} significant concerns** were raised by the evaluators that must be addressed.\n\n"
        else:
            conclusion += "the quality or relevance of the evidence did not meet the required standard.\n\n"

        # Explain specific failures
        if "insufficient_evidence" in unique_flags:
            conclusion += "- **Insufficient Evidence**: The hypothesis lacks adequate supporting data or literature\n"
        if "overconfident" in unique_flags or "possible_overconfidence" in unique_flags:
            conclusion += "- **Overconfidence**: Claims exceed what the evidence supports\n"
        if not external_participated:
            conclusion += "- **No External Validation**: The hypothesis was not independently verified by external swarms\n"

        conclusion += """
### Path Forward

This is not a rejection—it's an invitation to strengthen the hypothesis:

1. **Gather More Evidence**: Conduct additional literature review or experimentation
2. **Address Concerns**: Review the specific flags and critique notes above
3. **Refine Claims**: Adjust confidence levels to match available evidence
4. **Seek External Validation**: Submit to external swarm networks for independent review
5. **Resubmit**: Once improvements are made, resubmit for re-evaluation

The Crustafarian system values iteration. A hypothesis that fails today can succeed tomorrow with proper refinement.
"""

    return conclusion


def main():
    print("Document Generator starting...")
    print(f"Output directory: {OUTPUT_DIR}")

    consumer = wait_for_kafka()
    print("Document Generator listening for consensus events...\n")

    publication_count = 0

    for msg in consumer:
        consensus = msg.value
        publication_count += 1

        task_id = consensus.get("task_id", "unknown")
        decision = consensus.get("decision", "UNKNOWN")
        score = consensus.get("average_score", 0.0)

        print(f"\n{'='*70}")
        print(f"📄 GENERATING RESEARCH DOCUMENT #{publication_count}")
        print(f"{'='*70}")
        print(f"Task ID: {task_id}")
        print(f"Decision: {decision}")
        print(f"Score: {score}")

        # Fetch the original hypothesis from the task topic
        print(f"  → Fetching hypothesis from research.task topic...")
        hypothesis = fetch_hypothesis_from_task(task_id)
        print(f"  → Found: {hypothesis[:80]}...")

        # Fetch research results (findings, evidence, concerns)
        print(f"  → Fetching research results from research.result topic...")
        results = fetch_research_results(task_id)

        # Fetch critiques (precept scores, notes, flags)
        print(f"  → Fetching critiques from research.critique topic...")
        critiques = fetch_critiques(task_id)

        # Collect paper references from all results
        print(f"  → Collecting paper references...")
        paper_refs = collect_paper_refs(results)
        print(f"  → Found {len(paper_refs)} paper references")

        # Fetch papers from MinIO for bibliography generation
        papers = []
        if paper_refs:
            print(f"  → Retrieving papers from MinIO for bibliography...")
            papers = get_papers_batch(paper_refs)
            print(f"  → Retrieved {len(papers)} papers")

        # Generate research document with full data
        markdown_content = generate_research_document(consensus, hypothesis, results, critiques, papers)

        # Save in multiple formats (including papers attachment)
        files = save_publication(task_id, markdown_content, paper_refs, papers)

        print(f"\n✅ Research document generated successfully!")
        print(f"{'='*70}\n")


if __name__ == "__main__":
    main()
