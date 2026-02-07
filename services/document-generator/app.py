import os
import json
import time
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from jinja2 import Template
import markdown
from weasyprint import HTML, CSS

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
OUTPUT_DIR = "/app/publications"

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)


def wait_for_kafka(max_retries=30, delay=2):
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


# Markdown template for research paper
PAPER_TEMPLATE = """
# {{ title }}

**Distributed Multi-Agent Research Validation**
**Generated**: {{ generated_date }}
**Consensus ID**: `{{ task_id }}`
**Swarm ID**: `{{ swarm_id }}`

---

## Executive Summary

This hypothesis was evaluated by a distributed multi-agent research system comprising:
- **Local Research Agents**: arXiv literature search and analysis
- **External Swarm Network**: Independent validation (Moltbook Crustafarian Council)
- **Skeptical Peer Review**: Critical evaluation with bias correction
- **Consensus Algorithm**: Democratic aggregation of independent assessments

**Final Decision**: **{{ decision }}**
**Consensus Score**: {{ average_score }} / 1.0
**Consensus Strength**: {{ consensus_status }}
**Score Variance**: {{ score_variance }} (lower = stronger agreement)

---

## Hypothesis

{{ hypothesis }}

---

## Research Methodology

### Multi-Agent Validation Process

1. **Task Publication** (`research.task` topic)
   - Hypothesis submitted to distributed research swarm
   - Timestamp: {{ created_at }}
   - Assigned unique task ID for full traceability

2. **Parallel Research Execution**
   - **Local Agents**: arXiv literature search, LLM analysis
   - **External Swarm**: Independent validation via Moltbook network
   - Both agents worked independently to avoid bias

3. **Skeptical Peer Review** (`research.critique` topic)
   - {{ critique_count }} independent evaluations performed
   - External sources received 5% skepticism discount
   - Critiques evaluated methodology, evidence, and rigor

4. **Consensus Building** (`research.consensus` topic)
   - Democratic aggregation of all critiques
   - Statistical analysis of score distribution
   - Consensus threshold: 2+ independent evaluations

---

## Evaluation Results

### Consensus Metrics

| Metric | Value | Interpretation |
|--------|-------|----------------|
| **Average Score** | {{ average_score }} | {{ score_interpretation }} |
| **Score Variance** | {{ score_variance }} | {{ variance_interpretation }} |
| **Consensus Status** | {{ consensus_status }} | {{ consensus_interpretation }} |
| **Decision** | {{ decision }} | {{ decision_interpretation }} |
| **External Swarm** | {{ external_participation }} | Independent validation performed |

### Score Interpretation Guide

- **0.85 - 1.00**: Exceptional - Strong literature support, highly novel
- **0.70 - 0.84**: Strong - Good evidence, worth significant investment
- **0.60 - 0.69**: Promising - Adequate foundation, needs refinement
- **0.40 - 0.59**: Needs Work - Insufficient evidence or methodology issues
- **0.00 - 0.39**: Not Viable - Contradicts evidence or lacks foundation

---

## Research Findings

{{ findings_narrative }}

---

{{ precept_analysis }}

---

{{ dissertation_conclusion }}

---

## Reproducibility & Transparency

### Event Sourcing Trail
This research validation is **fully reproducible**. The complete event log is available:

- **Task ID**: `{{ task_id }}`
- **Swarm ID**: `{{ swarm_id }}`
- **Kafka Topics**: `research.task`, `research.result`, `research.critique`, `research.consensus`

Any researcher can replay the exact sequence of events that led to this consensus decision by querying the Kafka event log.

### Peer Review Transparency
Unlike traditional peer review (anonymous, opaque), this validation provides:
- ✅ Complete audit trail of all evaluations
- ✅ Quantitative scores from each reviewer
- ✅ Specific critique notes and concerns
- ✅ Timestamp of every decision point
- ✅ Bias correction methodology (5% external source discount)

---

## Conclusion

{{ conclusion }}

### Recommended Next Steps

{% if decision == "APPROVED" %}
**This hypothesis has been approved for publication and further research.**

Recommended actions:
1. **Literature Review**: Conduct comprehensive review of related arXiv papers
2. **Experimental Design**: Develop testable predictions and observational tests
3. **Peer Collaboration**: Share findings with relevant research communities
4. **Funding Applications**: Use this consensus as supporting evidence
5. **Refinement**: Continue iterating on hypothesis based on feedback
{% else %}
**This hypothesis requires additional research before publication.**

Recommended actions:
1. **Address Concerns**: Review critique notes and address identified issues
2. **Strengthen Evidence**: Find additional supporting literature or data
3. **Refine Hypothesis**: Make predictions more specific and testable
4. **Resubmit**: Submit refined version for re-evaluation
5. **Iterate**: Use this feedback loop to improve research quality
{% endif %}

---

## Appendix: Technical Details

### System Architecture
- **Event Backbone**: Apache Kafka (event sourcing)
- **Research Agents**: Python microservices with arXiv API integration
- **LLM Analysis**: Ollama (phi3:mini, 3.8B parameters, local inference)
- **Peer Review**: Skeptical critic with bias correction
- **External Validation**: Moltbook distributed swarm network

### Consensus Algorithm
```
consensus_status =
  if score_variance < 0.05: "STRONG_CONSENSUS"
  elif score_variance < 0.15: "MODERATE_CONSENSUS"
  else: "DIVERGENT_OPINIONS"

decision =
  if average_score > 0.6: "APPROVED"
  else: "NEEDS_MORE_RESEARCH"
```

### Bias Mitigation
- External sources receive 5% skepticism discount
- Multiple independent evaluators required (minimum 2)
- Distributed consensus prevents single-point-of-failure
- Full transparency enables bias detection

---

**Generated by Research Stack v1.0**
*Distributed Multi-Agent Research Validation System*
*Event-Sourced • Transparent • Reproducible*

Repository: https://github.com/yourusername/research-stack
License: MIT
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


def save_publication(task_id, markdown_content):
    """Save publication in multiple formats"""
    # Create filename-safe task ID
    safe_id = task_id[:8]
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    base_filename = f"publication_{timestamp}_{safe_id}"

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

    return {
        "markdown": md_path,
        "html": html_path,
        "pdf": pdf_path if os.path.exists(pdf_path) else None
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
        print(f"📄 GENERATING PUBLICATION #{publication_count}")
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

        # Generate publication with full data
        markdown_content = generate_publication(consensus, hypothesis, results, critiques)

        # Save in multiple formats
        files = save_publication(task_id, markdown_content)

        print(f"\n✅ Publication generated successfully!")
        print(f"{'='*70}\n")


if __name__ == "__main__":
    main()
