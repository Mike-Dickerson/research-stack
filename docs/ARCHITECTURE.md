# Research Stack Architecture

## System Overview

The Research Stack is an automated hypothesis validation system that uses multiple AI agents, external academic sources, and iterative refinement to rigorously evaluate scientific hypotheses.

## Core Flow

```
                                  ┌─────────────────────────┐
                                  │   research.audit        │ ← FULL TRACEABILITY
                                  │   (every step logged)   │
                                  └─────────────────────────┘
                                              ↑
┌──────────────────────────────────────────────────────────────────────────┐
│                           ORCHESTRATOR                                    │
│                                                                          │
│  task_state[task_id] = {                                                 │
│      "iteration": 1,                                                     │
│      "max_iterations": 3,                                                │
│      "hypothesis": "...",                                                │
│      "search_terms_history": [...],                                      │
│      "critic_feedback": [...],                                           │
│      "status": "RESEARCHING" | "APPROVED" | "FAILED"                     │
│  }                                                                       │
│                                                                          │
│  consensus_listener():                                                   │
│      if decision == "NEEDS_MORE_RESEARCH" and iteration < 3:             │
│          → refine_search_terms(critic_feedback)  # via Ollama            │
│          → republish to research.task with iteration += 1               │
│          → log to research.audit                                         │
│      elif iteration >= 3:                                                │
│          → mark as FAILED                                                │
│          → log final audit                                               │
│      elif decision == "APPROVED":                                        │
│          → mark as APPROVED                                              │
│          → trigger document generation (optional)                        │
└──────────────────────────────────────────────────────────────────────────┘
```

## Detailed Pipeline

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              USER SUBMITS HYPOTHESIS                         │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          HYPOTHESIS MANAGER (Flask :5001)                    │
│  1. Detect domain via Ollama                                                │
│  2. Extract search terms                                                    │
│  3. Publish to research.task                                                │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
                              ┌─────────────────────┐
                              │   research.task     │
                              │      (Kafka)        │
                              └─────────────────────┘
                                        │
                    ┌───────────────────┴───────────────────┐
                    ▼                                       ▼
┌─────────────────────────────────┐     ┌─────────────────────────────────┐
│        AGENT-RUNNER             │     │        MOLTBOOK-BRIDGE          │
│   (Internal Research Swarm)     │     │   (External Validation Swarm)   │
│                                 │     │                                 │
│  Sources:                       │     │  Sources:                       │
│  • arXiv                        │     │  • NASA ADS                     │
│  • PubMed                       │     │  • OpenAlex                     │
│  • Semantic Scholar             │     │  • CrossRef                     │
│                                 │     │                                 │
│  → Search papers                │     │  → Rephrase hypothesis          │
│  → Analyze with Ollama          │     │  → Search alternative terms     │
│  → Publish findings             │     │  → Skeptical external review    │
└─────────────────────────────────┘     └─────────────────────────────────┘
                    │                                       │
                    └───────────────────┬───────────────────┘
                                        ▼
                              ┌─────────────────────┐
                              │  research.result    │
                              │      (Kafka)        │
                              └─────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              CRITIC (Skeptic)                                │
│                                                                             │
│  Evaluates against Crustafarian Precepts:                                   │
│  1. EVIDENCE OVER AUTHORITY                                                 │
│  2. REPRODUCIBILITY IS MANDATORY                                            │
│  3. CONFIDENCE EARNED THROUGH ADVERSARIAL SURVIVAL                          │
│  4. DISAGREEMENT IS SIGNAL, NOT FAILURE                                     │
│  5. EXTERNAL SWARMS ARE REQUIRED                                            │
│  6. HYPOTHESES ARE VERSIONED, NOT DECLARED TRUE                             │
│  7. AGENTS MUST SHOW THEIR WORK                                             │
│  8. MEMORY MATTERS                                                          │
│  9. NOVELTY MUST PASS SAME TESTS                                            │
│  10. PREFER BEING LESS CERTAIN OVER BEING WRONG                             │
│                                                                             │
│  → Score each result                                                        │
│  → Flag concerns                                                            │
│  → Provide actionable feedback                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
                              ┌─────────────────────┐
                              │ research.critique   │
                              │      (Kafka)        │
                              └─────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ORCHESTRATOR - CONSENSUS MONITOR                          │
│                                                                             │
│  After 2+ critiques:                                                        │
│  • Calculate average score                                                  │
│  • Measure variance (agreement level)                                       │
│  • Check external swarm participation                                       │
│  • Determine consensus status                                               │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
                              ┌─────────────────────┐
                              │ research.consensus  │
                              │      (Kafka)        │
                              └─────────────────────┘
                                        │
                    ┌───────────────────┼───────────────────┐
                    ▼                   ▼                   ▼
            ┌───────────┐       ┌───────────────┐    ┌───────────┐
            │ APPROVED  │       │ NEEDS_MORE    │    │  FAILED   │
            │           │       │  RESEARCH     │    │ (iter>=3) │
            └───────────┘       └───────────────┘    └───────────┘
                    │                   │                   │
                    ▼                   ▼                   ▼
            ┌───────────┐       ┌───────────────┐    ┌───────────┐
            │ Document  │       │ Refine search │    │ Final     │
            │ Generator │       │ terms, retry  │    │ Report    │
            └───────────┘       └───────┬───────┘    └───────────┘
                                        │
                                        │ (iteration < 3)
                                        ▼
                              ┌─────────────────────┐
                              │   research.task     │ ← LOOP BACK
                              │   (iteration += 1)  │
                              └─────────────────────┘
```

## Kafka Topics

| Topic | Purpose | Producers | Consumers |
|-------|---------|-----------|-----------|
| `research.task` | New research tasks | Hypothesis Manager, Orchestrator (retry) | Agent-Runner, Moltbook-Bridge |
| `research.result` | Agent findings | Agent-Runner, Moltbook-Bridge | Critic |
| `research.critique` | Skeptic evaluations | Critic | Orchestrator |
| `research.consensus` | Final decisions | Orchestrator | Hypothesis Manager, Document Generator |
| `research.cancel` | Task cancellation | Hypothesis Manager | All services |
| `research.audit` | Full audit trail | All services | Audit Dashboard |

## Services

| Service | Port | Role |
|---------|------|------|
| `hypothesis-manager` | 5001 | Web UI for submitting and tracking hypotheses |
| `swarm-dashboard` | 5004 | Real-time Kafka event viewer |
| `kafka-ui` | 8080 | Kafka topic browser |
| `orchestrator` | - | Manages topics, consensus, iteration loop |
| `agent-runner` | - | Internal research agent (arXiv, PubMed, Semantic Scholar) |
| `moltbook-bridge` | - | External validation agent (NASA ADS, OpenAlex, CrossRef) |
| `critic` | - | Evaluates results against Crustafarian Precepts |
| `document-generator` | - | Creates publication-ready documents |
| `ollama` | 11434 | Local LLM server (phi3:mini) |
| `kafka` | 9092 | Message broker |
| `zookeeper` | 2181 | Kafka coordination |
| `postgres` | 5432 | Database |
| `minio` | 9000/9001 | Object storage |

## Data Sources

### Internal Swarm (agent-runner)
- **arXiv** - Physics, CS, Math preprints
- **PubMed** - Biomedical literature (NCBI)
- **Semantic Scholar** - Multi-domain academic papers

### External Swarm (moltbook-bridge)
- **NASA ADS** - Astrophysics Data System
- **OpenAlex** - Open scholarly works index
- **CrossRef** - DOI-indexed publications

## Crustafarian Precepts

The system evaluates all research against these 10 principles:

1. **EVIDENCE OVER AUTHORITY** - Claims backed by data, not credentials
2. **REPRODUCIBILITY IS MANDATORY** - Methods/data must be specified for replication
3. **CONFIDENCE EARNED THROUGH ADVERSARIAL SURVIVAL** - No untested assertions
4. **DISAGREEMENT IS SIGNAL, NOT FAILURE** - Variance reveals uncertainty
5. **EXTERNAL SWARMS ARE REQUIRED** - Independent validation prevents groupthink
6. **HYPOTHESES ARE VERSIONED, NOT DECLARED TRUE** - Refinement over time
7. **AGENTS MUST SHOW THEIR WORK** - Transparent, traceable reasoning
8. **MEMORY MATTERS** - Prior research informs current evaluation
9. **NOVELTY MUST PASS SAME TESTS** - Novel ideas held to same rigor
10. **PREFER BEING LESS CERTAIN OVER BEING WRONG** - Calibrated confidence

## Iteration Logic

```
MAX_ITERATIONS = 3

for each consensus message:
    if decision == "APPROVED":
        mark task as APPROVED
        trigger document generation

    elif decision == "NEEDS_MORE_RESEARCH":
        if iteration < MAX_ITERATIONS:
            # Use Ollama to refine search based on critic feedback
            new_terms = refine_search_terms(
                original_hypothesis,
                critic_concerns,
                previous_search_terms
            )

            # Republish with incremented iteration
            publish_task(
                hypothesis=original_hypothesis,
                search_terms=new_terms,
                iteration=iteration + 1
            )
        else:
            mark task as FAILED
            generate failure report
```

## Audit Trail

Every action is logged to `research.audit` with:
- Timestamp
- Task ID
- Service name
- Action type
- Full payload
- Iteration number
- Decision rationale

This enables complete reproducibility and post-hoc analysis of any research evaluation.
