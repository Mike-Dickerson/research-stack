# Research Swarm Stack — Current State + Implementation Start

## Purpose

Build a local-first, Kafka-backed, multi-agent research system where:
- Many agents can work the same task
- Agents produce results
- Critics evaluate results
- All activity is event-driven through Kafka
- External research network (Moltbook) will later exchange research problems and results

This is research infrastructure, not a chatbot or RAG system.

---

## Current Environment

Host:
Windows + Docker Desktop (WSL2)

Hardware:
52GB RAM  
24 Core Xeon  

No meaningful resource limits.

---

## Running Infrastructure (Already Working)

Kafka Stack:
- confluentinc/cp-zookeeper:7.5.3
- confluentinc/cp-kafka:7.5.3
- provectuslabs/kafka-ui:v0.7.2

Database:
- postgres:16

Object Storage:
- minio/minio:latest

All containers currently start and stay running.

---

## Data Storage Model

Bind mounts preferred.

Base folder:
G:\research-stack\data\

Subfolders:
kafka  
zookeeper  
postgres  
minio  

---

## Existing Service Containers

These exist but may only contain stub code:

research-orchestrator  
research-agent-runner  
research-critic  
moltbook-bridge  

Language target: Python

---

## Core Architecture Rules

Kafka = Event history and inter-service communication  
Postgres = Structured queryable state  
MinIO = Files / artifacts  

Kafka is the source of truth for research activity history.

---

## Required Kafka Topics (Implement First)

research.task  
research.result  
research.critique  
research.consensus  

---

## Message Shapes (Initial)

### research.task
Produced by orchestrator, consumed by agents.

{
  "task_id": "uuid",
  "swarm_id": "uuid",
  "task_type": "string",
  "payload": {},
  "created_at": "timestamp"
}

---

### research.result
Produced by agent, consumed by critic and orchestrator.

{
  "task_id": "uuid",
  "swarm_id": "uuid",
  "agent_id": "string",
  "result": {},
  "confidence": 0.0,
  "created_at": "timestamp"
}

---

### research.critique
Produced by critic, consumed by orchestrator.

{
  "task_id": "uuid",
  "swarm_id": "uuid",
  "critic_id": "string",
  "score": 0.0,
  "flags": [],
  "notes": "string",
  "created_at": "timestamp"
}

---

## First Implementation Goal

Create continuous event loop:

Orchestrator → publishes research.task  
Agent → consumes task → publishes research.result  
Critic → consumes result → publishes research.critique  

All messages visible in Kafka UI.

System runs continuously without crashing.

---

## Swarm Direction (Later — Not Required For First Code)

Eventually multiple agents will:
- Work same task simultaneously
- Share evidence
- Challenge each other
- Propose alternate hypotheses

Each research run will use a shared swarm_id.

---

## Moltbook Bridge:

Core architectural component.
This service represents the connection to an external research swarm network.
The long-term role of Moltbook is to:
- Receive active research problems
- Run external agent debate and analysis
- Return alternative hypotheses
- Return supporting or contradicting evidence
- Return external critique signals

The local system must be designed assuming Moltbook participation is normal operation.
Initial implementation may use stub transport or mock external responses,
but message contracts and architecture must treat Moltbook as a first-class research participant.

---

## Container Responsibilities

Orchestrator:
Creates tasks and tracks research sessions.

Agent Runner:
Executes research tasks and produces results.

Critic:
Evaluates results and produces critique scoring.

Moltbook Bridge:
Future external communication only.

---

## Immediate Task For Implementation

Implement base pipeline loop only:

Task → Result → Critique → Persist via Kafka

Do NOT implement swarm behavior yet.  
Do NOT implement Moltbook yet.  
Do NOT build complex scoring yet.

Just make the loop reliable.

---

## Success Definition

System continuously produces:

Task → Result → Critique

Without manual intervention.


