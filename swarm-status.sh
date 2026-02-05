#!/bin/bash
# Research Stack - Swarm Status Dashboard

echo "=================================="
echo "🧬 RESEARCH SWARM STATUS"
echo "=================================="
echo ""

# Container health
echo "📦 CONTAINER STATUS:"
docker ps --format "  {{.Names}}: {{.Status}}" | grep -E "research|ollama|kafka-ui" | sed 's/^/  /'
echo ""

# Get latest activity from each service
echo "🤖 AGENT ACTIVITY (last 3 lines):"
echo ""

echo "  📋 Orchestrator:"
docker logs research-orchestrator --tail 3 2>&1 | sed 's/^/    /'
echo ""

echo "  🔬 Research Agent:"
docker logs research-agent-runner --tail 3 2>&1 | sed 's/^/    /'
echo ""

echo "  🧐 Critic:"
docker logs research-critic --tail 3 2>&1 | sed 's/^/    /'
echo ""

echo "  🦀 Moltbook Bridge:"
docker logs moltbook-bridge --tail 3 2>&1 | sed 's/^/    /'
echo ""

# Consensus summary
echo "🎯 RECENT CONSENSUS:"
docker logs research-orchestrator 2>&1 | grep "CONSENSUS REACHED" | tail -3 | sed 's/^/  /'
echo ""

# Kafka topic stats (requires kafka container tools)
echo "📊 KAFKA TOPICS:"
echo "  Visit http://localhost:8080 for detailed message browser"
echo ""

echo "🔗 QUICK LINKS:"
echo "  • Kafka UI: http://localhost:8080"
echo "  • Ollama API: http://localhost:11434"
echo ""

echo "📝 LOGS COMMANDS:"
echo "  docker logs research-orchestrator -f"
echo "  docker logs research-agent-runner -f"
echo "  docker logs research-critic -f"
echo "  docker logs moltbook-bridge -f"
echo ""
