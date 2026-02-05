# Real Research Upgrade Guide

## What Changed

The research-stack now uses **real research agents** instead of stubs:

### Agent Runner (services/agent-runner)
- **Real arXiv Search**: Searches scientific papers via arXiv API
- **LLM Analysis**: Uses Claude 3.5 Haiku to analyze hypothesis against literature
- **Fallback Mode**: Works without API key using heuristic analysis

### Critic (services/critic)
- **LLM Peer Review**: Uses Claude 3.5 Haiku for scientific peer review
- **Skepticism Applied**: 5% discount on external sources (Moltbook)
- **Fallback Mode**: Works without API key using heuristic scoring

### Moltbook Bridge (services/moltbook-bridge)
- **NO CHANGES**: Still uses glorious Crustafarian Council randomness
- This contrast between rigorous science and chaotic wisdom makes consensus interesting

## Setup

1. **Get Anthropic API Key**
   - Visit: https://console.anthropic.com/
   - Create an API key
   - Copy `.env.example` to `.env`
   - Add your key: `ANTHROPIC_API_KEY=sk-ant-...`

2. **Rebuild Containers**
   ```bash
   docker-compose down
   docker-compose build
   docker-compose up -d
   ```

3. **Monitor Real Research**
   ```bash
   docker logs research-agent-runner -f
   docker logs research-critic -f
   docker logs research-orchestrator -f
   ```

## How It Works Now

1. **Orchestrator** publishes hypothesis
2. **Agent Runner** searches arXiv, finds real papers, Claude analyzes them
3. **Moltbook Bridge** convenes Crustafarian Council (random wisdom)
4. **Critic** performs LLM peer review with skepticism
5. **Orchestrator** builds consensus from critiques

## Fallback Mode

Without ANTHROPIC_API_KEY, the system still works:
- Agent uses heuristic analysis based on paper count
- Critic uses formula-based scoring
- Logs will show "⚠ No ANTHROPIC_API_KEY, using fallback"

## Cost Estimates

Using Claude 3.5 Haiku (cheapest, fastest model):
- Agent analysis: ~500 tokens per task (~$0.0004 per research)
- Critic review: ~300 tokens per critique (~$0.0002 per critique)
- Estimated: ~$0.001 per hypothesis with full pipeline

For 100 hypotheses: ~$0.10 total
