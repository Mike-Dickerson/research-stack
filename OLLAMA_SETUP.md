# Ollama Local LLM Setup

## What Changed

The research-stack now uses **Ollama** for local LLM inference - no API keys, no cloud costs!

### Why Ollama?

- **100% Free**: No API costs
- **100% Local**: Data never leaves your machine
- **Fast**: Optimized for CPU/GPU inference
- **Small Models**: phi3:mini (3.8B params) is fast and capable

### Services Using Ollama

**Agent Runner**: Analyzes hypotheses against arXiv papers
**Critic**: Performs skeptical peer review
**Moltbook**: Still uses glorious random Crustafarian wisdom (unchanged)

## Setup Instructions

### 1. First-Time Ollama Setup

The Ollama container will start automatically, but you need to pull the model on first run:

```bash
# Start the stack
docker-compose up -d

# Wait for Ollama to be ready (check logs)
docker logs ollama

# Pull the phi3:mini model (3.8B params, ~2.3GB download)
docker exec ollama ollama pull phi3:mini
```

**Model Download**: This downloads ~2.3GB once. Subsequent startups use the cached model.

### 2. Verify Ollama is Working

```bash
# Test Ollama directly
docker exec ollama ollama run phi3:mini "Hello, respond with JSON: {\"status\": \"ready\"}"

# Check if research services can reach it
docker logs research-agent-runner -f
```

You should see: `→ Analyzing with Ollama (phi3:mini)...`

### 3. Monitor Real Research

```bash
# Watch agent doing real arXiv research
docker logs research-agent-runner -f

# Watch critic performing peer review
docker logs research-critic -f

# Watch Crustafarian Council wisdom
docker logs moltbook-bridge -f

# Watch consensus building
docker logs research-orchestrator -f
```

## Performance

**Agent Analysis**: ~5-15 seconds per hypothesis (arXiv search + LLM analysis)
**Critic Review**: ~3-8 seconds per result (LLM peer review)
**Total Pipeline**: ~10-30 seconds from hypothesis to consensus

**Hardware**: Works on CPU, faster with GPU support (NVIDIA/AMD)

## Alternative Models

Want a different model? Edit docker-compose.yml and change:

```yaml
environment:
  OLLAMA_MODEL: "phi3:mini"  # Change to: mistral, llama3, gemma, etc.
```

Available models: https://ollama.com/library

**Recommendations**:
- **Fastest**: `phi3:mini` (3.8B, current default)
- **Better quality**: `mistral` (7B)
- **Best quality**: `llama3:8b` (8B, slower)

## Fallback Mode

If Ollama fails or model isn't pulled, services automatically fall back to heuristic analysis:
- Agent uses paper count + random confidence
- Critic uses formula-based scoring

Logs will show: `⚠ Ollama analysis error` and `→ Falling back to heuristic analysis`

## Cost Comparison

**Anthropic Claude Haiku**: ~$0.001 per hypothesis (~$0.10 for 100 hypotheses)
**Ollama (Local)**: $0.00 (just electricity)

## Troubleshooting

**Model not found**: Run `docker exec ollama ollama pull phi3:mini`
**Ollama connection error**: Check `docker logs ollama` and ensure container is running
**Slow performance**: Consider using GPU or smaller model

## No More API Keys Needed!

Delete `.env` file - you don't need it anymore. The stack is fully self-contained.
