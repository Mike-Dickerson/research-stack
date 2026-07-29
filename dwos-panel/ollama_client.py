"""Minimal wrapper around Ollama's HTTP API for the DWOS panel."""

import os
import requests

OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")
DEFAULT_MODEL = "phi3:mini"
TEMPERATURE = 0.2
NUM_PREDICT = 1024
TIMEOUT = 180


def generate(system_prompt, user_prompt, model=DEFAULT_MODEL):
    """Send a single prompt to Ollama and return the response text."""
    resp = requests.post(
        f"{OLLAMA_HOST}/api/generate",
        json={
            "model": model,
            "system": system_prompt,
            "prompt": user_prompt,
            "stream": False,
            "options": {
                "temperature": TEMPERATURE,
                "num_predict": NUM_PREDICT,
            },
        },
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json().get("response", "").strip()
