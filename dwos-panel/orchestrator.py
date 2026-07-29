"""Runs the 11 DWOS review personas against a theory write-up and produces a report.

Usage:
    python orchestrator.py [--theory PATH]
"""

import argparse
import json
import os
from datetime import datetime

from ollama_client import generate

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PERSONAS_PATH = os.path.join(BASE_DIR, "personas.json")
DEFAULT_THEORY_PATH = os.path.join(BASE_DIR, "theory.md")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")


def load_personas():
    with open(PERSONAS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def build_system_prompt(persona):
    lines = [persona["role_intro"], "", "Responsibilities:"]
    lines += [f"- {r}" for r in persona["responsibilities"]]
    return "\n".join(lines)


def run_specialist(persona_id, persona, theory_text):
    print(f"  -> Running {persona['title']}...")
    system_prompt = build_system_prompt(persona)
    response = generate(system_prompt, theory_text)
    return response


def run_synthesis(persona_id, persona, theory_text, prior_results):
    print(f"  -> Running {persona['title']}...")
    system_prompt = build_system_prompt(persona)

    context_lines = ["THEORY UNDER REVIEW:", theory_text, "", "PRIOR AGENT FINDINGS:"]
    for pid, result in prior_results.items():
        context_lines.append(f"\n### {result['title']}\n{result['response']}")

    user_prompt = "\n".join(context_lines)
    response = generate(system_prompt, user_prompt)
    return response


def write_report(theory_text, specialist_results, synthesis_results):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(OUTPUT_DIR, f"dwos_review_{timestamp}.md")

    lines = ["# DWOS Physics Review Panel", "", "## Theory Under Review", "", theory_text, ""]

    lines.append("## Specialist Findings")
    for pid, result in specialist_results.items():
        lines += ["", f"### {result['title']}", "", result["response"]]

    lines.append("")
    lines.append("## Synthesis")
    for pid, result in synthesis_results.items():
        lines += ["", f"### {result['title']}", "", result["response"]]

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    return out_path


def main():
    parser = argparse.ArgumentParser(description="Run the DWOS physics review panel")
    parser.add_argument("--theory", default=DEFAULT_THEORY_PATH, help="Path to the theory write-up")
    args = parser.parse_args()

    with open(args.theory, "r", encoding="utf-8") as f:
        theory_text = f.read().strip()

    personas = load_personas()

    print("Phase 1: specialist review")
    specialist_results = {}
    for persona_id, persona in personas.items():
        if persona["phase"] != "specialist":
            continue
        response = run_specialist(persona_id, persona, theory_text)
        specialist_results[persona_id] = {"title": persona["title"], "response": response}

    print("Phase 2: synthesis")
    synthesis_results = {}
    prior_results = dict(specialist_results)
    for persona_id, persona in personas.items():
        if persona["phase"] != "synthesis":
            continue
        response = run_synthesis(persona_id, persona, theory_text, prior_results)
        entry = {"title": persona["title"], "response": response}
        synthesis_results[persona_id] = entry
        prior_results[persona_id] = entry

    out_path = write_report(theory_text, specialist_results, synthesis_results)
    print(f"\nReport written to: {out_path}")


if __name__ == "__main__":
    main()
