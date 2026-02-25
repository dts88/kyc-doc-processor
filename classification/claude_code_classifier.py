"""Document classifier using Claude Code CLI (`claude -p`) as backend."""

import json
import logging
import os
import subprocess
from pathlib import Path

from classification.classifier import ClassificationResult
from classification.doc_types import DOC_TYPES
from classification.prompts import CLASSIFICATION_SYSTEM_PROMPT, CLASSIFICATION_USER_PROMPT

logger = logging.getLogger(__name__)

# JSON schema for structured output from Claude Code CLI
_CLASSIFICATION_JSON_SCHEMA = json.dumps({
    "type": "object",
    "properties": {
        "doc_types": {
            "type": "array",
            "items": {"type": "string"},
            "description": "All matching doc type codes, primary first",
        },
        "company_name": {"type": "string"},
        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
        "reasoning": {"type": "string"},
    },
    "required": ["doc_types", "company_name", "confidence", "reasoning"],
})


def classify_with_claude_code(
    text_content: str | None = None,
    image_paths: list[Path] | None = None,
    model: str = "sonnet",
    few_shot_examples: str = "",
) -> ClassificationResult:
    """Classify a document using Claude Code CLI subprocess.

    For text documents: pipes text via stdin with tools disabled.
    For scanned images: passes image file paths for Claude to read.
    """
    # Build the prompt
    prompt_template = CLASSIFICATION_USER_PROMPT.replace(
        "{few_shot_block}",
        few_shot_examples,
    )
    if text_content:
        user_prompt = prompt_template.replace("{content}", text_content)
    elif image_paths:
        # Limit images: each requires a Read tool round-trip via CLI
        pages_to_send = image_paths[:3]
        paths_str = "\n".join(str(p) for p in pages_to_send)
        user_prompt = prompt_template.replace(
            "{content}",
            f"[Image files to analyze — please read these files:]\n{paths_str}",
        )
    else:
        raise ValueError("Either text_content or image_paths must be provided")

    full_prompt = (
        f"{CLASSIFICATION_SYSTEM_PROMPT}\n\n{user_prompt}"
    )

    # Build CLI command
    cmd = [
        "claude",
        "-p",
        "--output-format", "json",
        "--json-schema", _CLASSIFICATION_JSON_SCHEMA,
        "--no-session-persistence",
        "--model", model,
    ]

    if image_paths and not text_content:
        # Scanned document: restrict to Read tool only AND auto-approve it
        cmd.extend(["--tools", "Read", "--allowedTools", "Read"])
    else:
        # Text document: no tools needed
        cmd.extend(["--tools", ""])

    # Environment: strip CLAUDECODE to prevent nested-session blocking
    env = {k: v for k, v in os.environ.items() if k not in ("CLAUDECODE", "CLAUDE_CODE_ENTRYPOINT")}

    logger.info("Calling Claude Code CLI for classification (model=%s)", model)

    proc = subprocess.run(
        cmd,
        input=full_prompt,
        capture_output=True,
        text=True,
        timeout=300,
        env=env,
    )

    if proc.returncode != 0:
        raise RuntimeError(
            f"Claude Code CLI failed (rc={proc.returncode}): {proc.stderr[:500]}"
        )

    raw_output = proc.stdout.strip()

    # Parse the outer JSON wrapper from --output-format json
    try:
        outer = json.loads(raw_output)
    except json.JSONDecodeError:
        raise RuntimeError(f"Failed to parse CLI output as JSON: {raw_output[:500]}")

    # With --json-schema, structured data is in "structured_output"; otherwise in "result"
    if "structured_output" in outer and isinstance(outer["structured_output"], dict):
        result = outer["structured_output"]
    else:
        result_text = outer.get("result", "")
        if isinstance(result_text, dict):
            result = result_text
        else:
            try:
                result = json.loads(result_text)
            except json.JSONDecodeError:
                raise RuntimeError(
                    f"Failed to parse classification result: {result_text[:500]}"
                )

    # Normalize doc_types
    raw_types = result.get("doc_types") or result.get("doc_type", "unknown")
    if isinstance(raw_types, str):
        raw_types = [raw_types]

    doc_types = []
    for dt in raw_types:
        if dt in DOC_TYPES or dt == "unknown":
            doc_types.append(dt)
        else:
            logger.warning("Unknown doc_type from Claude Code CLI: %s", dt)
    if not doc_types:
        doc_types = ["unknown"]

    return ClassificationResult(
        doc_types=doc_types,
        company_name=result.get("company_name", "unknown"),
        confidence=float(result.get("confidence", 0.0)),
        reasoning=result.get("reasoning", ""),
        model_used=f"claude-code-cli/{model}",
        input_tokens=0,
        output_tokens=0,
        raw_response=raw_output,
    )
