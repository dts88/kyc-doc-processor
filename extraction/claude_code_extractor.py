"""Document data extractor using Claude Code CLI (`claude -p`) as backend."""

import json
import logging
import os
import subprocess
from pathlib import Path

from classification.prompts import EXTRACTION_PROMPTS, EXTRACTION_SYSTEM_PROMPT
from extraction.extractor import ExtractionResult
from extraction.schemas import DOC_TYPE_SCHEMA_MAP
from extraction.validation import validate_extraction

logger = logging.getLogger(__name__)

# Document types that benefit from a more capable model
COMPLEX_DOC_TYPES = {"financial_reports", "ownership_structure"}


def _build_json_schema(doc_type: str) -> str:
    """Build a JSON Schema string from the Pydantic model for the given doc type."""
    schema_cls = DOC_TYPE_SCHEMA_MAP.get(doc_type)
    if schema_cls:
        return json.dumps(schema_cls.model_json_schema())
    # Fallback: accept any object
    return json.dumps({"type": "object"})


def extract_with_claude_code(
    doc_type: str,
    text_content: str | None = None,
    image_paths: list[Path] | None = None,
    model: str = "sonnet",
) -> ExtractionResult:
    """Extract structured data from a classified KYC document using Claude Code CLI.

    Uses the appropriate extraction prompt for the document type.
    Complex documents use the model specified; caller can override.
    """
    if doc_type not in EXTRACTION_PROMPTS:
        raise ValueError(f"No extraction prompt for document type: {doc_type}")

    # Build the prompt
    extraction_prompt = EXTRACTION_PROMPTS[doc_type]
    if text_content:
        user_prompt = f"Document content:\n\n{text_content}\n\n{extraction_prompt}"
    elif image_paths:
        # Limit images: each requires a Read tool round-trip via CLI
        pages_to_send = image_paths[:5]
        paths_str = "\n".join(str(p) for p in pages_to_send)
        user_prompt = (
            f"Analyze the following document image files — please read them:\n"
            f"{paths_str}\n\n{extraction_prompt}"
        )
    else:
        raise ValueError("Either text_content or image_paths must be provided")

    full_prompt = f"{EXTRACTION_SYSTEM_PROMPT}\n\n{user_prompt}"

    # CLI mode: always use sonnet (opus via CLI is too slow due to multi-turn Read tool calls)
    effective_model = model

    # Build JSON schema from Pydantic model
    json_schema = _build_json_schema(doc_type)

    # Build CLI command
    cmd = [
        "claude",
        "-p",
        "--output-format", "json",
        "--json-schema", json_schema,
        "--no-session-persistence",
        "--model", effective_model,
    ]

    if image_paths and not text_content:
        # Scanned document: restrict to Read tool only AND auto-approve it
        cmd.extend(["--tools", "Read", "--allowedTools", "Read"])
    else:
        cmd.extend(["--tools", ""])

    # Environment: strip CLAUDECODE to prevent nested-session blocking
    env = {k: v for k, v in os.environ.items() if k not in ("CLAUDECODE", "CLAUDE_CODE_ENTRYPOINT")}

    logger.info(
        "Calling Claude Code CLI for extraction (type=%s, model=%s)",
        doc_type, effective_model,
    )

    proc = subprocess.run(
        cmd,
        input=full_prompt,
        capture_output=True,
        text=True,
        timeout=600,
        env=env,
    )

    if proc.returncode != 0:
        raise RuntimeError(
            f"Claude Code CLI extraction failed (rc={proc.returncode}): {proc.stderr[:500]}"
        )

    raw_output = proc.stdout.strip()

    # Parse the outer JSON wrapper from --output-format json
    try:
        outer = json.loads(raw_output)
    except json.JSONDecodeError:
        raise RuntimeError(f"Failed to parse CLI output as JSON: {raw_output[:500]}")

    # With --json-schema, structured data is in "structured_output"; otherwise in "result"
    if "structured_output" in outer and isinstance(outer["structured_output"], dict):
        extracted = outer["structured_output"]
    else:
        result_text = outer.get("result", "")
        if isinstance(result_text, dict):
            extracted = result_text
        else:
            try:
                extracted = json.loads(result_text)
            except json.JSONDecodeError:
                raise RuntimeError(
                    f"Failed to parse extraction result: {result_text[:500]}"
                )

    # Validate with Pydantic model
    schema_cls = DOC_TYPE_SCHEMA_MAP.get(doc_type)
    if schema_cls:
        model_instance = schema_cls.model_validate(extracted)
        extracted = model_instance.model_dump()

    # Run business validation
    valid, errors = validate_extraction(doc_type, extracted)

    return ExtractionResult(
        doc_type=doc_type,
        extracted_data=extracted,
        validated=valid,
        validation_errors=errors,
        model_used=f"claude-code-cli/{effective_model}",
        input_tokens=0,
        output_tokens=0,
        raw_response=raw_output,
    )
