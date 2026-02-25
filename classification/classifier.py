"""Claude API document classifier - identifies document type and counterparty."""

import json
import logging
import re
import time
from dataclasses import dataclass

import anthropic

from classification.doc_types import DOC_TYPES
from classification.prompts import CLASSIFICATION_SYSTEM_PROMPT, CLASSIFICATION_USER_PROMPT
from processing.image_handler import image_to_base64

logger = logging.getLogger(__name__)


@dataclass
class ClassificationResult:
    doc_types: list[str]  # list of codes from doc_types (primary first)
    company_name: str
    confidence: float
    reasoning: str
    model_used: str
    input_tokens: int
    output_tokens: int
    raw_response: str

    @property
    def primary_doc_type(self) -> str:
        """Return the primary (first) document type."""
        return self.doc_types[0] if self.doc_types else "unknown"


DEFAULT_MAX_CLASSIFICATION_IMAGES = 5


def classify_document(
    client: anthropic.Anthropic,
    text_content: str | None = None,
    image_paths: list | None = None,
    model: str = "claude-sonnet-4-20250514",
    max_retries: int = 3,
    retry_base_delay: float = 1.0,
    max_images: int = DEFAULT_MAX_CLASSIFICATION_IMAGES,
    few_shot_examples: str = "",
) -> ClassificationResult:
    """Classify a document using Claude API.

    Sends text or images (or both) to Claude for classification.
    For multi-page scanned documents, only first few pages are sent.
    Returns classification result with doc_type, company_name, confidence.
    """
    # Build message content
    content = []

    if image_paths:
        # For classification, first few pages are sufficient
        pages_to_send = image_paths[:max_images]
        if len(image_paths) > max_images:
            logger.info(
                "Document has %d pages, sending first %d for classification",
                len(image_paths), max_images,
            )
        for img_path in pages_to_send:
            b64_data, media_type = image_to_base64(img_path)
            content.append({
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": media_type,
                    "data": b64_data,
                },
            })

    # Build prompt text
    prompt_text = CLASSIFICATION_USER_PROMPT.replace(
        "{few_shot_block}",
        few_shot_examples,
    ).replace(
        "{content}",
        text_content if text_content else "[See attached image(s)]",
    )
    content.append({"type": "text", "text": prompt_text})

    # Call Claude with retries
    last_error = None
    for attempt in range(max_retries):
        try:
            response = client.messages.create(
                model=model,
                max_tokens=1024,
                system=CLASSIFICATION_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": content}],
            )

            raw_text = response.content[0].text
            # Parse JSON from response (handle markdown code blocks)
            json_match = re.search(r"```(?:json)?\s*(.*?)```", raw_text, re.DOTALL)
            json_str = json_match.group(1).strip() if json_match else raw_text.strip()
            result = json.loads(json_str)

            # Support both "doc_types" (array) and legacy "doc_type" (string)
            raw_types = result.get("doc_types") or result.get("doc_type", "unknown")
            if isinstance(raw_types, str):
                raw_types = [raw_types]

            doc_types = []
            for dt in raw_types:
                if dt in DOC_TYPES or dt == "unknown":
                    doc_types.append(dt)
                else:
                    logger.warning("Unknown doc_type from Claude: %s", dt)
            if not doc_types:
                doc_types = ["unknown"]

            return ClassificationResult(
                doc_types=doc_types,
                company_name=result.get("company_name", "unknown"),
                confidence=float(result.get("confidence", 0.0)),
                reasoning=result.get("reasoning", ""),
                model_used=model,
                input_tokens=response.usage.input_tokens,
                output_tokens=response.usage.output_tokens,
                raw_response=raw_text,
            )

        except (json.JSONDecodeError, KeyError, IndexError) as e:
            logger.warning("Failed to parse classification response (attempt %d): %s", attempt + 1, e)
            last_error = e
        except anthropic.APIError as e:
            logger.warning("Claude API error (attempt %d): %s", attempt + 1, e)
            last_error = e

        if attempt < max_retries - 1:
            delay = retry_base_delay * (2 ** attempt)
            logger.info("Retrying in %.1fs...", delay)
            time.sleep(delay)

    raise RuntimeError(f"Classification failed after {max_retries} attempts: {last_error}")
