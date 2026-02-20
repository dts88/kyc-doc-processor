"""Claude API structured data extractor for KYC documents."""

import json
import logging
import re
import time
from dataclasses import dataclass

import anthropic

from classification.prompts import EXTRACTION_PROMPTS, EXTRACTION_SYSTEM_PROMPT
from extraction.schemas import DOC_TYPE_SCHEMA_MAP
from extraction.validation import validate_extraction
from processing.image_handler import image_to_base64

logger = logging.getLogger(__name__)

# Document types that benefit from Opus (complex structure)
COMPLEX_DOC_TYPES = {"financial_reports", "ownership_structure"}

# Default max images to send per API call to avoid 413 errors
DEFAULT_max_images = 10


@dataclass
class ExtractionResult:
    doc_type: str
    extracted_data: dict
    validated: bool
    validation_errors: list[str]
    model_used: str
    input_tokens: int
    output_tokens: int
    raw_response: str


def extract_document_data(
    client: anthropic.Anthropic,
    doc_type: str,
    text_content: str | None = None,
    image_paths: list | None = None,
    model_simple: str = "claude-sonnet-4-20250514",
    model_complex: str = "claude-opus-4-20250115",
    max_retries: int = 3,
    retry_base_delay: float = 1.0,
    max_images: int = DEFAULT_max_images,
) -> ExtractionResult:
    """Extract structured data from a classified KYC document.

    Uses the appropriate extraction prompt for the document type.
    Complex documents (financial reports, ownership structures) use Opus.
    """
    if doc_type not in EXTRACTION_PROMPTS:
        raise ValueError(f"No extraction prompt for document type: {doc_type}")

    # Select model based on complexity
    model = model_complex if doc_type in COMPLEX_DOC_TYPES else model_simple

    # Build message content
    content = []

    if image_paths:
        pages_to_send = _select_pages_for_extraction(image_paths, doc_type)
        if len(pages_to_send) < len(image_paths):
            logger.info(
                "Document has %d pages, sending %d for extraction (type=%s)",
                len(image_paths), len(pages_to_send), doc_type,
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

    # Build extraction prompt
    prompt_text = EXTRACTION_PROMPTS[doc_type]
    if text_content:
        prompt_text = f"Document content:\n\n{text_content}\n\n{prompt_text}"
    else:
        prompt_text = f"Analyze the attached document image(s).\n\n{prompt_text}"

    content.append({"type": "text", "text": prompt_text})

    # Call Claude with retries
    last_error = None
    for attempt in range(max_retries):
        try:
            response = client.messages.create(
                model=model,
                max_tokens=4096,
                system=EXTRACTION_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": content}],
            )

            raw_text = response.content[0].text
            # Parse JSON from response
            json_match = re.search(r"```(?:json)?\s*(.*?)```", raw_text, re.DOTALL)
            json_str = json_match.group(1).strip() if json_match else raw_text.strip()
            extracted = json.loads(json_str)

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
                model_used=model,
                input_tokens=response.usage.input_tokens,
                output_tokens=response.usage.output_tokens,
                raw_response=raw_text,
            )

        except (json.JSONDecodeError, KeyError, IndexError) as e:
            logger.warning("Failed to parse extraction response (attempt %d): %s", attempt + 1, e)
            last_error = e
        except anthropic.APIError as e:
            logger.warning("Claude API error (attempt %d): %s", attempt + 1, e)
            last_error = e

        if attempt < max_retries - 1:
            delay = retry_base_delay * (2 ** attempt)
            logger.info("Retrying in %.1fs...", delay)
            time.sleep(delay)

    raise RuntimeError(f"Extraction failed after {max_retries} attempts: {last_error}")


def _select_pages_for_extraction(image_paths: list, doc_type: str) -> list:
    """Select which pages to send for extraction, based on document type.

    For financial reports (often 20-30 pages), sample key pages:
      - First 3 pages (cover, auditor report, opinion)
      - Middle pages (balance sheet, income statement usually around pages 5-8)
      - Last 2 pages (notes summary)
    For other document types, send up to max_images pages.
    """
    total = len(image_paths)
    if total <= max_images:
        return image_paths

    if doc_type == "financial_reports":
        # Sample strategy for financial reports
        indices = set()
        # First 4 pages (cover, auditor info, audit opinion)
        for i in range(min(4, total)):
            indices.add(i)
        # Pages around 1/3 mark (often balance sheet / income statement)
        mid1 = total // 3
        for i in range(max(0, mid1 - 1), min(total, mid1 + 2)):
            indices.add(i)
        # Pages around 1/2 mark
        mid2 = total // 2
        for i in range(max(0, mid2 - 1), min(total, mid2 + 1)):
            indices.add(i)
        # Last 2 pages
        for i in range(max(0, total - 2), total):
            indices.add(i)
        selected = sorted(indices)[:max_images]
        return [image_paths[i] for i in selected]

    # Default: first N pages
    return image_paths[:max_images]
