"""PDF processing: text extraction and scan-to-image conversion."""

import logging
from pathlib import Path

import fitz  # PyMuPDF

logger = logging.getLogger(__name__)


def extract_text_from_pdf(file_path: str | Path) -> str | None:
    """Extract text from a text-based PDF.

    Returns extracted text if the PDF contains selectable text,
    or None if it appears to be a scanned document.
    """
    file_path = Path(file_path)
    doc = fitz.open(str(file_path))
    all_text = []

    for page in doc:
        text = page.get_text("text")
        if text.strip():
            all_text.append(text.strip())

    doc.close()

    combined = "\n\n".join(all_text)
    # If very little text was extracted, treat as scanned
    if len(combined.strip()) < 50:
        return None
    return combined


def pdf_to_images(file_path: str | Path, dpi: int = 300) -> list[Path]:
    """Convert a PDF (typically scanned) to PNG images.

    Returns list of paths to generated image files.
    """
    file_path = Path(file_path)
    output_dir = file_path.parent
    doc = fitz.open(str(file_path))
    image_paths = []

    zoom = dpi / 72  # 72 is default PDF DPI
    matrix = fitz.Matrix(zoom, zoom)

    for i, page in enumerate(doc):
        pix = page.get_pixmap(matrix=matrix)
        img_path = output_dir / f"{file_path.stem}_page_{i + 1}.png"
        pix.save(str(img_path))
        image_paths.append(img_path)
        logger.info("Converted page %d to %s", i + 1, img_path.name)

    doc.close()
    return image_paths


def is_scanned_pdf(file_path: str | Path) -> bool:
    """Check if a PDF is scanned (image-based) vs text-based."""
    text = extract_text_from_pdf(file_path)
    return text is None
