"""Image preprocessing for Claude vision API."""

import base64
import logging
from pathlib import Path

from PIL import Image

logger = logging.getLogger(__name__)

SUPPORTED_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".tiff", ".tif", ".bmp", ".webp"}


def preprocess_image(
    file_path: str | Path,
    max_dimension: int = 4096,
    jpeg_quality: int = 85,
) -> Path:
    """Resize and compress image for Claude API.

    Returns path to the processed image (may be the original if no changes needed).
    Raises RuntimeError if image cannot be processed.
    """
    file_path = Path(file_path)
    img = None
    try:
        img = Image.open(file_path)

        # Convert TIFF/BMP to PNG
        if file_path.suffix.lower() in (".tiff", ".tif", ".bmp"):
            new_path = file_path.with_suffix(".png")
            img.save(str(new_path))
            file_path = new_path
            logger.info("Converted %s to PNG", file_path.name)

        # Resize if too large
        w, h = img.size
        if w > max_dimension or h > max_dimension:
            ratio = min(max_dimension / w, max_dimension / h)
            new_size = (int(w * ratio), int(h * ratio))
            img = img.resize(new_size, Image.LANCZOS)
            output_path = file_path.parent / f"{file_path.stem}_resized{file_path.suffix}"
            img.save(str(output_path), quality=jpeg_quality)
            logger.info("Resized image from %dx%d to %dx%d", w, h, *new_size)
            return output_path

        return file_path
    except Exception as e:
        raise RuntimeError(f"Failed to preprocess image {file_path.name}: {e}") from e
    finally:
        if img:
            try:
                img.close()
            except Exception:
                pass


def image_to_base64(file_path: str | Path) -> tuple[str, str]:
    """Read an image and return (base64_data, media_type)."""
    file_path = Path(file_path)
    suffix = file_path.suffix.lower()

    media_type_map = {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".gif": "image/gif",
        ".webp": "image/webp",
    }
    media_type = media_type_map.get(suffix, "image/png")

    with open(file_path, "rb") as f:
        data = base64.standard_b64encode(f.read()).decode("utf-8")

    return data, media_type
