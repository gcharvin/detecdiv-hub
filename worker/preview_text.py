from __future__ import annotations


def text_pixel_width(text: str, *, scale: int) -> int:
    return max(0, sum((5 * scale) + scale for _ in text) - scale)


def fit_text_scale(
    text: str,
    *,
    available_width: int,
    desired_scale: int,
    minimum_scale: int = 1,
    maximum_scale: int = 6,
) -> int:
    clean = str(text or "").strip()
    if not clean:
        return max(1, minimum_scale)
    width_at_unit_scale = text_pixel_width(clean, scale=1)
    if width_at_unit_scale <= 0 or available_width <= 0:
        return max(1, minimum_scale)
    fitted = available_width // width_at_unit_scale
    fitted = max(minimum_scale, min(maximum_scale, int(fitted)))
    return max(minimum_scale, min(int(desired_scale), fitted))
