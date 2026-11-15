"""Schema normalization and nested JSON flattening utilities for webhook payloads."""

from __future__ import annotations

import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict

EMAIL_REGEX = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")


def flatten_dict(data: Dict[str, Any], parent_key: str = "", sep: str = ".") -> Dict[str, Any]:
    """Recursively flatten nested JSON dictionaries into dot-delimited keys."""
    items: Dict[str, Any] = {}
    for key, value in data.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else str(key)
        if isinstance(value, dict):
            items.update(flatten_dict(value, new_key, sep=sep))
        elif isinstance(value, list):
            items[new_key] = ", ".join(str(v) for v in value if v is not None)
        elif value is not None:
            items[new_key] = value
    return items


def normalize_lead_payload(raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    """Extract, clean, and validate lead fields from heterogeneous webhook sources."""
    body = raw_payload.get("body", raw_payload)
    contact = body.get("contact") or body.get("user") or {}

    raw_email = str(contact.get("email") or body.get("email") or "").strip().lower()
    is_valid = bool(EMAIL_REGEX.match(raw_email))
    domain = raw_email.split("@")[1] if "@" in raw_email else ""

    first = str(contact.get("first_name") or "").strip()
    last = str(contact.get("last_name") or "").strip()
    full_name = " ".join(part for part in (first, last) if part) or "Unknown Contact"

    return {
        "lead_id": str(body.get("event_id") or f"evt_{uuid.uuid4().hex[:10]}"),
        "full_name": full_name,
        "email": raw_email,
        "company_domain": domain,
        "source": str(body.get("utm_source") or "webhook_ingest").strip(),
        "is_valid": is_valid,
        "processed_at": datetime.now(timezone.utc).isoformat(),
    }
