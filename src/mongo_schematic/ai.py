"""AI-powered recommendations and migration planning with Gemini."""

from __future__ import annotations

import json
import logging
import re
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def _extract_json_from_text(text: str) -> Optional[str]:
    """Extract JSON from text that may contain markdown code blocks."""
    json_pattern = r"```(?:json)?\s*([\s\S]*?)```"
    matches = re.findall(json_pattern, text)
    if matches:
        return matches[0].strip()

    text = text.strip()
    if text.startswith("[") or text.startswith("{"):
        return text

    return None


def _parse_json_safe(text: str, default: Any) -> Any:
    """Safely parse JSON with fallback extraction."""
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        extracted = _extract_json_from_text(text)
        if extracted:
            try:
                return json.loads(extracted)
            except json.JSONDecodeError:
                pass
        return default


def _retry_api_call(func, max_retries: int = 2, delay: float = 1.0):
    """Retry an API call with exponential backoff."""
    last_error = None
    for attempt in range(max_retries + 1):
        try:
            return func()
        except Exception as e:
            last_error = e
            if attempt < max_retries:
                logger.warning(f"API call failed (attempt {attempt + 1}), retrying: {e}")
                time.sleep(delay * (2 ** attempt))
            else:
                logger.error(f"API call failed after {max_retries + 1} attempts: {e}")
    raise last_error


def generate_recommendations_with_gemini(
    api_key: Optional[str],
    schema: Dict[str, Any],
    anomalies: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Generate schema recommendations using Gemini AI.

    Args:
        api_key: Gemini API key.
        schema: The analyzed schema.
        anomalies: Detected anomalies.

    Returns:
        List of recommendation objects with type, title, description, priority.
    """
    if not api_key:
        logger.warning("Gemini API key not configured. Skipping AI recommendations.")
        return []

    try:
        import google.generativeai as genai
    except ImportError:
        logger.warning("google-generativeai package not installed. Skipping AI recommendations.")
        return []

    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel("gemini-1.5-flash")

        prompt = (
            "You are a MongoDB schema advisor. Provide 3-5 concise recommendations.\n"
            "Schema (JSON):\n"
            f"{json.dumps(schema, indent=2)}\n\n"
            "Anomalies (JSON):\n"
            f"{json.dumps(anomalies, indent=2)}\n\n"
            "Return a JSON array of objects with fields: type, title, description, priority.\n"
            "Priority should be: high, medium, or low.\n"
            "Return ONLY the JSON array, no markdown formatting."
        )

        def make_request():
            return model.generate_content(prompt)

        response = _retry_api_call(make_request)
        text = response.text or "[]"

        parsed = _parse_json_safe(text, [])
        if isinstance(parsed, list):
            valid_recs = []
            for rec in parsed:
                if isinstance(rec, dict) and "title" in rec:
                    valid_recs.append({
                        "type": rec.get("type", "AI_RECOMMENDATION"),
                        "title": rec.get("title", ""),
                        "description": rec.get("description", ""),
                        "priority": rec.get("priority", "medium"),
                    })
            return valid_recs

        return []

    except Exception as e:
        logger.error(f"Failed to generate AI recommendations: {e}")
        return []


def generate_migration_plan_with_gemini(
    api_key: Optional[str],
    draft_plan: Dict[str, Any],
    constraints: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Generate a refined migration plan using Gemini AI.

    Args:
        api_key: Gemini API key.
        draft_plan: The initial migration plan from diff analysis.
        constraints: Optional constraints (strategy, batch_size, allow_remove).

    Returns:
        Refined migration plan with strategy, batch_size, and steps.
    """
    if not api_key:
        logger.warning("Gemini API key not configured. Returning draft plan.")
        return draft_plan

    try:
        import google.generativeai as genai
    except ImportError:
        logger.warning("google-generativeai package not installed. Returning draft plan.")
        return draft_plan

    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel("gemini-1.5-flash")

        constraints_payload = constraints or {
            "strategy": "eager",
            "batch_size": 1000,
            "allow_remove": False,
        }

        prompt = (
            "You are a MongoDB migration planner. Provide a safe migration plan in JSON.\n"
            "Draft plan (JSON):\n"
            f"{json.dumps(draft_plan, indent=2)}\n\n"
            "Constraints (JSON):\n"
            f"{json.dumps(constraints_payload, indent=2)}\n\n"
            "Return JSON object with fields: strategy, batch_size, steps[].\n"
            "Each step: action (add_field|remove_field|convert_type|rename_field), field, details.\n"
            "Return ONLY the JSON object, no markdown formatting."
        )

        def make_request():
            return model.generate_content(prompt)

        response = _retry_api_call(make_request)
        text = response.text or "{}"

        parsed = _parse_json_safe(text, {})
        if isinstance(parsed, dict):
            return {
                "strategy": parsed.get("strategy", "eager"),
                "batch_size": parsed.get("batch_size", 1000),
                "steps": parsed.get("steps", []),
            }

        return draft_plan

    except Exception as e:
        logger.error(f"Failed to generate AI migration plan: {e}")
        return draft_plan
