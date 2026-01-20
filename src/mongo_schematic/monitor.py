from __future__ import annotations

import json
import time
from typing import Any, Dict, Optional
from urllib import request

from mongo_schematic.drift import detect_drift


def _post_webhook(url: str, payload: Dict[str, Any]) -> None:
    data = json.dumps(payload).encode("utf-8")
    req = request.Request(url, data=data, headers={"Content-Type": "application/json"})
    request.urlopen(req, timeout=10)


def run_monitor(
    expected_schema: Dict[str, Any],
    observed_schema: Dict[str, Any],
    interval_seconds: int,
    webhook_url: Optional[str] = None,
    once: bool = False,
) -> Dict[str, Any]:
    last_result: Dict[str, Any] = {}
    while True:
        result = detect_drift(expected_schema, observed_schema)
        last_result = result
        if webhook_url:
            _post_webhook(webhook_url, result)
        if once:
            break
        time.sleep(interval_seconds)
    return last_result
