import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone


def callback_enabled() -> bool:
    value = os.getenv("TASK_CALLBACK_ENABLED", "false").strip().lower()
    return value == "true"


def callback_url() -> str:
    return os.getenv("TASK_CALLBACK_URL", "").strip()


def post_procedure_result(query: str, rows: list[dict], meta: dict | None = None) -> None:
    if not callback_enabled():
        return

    url = callback_url()
    if not url:
        raise ValueError("TASK_CALLBACK_URL is required when callback is enabled")

    payload = {
        "query": query,
        "rows": rows,
        "meta": {
            "sent_at": datetime.now(timezone.utc).isoformat(),
            **(meta or {}),
        },
    }

    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(request, timeout=10) as response:
        if response.status < 200 or response.status >= 300:
            raise urllib.error.HTTPError(
                url,
                response.status,
                f"unexpected callback status {response.status}",
                response.headers,
                None,
            )


def report_callback_failure(err: Exception) -> None:
    print(json.dumps({"callback_error": str(err)}, indent=2), file=sys.stderr)
