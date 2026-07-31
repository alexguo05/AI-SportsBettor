"""Shared Google Cloud Storage and canonical JSON helpers."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from google.cloud import storage
from google.oauth2 import service_account


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def create_gcs_client(src_dir: Path) -> storage.Client:
    credentials_path = src_dir / "ai-sports-bettor-559e8837739f.json"
    configured_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if configured_path:
        return storage.Client()
    if credentials_path.exists():
        credentials = service_account.Credentials.from_service_account_file(
            str(credentials_path)
        )
        return storage.Client(
            credentials=credentials,
            project=credentials.project_id,
        )
    return storage.Client()
