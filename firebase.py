import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv


def _find_env_file() -> tuple[Path, Path]:
    """
    Locate the nearest .env file (searching up a few levels) to keep
    behaviour consistent with the rest of the app.
    """
    current = Path.cwd()
    for _ in range(3):
        env_file = current / ".env"
        if env_file.exists():
            return env_file, current
        current = current.parent
    return Path.cwd() / ".env", Path.cwd()


_env_path, _project_root = _find_env_file()
load_dotenv(dotenv_path=_env_path, override=True)

FIREBASE_API_KEY = os.getenv("FIREBASE_API_KEY")
FIREBASE_AUTH_DOMAIN = os.getenv("FIREBASE_AUTH_DOMAIN")
FIREBASE_PROJECTID = os.getenv("FIREBASE_PROJECTID")
FIREBASE_STORAGEBUCKET = os.getenv("FIREBASE_STORAGEBUCKET")
FIREBASE_MESSAGING_SENDER_ID = os.getenv("FIREBASE_MESSAGING_SENDER_ID")
FIREBASE_APP_ID = os.getenv("FIREBASE_APP_ID")
FIREBASE_MEASUREMENT_ID = os.getenv("FIREBASE_MEASUREMENT_ID")

FIRESTORE_BASE_URL = (
    f"https://firestore.googleapis.com/v1/projects/{FIREBASE_PROJECTID}"
    "/databases/(default)/documents"
)


def _decode_value(value: Dict[str, Any]) -> Any:
    """Decode a single Firestore REST API value into a plain Python value."""
    if "stringValue" in value:
        return value["stringValue"]
    if "integerValue" in value:
        return int(value["integerValue"])
    if "doubleValue" in value:
        return float(value["doubleValue"])
    if "booleanValue" in value:
        return bool(value["booleanValue"])
    if "nullValue" in value:
        return None
    if "timestampValue" in value:
        return value["timestampValue"]
    if "mapValue" in value:
        return {
            k: _decode_value(v)
            for k, v in value.get("mapValue", {}).get("fields", {}).items()
        }
    if "arrayValue" in value:
        return [
            _decode_value(v)
            for v in value.get("arrayValue", {}).get("values", []) or []
        ]
    # Fallback – return the raw structure
    return value


def _decode_document(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Convert a Firestore REST document into a flat dict (including id)."""
    name: str = doc.get("name", "")
    doc_id = name.rsplit("/", 1)[-1] if name else ""
    fields = doc.get("fields", {}) or {}
    decoded = {k: _decode_value(v) for k, v in fields.items()}
    decoded["id"] = doc_id
    return decoded


def get_user_config(user_id: str) -> Optional[Dict[str, Any]]:
    """
    Read a single document from the `user-config` collection using the REST API.
    """
    url = f"{FIRESTORE_BASE_URL}/user-config/{user_id}"
    params = {"key": FIREBASE_API_KEY} if FIREBASE_API_KEY else {}
    resp = requests.get(url, params=params, timeout=10)

    if resp.status_code == 404:
        return None
    resp.raise_for_status()

    doc = resp.json()
    if "fields" not in doc:
        return None
    return _decode_document(doc)


def get_all_user_configs() -> List[Dict[str, Any]]:
    """
    Fetch all documents from the `user-config` collection using the REST API.
    """
    url = f"{FIRESTORE_BASE_URL}/user-config"
    params = {"key": FIREBASE_API_KEY} if FIREBASE_API_KEY else {}
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()

    body = resp.json()
    documents = body.get("documents", []) or []
    return [_decode_document(doc) for doc in documents]
