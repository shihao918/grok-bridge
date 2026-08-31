"""Shared helpers: config, DPAPI protect/unprotect, Grok Bot token derivation, bridge state."""

import base64
import ctypes
import json
import os

BRIDGE_ROOT = os.path.dirname(os.path.abspath(__file__))
STATE_DIR = os.path.join(BRIDGE_ROOT, "state")
STATE_FILE = os.path.join(STATE_DIR, "bridge_state.json")
CONFIG_FILE = os.path.join(STATE_DIR, "config.json")

GROK_USERDATA = os.path.expandvars(r"%APPDATA%\Grok Bot")
SAND_SECRETS = os.path.join(GROK_USERDATA, "sand-secrets.json")
LOCAL_STATE = os.path.join(GROK_USERDATA, "Local State")

DEFAULT_CONFIG = {
    "gateway": "http://your-model-gateway/v1",  # OpenAI-compatible endpoint behind local_proxy
    "local_root": r"C:\path\to\workspace",
    "label": "MultiAgent-Bridge",
    "ags_user": "your-agentsstudio-user@example.com",
}


def config() -> dict:
    cfg = dict(DEFAULT_CONFIG)
    if os.path.exists(CONFIG_FILE):
        cfg.update(json.load(open(CONFIG_FILE, encoding="utf-8")))
    return cfg


class DATA_BLOB(ctypes.Structure):
    _fields_ = [("cbData", ctypes.c_uint), ("pbData", ctypes.POINTER(ctypes.c_ubyte))]


def dpapi_unprotect(data: bytes) -> bytes:
    buf = ctypes.create_string_buffer(data, len(data))
    blob_in = DATA_BLOB(len(data), ctypes.cast(buf, ctypes.POINTER(ctypes.c_ubyte)))
    blob_out = DATA_BLOB()
    ok = ctypes.windll.crypt32.CryptUnprotectData(
        ctypes.byref(blob_in), None, None, None, None, 0, ctypes.byref(blob_out)
    )
    if not ok:
        raise OSError(f"CryptUnprotectData failed: {ctypes.GetLastError()}")
    out = ctypes.string_at(blob_out.pbData, blob_out.cbData)
    ctypes.windll.kernel32.LocalFree(blob_out.pbData)
    return out


def dpapi_protect(data: bytes) -> bytes:
    buf = ctypes.create_string_buffer(data, len(data))
    blob_in = DATA_BLOB(len(data), ctypes.cast(buf, ctypes.POINTER(ctypes.c_ubyte)))
    blob_out = DATA_BLOB()
    ok = ctypes.windll.crypt32.CryptProtectData(
        ctypes.byref(blob_in), "grok-bridge", None, None, None, 0, ctypes.byref(blob_out)
    )
    if not ok:
        raise OSError(f"CryptProtectData failed: {ctypes.GetLastError()}")
    out = ctypes.string_at(blob_out.pbData, blob_out.cbData)
    ctypes.windll.kernel32.LocalFree(blob_out.pbData)
    return out


def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        return json.load(open(STATE_FILE, encoding="utf-8"))
    return {}


def save_state(state: dict) -> None:
    os.makedirs(STATE_DIR, exist_ok=True)
    json.dump(state, open(STATE_FILE, "w", encoding="utf-8"), indent=2)


def _active_account_key() -> bytes:
    """DPAPI-unprotected os_crypt key from the Grok Bot app's Local State."""
    ls = json.load(open(LOCAL_STATE, encoding="utf-8"))
    enc = base64.b64decode(ls["os_crypt"]["encrypted_key"])
    if enc[:5] != b"DPAPI":
        raise ValueError(f"unexpected key prefix {enc[:5]!r}")
    return dpapi_unprotect(enc[5:])


def get_grok_access_token() -> str:
    """Derive the logged-in Grok Bot access token at runtime (nothing stored in plaintext)."""
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    store = json.load(open(SAND_SECRETS, encoding="utf-8"))
    accounts = json.loads(store["cursor-accounts"])
    acct = accounts["accounts"][accounts["active"]]
    blob = base64.b64decode(acct["cursor-access-token"])
    if blob[:3] != b"v10":
        raise ValueError(f"unexpected token envelope {blob[:3]!r}")
    nonce, ct = blob[3:15], blob[15:]
    return AESGCM(_active_account_key()).decrypt(nonce, ct, None).decode("utf-8")


def _codex_key_path() -> str:
    return os.path.join(STATE_DIR, "codex_key.bin")


def get_codex_key() -> str:
    """Model-gateway API key, DPAPI-encrypted at rest (never plaintext)."""
    return dpapi_unprotect(open(_codex_key_path(), "rb").read()).decode("utf-8")


def set_codex_key(key: str) -> None:
    os.makedirs(STATE_DIR, exist_ok=True)
    with open(_codex_key_path(), "wb") as f:
        f.write(dpapi_protect(key.encode("utf-8")))
