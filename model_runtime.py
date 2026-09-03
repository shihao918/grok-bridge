"""Explicit model binding and execution for the local Grok Bot bridge.

The default binding mirrors the active Codex provider identity without copying a
secret into this repository.  A binding is configuration intent; a successful
HTTP response is separate execution evidence.
"""

from __future__ import annotations

import argparse
import ipaddress
import json
import os
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Mapping
from urllib.parse import urlsplit

import httpx


DEFAULT_CODEX_CONFIG = Path.home() / ".codex" / "config.toml"
DEFAULT_MODEL_BACKEND = "codex"
DEFAULT_RESPONSES_MODEL = "gpt-5.6-sol"
DEFAULT_RESPONSES_REASONING_EFFORT = "xhigh"
DEFAULT_RESPONSES_AUTH_ENV = "SUB2API_USER_API_KEY"
DEFAULT_OLLAMA_URL = "http://127.0.0.1:11434"
DEFAULT_OLLAMA_MODEL = "lfm2.5:8b-a1b"
ALLOW_INSECURE_REMOTE_HTTP_ENV = "GROK_ALLOW_INSECURE_REMOTE_HTTP"


class ModelConfigurationError(RuntimeError):
    """The selected model route is incomplete or protocol-incompatible."""


class ModelExecutionError(RuntimeError):
    """The selected model route failed without falling back to another route."""


@dataclass(frozen=True)
class ModelBinding:
    backend: str
    source: str
    provider_key: str
    base_url: str
    model: str
    wire_api: str
    reasoning_effort: str = ""
    auth_env: str = ""

    def safe_dict(self, *, auth_available: bool | None = None) -> dict:
        result = {
            "backend": self.backend,
            "source": self.source,
            "providerKey": self.provider_key,
            "baseUrl": self.base_url,
            "model": self.model,
            "wireApi": self.wire_api,
            "reasoningEffort": self.reasoning_effort or None,
            "authEnv": self.auth_env or None,
        }
        if auth_available is not None:
            result["authAvailable"] = auth_available
        return result


def _text(value) -> str:
    return str(value or "").strip()


def _validated_base_url(value: str, *, label: str) -> str:
    base_url = _text(value).rstrip("/")
    parsed = urlsplit(base_url)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise ModelConfigurationError(f"{label} must be an absolute HTTP(S) base URL without credentials")
    return base_url


def _truthy_environment_value(value: str | None) -> bool:
    return _text(value).lower() in {"1", "true", "yes", "on"}


def _is_loopback_host(hostname: str | None) -> bool:
    host = _text(hostname).lower().rstrip(".")
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def responses_transport_state(
    binding: ModelBinding,
    *,
    environ: Mapping[str, str] | None = None,
) -> tuple[bool, bool, bool]:
    parsed = urlsplit(binding.base_url)
    secure = parsed.scheme == "https" or (parsed.scheme == "http" and _is_loopback_host(parsed.hostname))
    env = os.environ if environ is None else environ
    explicit_insecure_opt_in = _truthy_environment_value(env.get(ALLOW_INSECURE_REMOTE_HTTP_ENV))
    return secure, secure or explicit_insecure_opt_in, explicit_insecure_opt_in


def require_responses_transport(
    binding: ModelBinding,
    *,
    environ: Mapping[str, str] | None = None,
) -> None:
    secure, allowed, _ = responses_transport_state(binding, environ=environ)
    if secure or allowed:
        return
    raise ModelConfigurationError(
        "Responses API refuses non-loopback HTTP because it would expose the Bearer key; "
        f"use HTTPS/loopback or explicitly set {ALLOW_INSECURE_REMOTE_HTTP_ENV}=1"
    )


def _effective_codex_config(raw: dict) -> dict:
    effective = dict(raw)
    profile_name = _text(raw.get("profile"))
    profile = (raw.get("profiles") or {}).get(profile_name) if profile_name else None
    if isinstance(profile, dict):
        effective.update(profile)
    return effective


def resolve_codex_binding(config_path: str | os.PathLike[str] | None = None) -> ModelBinding:
    path = Path(config_path or DEFAULT_CODEX_CONFIG).expanduser()
    try:
        with path.open("rb") as handle:
            raw = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise ModelConfigurationError(f"unable to read Codex config at {path}: {type(exc).__name__}") from exc

    effective = _effective_codex_config(raw)
    provider_key = _text(effective.get("model_provider"))
    provider = (raw.get("model_providers") or {}).get(provider_key)
    if not provider_key or not isinstance(provider, dict):
        raise ModelConfigurationError("Codex config does not select a valid model_provider block")

    wire_api = _text(provider.get("wire_api")).lower()
    if wire_api != "responses":
        raise ModelConfigurationError(
            f"Codex provider {provider_key!r} uses wire_api={wire_api or '<missing>'}; "
            "Grok Bot refuses cross-wire fallback"
        )

    model = _text(effective.get("model"))
    auth_env = _text(provider.get("env_key"))
    if not model:
        raise ModelConfigurationError("Codex config does not select a model")
    if not auth_env:
        raise ModelConfigurationError(
            f"Codex provider {provider_key!r} must use an env_key; inline API keys are not imported"
        )

    return ModelBinding(
        backend="responses",
        source=f"codex_config:{path}",
        provider_key=provider_key,
        base_url=_validated_base_url(provider.get("base_url"), label="Codex provider base_url"),
        model=model,
        wire_api=wire_api,
        reasoning_effort=_text(effective.get("model_reasoning_effort")),
        auth_env=auth_env,
    )


def resolve_model_binding(
    config: Mapping | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    codex_config_path: str | os.PathLike[str] | None = None,
) -> ModelBinding:
    cfg = dict(config or {})
    env = os.environ if environ is None else environ
    backend = _text(env.get("GROK_MODEL_BACKEND") or cfg.get("model_backend") or DEFAULT_MODEL_BACKEND).lower()

    if backend == "codex":
        configured_path = env.get("GROK_CODEX_CONFIG_PATH") or codex_config_path
        return resolve_codex_binding(configured_path)

    if backend == "responses":
        base_url = env.get("GROK_RESPONSES_BASE_URL") or cfg.get("responses_base_url") or cfg.get("gateway")
        return ModelBinding(
            backend="responses",
            source="explicit_responses",
            provider_key=_text(env.get("GROK_RESPONSES_PROVIDER_KEY") or cfg.get("responses_provider_key") or "explicit"),
            base_url=_validated_base_url(base_url, label="Responses base URL"),
            model=_text(env.get("GROK_RESPONSES_MODEL") or cfg.get("responses_model") or DEFAULT_RESPONSES_MODEL),
            wire_api="responses",
            reasoning_effort=_text(
                env.get("GROK_RESPONSES_REASONING_EFFORT")
                or cfg.get("responses_reasoning_effort")
                or DEFAULT_RESPONSES_REASONING_EFFORT
            ),
            auth_env=_text(
                env.get("GROK_RESPONSES_AUTH_ENV")
                or cfg.get("responses_auth_env")
                or DEFAULT_RESPONSES_AUTH_ENV
            ),
        )

    if backend == "ollama":
        return ModelBinding(
            backend="ollama",
            source="explicit_ollama",
            provider_key="ollama",
            base_url=_validated_base_url(
                env.get("GROK_OLLAMA_URL") or cfg.get("ollama_url") or DEFAULT_OLLAMA_URL,
                label="Ollama base URL",
            ),
            model=_text(env.get("GROK_OLLAMA_MODEL") or cfg.get("ollama_model") or DEFAULT_OLLAMA_MODEL),
            wire_api="ollama_chat",
        )

    raise ModelConfigurationError(
        f"unsupported model_backend={backend or '<empty>'}; expected codex, responses, or ollama"
    )


def _registry_environment_value(name: str) -> str:
    if os.name != "nt":
        return ""
    try:
        import winreg
    except ImportError:
        return ""

    locations = (
        (winreg.HKEY_CURRENT_USER, r"Environment"),
        (winreg.HKEY_LOCAL_MACHINE, r"SYSTEM\CurrentControlSet\Control\Session Manager\Environment"),
    )
    for hive, subkey in locations:
        try:
            with winreg.OpenKey(hive, subkey) as key:
                value, _ = winreg.QueryValueEx(key, name)
        except OSError:
            continue
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def read_auth_value(name: str, *, environ: Mapping[str, str] | None = None) -> str:
    if not name:
        return ""
    env = os.environ if environ is None else environ
    value = _text(env.get(name))
    if value or environ is not None:
        return value
    return _registry_environment_value(name)


def model_runtime_summary(
    config: Mapping | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    codex_config_path: str | os.PathLike[str] | None = None,
) -> dict:
    binding = resolve_model_binding(
        config,
        environ=environ,
        codex_config_path=codex_config_path,
    )
    auth_available = None
    if binding.auth_env:
        auth_available = bool(read_auth_value(binding.auth_env, environ=environ))
    result = binding.safe_dict(auth_available=auth_available)
    if binding.backend == "responses":
        secure, allowed, explicit_insecure_opt_in = responses_transport_state(binding, environ=environ)
        result["transportSecure"] = secure
        result["transportAllowed"] = allowed
        result["insecureRemoteHttpOptIn"] = explicit_insecure_opt_in
    return result


def responses_endpoint(base_url: str) -> str:
    return base_url if base_url.endswith("/responses") else f"{base_url}/responses"


def extract_responses_text(payload: object) -> str:
    if not isinstance(payload, dict):
        return ""
    direct = payload.get("output_text")
    if isinstance(direct, str) and direct.strip():
        return direct.strip()

    parts = []
    for output in payload.get("output") or []:
        if not isinstance(output, dict):
            continue
        for content in output.get("content") or []:
            if not isinstance(content, dict):
                continue
            text = content.get("text")
            if isinstance(text, dict):
                text = text.get("value")
            if isinstance(text, str) and text.strip():
                parts.append(text.strip())
    return "\n".join(parts)


def _request_failure(label: str, binding: ModelBinding, exc: Exception) -> ModelExecutionError:
    status = getattr(getattr(exc, "response", None), "status_code", None)
    detail = f"HTTP {status}" if status else f"{type(exc).__name__}: {_text(exc)[:160]}"
    return ModelExecutionError(
        f"{label} request failed for provider={binding.provider_key} model={binding.model}: {detail}"
    )


def execute_model(
    binding: ModelBinding,
    task: str,
    *,
    post: Callable = httpx.post,
    timeout: float = 300,
    environ: Mapping[str, str] | None = None,
) -> str:
    if binding.backend == "responses":
        require_responses_transport(binding, environ=environ)
        auth_value = read_auth_value(binding.auth_env, environ=environ)
        if not auth_value:
            raise ModelConfigurationError(
                f"required auth environment variable {binding.auth_env!r} is unavailable"
            )
        body = {
            "model": binding.model,
            "input": task,
            "stream": False,
            "store": False,
        }
        if binding.reasoning_effort:
            body["reasoning"] = {"effort": binding.reasoning_effort}
        try:
            response = post(
                responses_endpoint(binding.base_url),
                headers={"Authorization": f"Bearer {auth_value}"},
                json=body,
                timeout=timeout,
                trust_env=False,
            )
            response.raise_for_status()
            content = extract_responses_text(response.json())
        except ModelConfigurationError:
            raise
        except Exception as exc:
            raise _request_failure("Responses API", binding, exc) from exc
        if not content:
            raise ModelExecutionError(
                f"Responses API returned no output text for provider={binding.provider_key} model={binding.model}"
            )
        return content

    if binding.backend == "ollama":
        try:
            response = post(
                f"{binding.base_url}/api/chat",
                json={
                    "model": binding.model,
                    "messages": [{"role": "user", "content": task}],
                    "stream": False,
                },
                timeout=timeout,
                trust_env=False,
            )
            response.raise_for_status()
            content = _text((response.json().get("message") or {}).get("content"))
        except Exception as exc:
            raise _request_failure("Ollama", binding, exc) from exc
        if not content:
            raise ModelExecutionError(f"Ollama returned no message.content for model={binding.model}")
        return content

    raise ModelConfigurationError(f"cannot execute unsupported backend={binding.backend!r}")


def _main() -> int:
    parser = argparse.ArgumentParser(description="Print the Grok Bot model binding without secret values")
    parser.add_argument("--backend", choices=("codex", "responses", "ollama"))
    parser.add_argument("--codex-config", help="Codex-compatible TOML used for the model binding")
    parser.add_argument("--require-auth", action="store_true")
    args = parser.parse_args()

    import bridge_common as bc

    if args.backend:
        os.environ["GROK_MODEL_BACKEND"] = args.backend
    try:
        summary = model_runtime_summary(bc.config(), codex_config_path=args.codex_config)
    except ModelConfigurationError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False))
        return 2
    summary["ok"] = not (
        args.require_auth
        and (
            summary.get("authAvailable") is False
            or summary.get("transportAllowed") is False
        )
    )
    if args.require_auth and summary.get("transportAllowed") is False:
        summary["error"] = (
            "Responses API refuses non-loopback HTTP because it would expose the Bearer key; "
            f"use HTTPS/loopback or explicitly set {ALLOW_INSECURE_REMOTE_HTTP_ENV}=1"
        )
    print(json.dumps(summary, ensure_ascii=False))
    return 0 if summary["ok"] else 3


if __name__ == "__main__":
    sys.exit(_main())
