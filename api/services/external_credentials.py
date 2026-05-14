from __future__ import annotations

import base64
import hashlib
from datetime import datetime, timedelta, timezone

import requests
from cryptography.fernet import Fernet, InvalidToken
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.config import get_settings
from api.models import ExternalUserCredential, User
from api.schemas import ExternalUserCredentialSummary, ExternalUserCredentialTestResult
from api.services.external_eln_clients import LabguruClient, normalize_system_key


def external_credential_summary(
    credential: ExternalUserCredential | None,
    *,
    system_key: str,
) -> ExternalUserCredentialSummary:
    normalized = normalize_system_key(system_key)
    if credential is None:
        return ExternalUserCredentialSummary(system_key=normalized, status="missing")
    status = credential_status(credential)
    return ExternalUserCredentialSummary(
        id=credential.id,
        system_key=credential.system_key,
        credential_kind=credential.credential_kind,
        status=status,
        expires_at=credential.expires_at,
        last_verified_at=credential.last_verified_at,
        last_error=credential.last_error,
        days_until_expiry=days_until_expiry(credential.expires_at),
        created_at=credential.created_at,
        updated_at=credential.updated_at,
    )


def get_user_credential(
    session: Session,
    *,
    user: User,
    system_key: str,
) -> ExternalUserCredential | None:
    normalized = normalize_system_key(system_key)
    return session.scalars(
        select(ExternalUserCredential).where(
            ExternalUserCredential.user_id == user.id,
            ExternalUserCredential.system_key == normalized,
        )
    ).first()


def upsert_user_credential(
    session: Session,
    *,
    user: User,
    system_key: str,
    token: str,
    expires_in_days: int = 30,
) -> ExternalUserCredential:
    normalized = normalize_system_key(system_key)
    clean_token = str(token or "").strip()
    if not clean_token:
        raise ValueError("Token is required")
    credential = get_user_credential(session, user=user, system_key=normalized)
    now = datetime.now(timezone.utc)
    if credential is None:
        credential = ExternalUserCredential(user_id=user.id, system_key=normalized, encrypted_token="")
        session.add(credential)
    credential.credential_kind = "api_token"
    credential.encrypted_token = encrypt_external_token(clean_token)
    credential.status = "stored"
    credential.expires_at = now + timedelta(days=max(1, int(expires_in_days)))
    credential.last_verified_at = None
    credential.last_error = None
    credential.updated_at = now
    session.flush()
    return credential


def delete_user_credential(session: Session, *, user: User, system_key: str) -> bool:
    credential = get_user_credential(session, user=user, system_key=system_key)
    if credential is None:
        return False
    session.delete(credential)
    session.flush()
    return True


def decrypt_user_credential_token(credential: ExternalUserCredential) -> str:
    try:
        return decrypt_external_token(credential.encrypted_token)
    except InvalidToken as exc:
        raise ValueError("Stored credential cannot be decrypted with the current server secret") from exc


def test_user_credential(
    session: Session,
    *,
    user: User,
    system_key: str,
) -> ExternalUserCredentialTestResult:
    normalized = normalize_system_key(system_key)
    credential = get_user_credential(session, user=user, system_key=normalized)
    if credential is None:
        raise LookupError("No token stored for this external system")
    try:
        token = decrypt_user_credential_token(credential)
    except ValueError as exc:
        return update_credential_test_status(
            session,
            credential=credential,
            status="invalid",
            message=str(exc),
        )
    if normalized == "labguru":
        return test_labguru_credential(session, credential=credential, token=token)
    return update_credential_test_status(
        session,
        credential=credential,
        status="unsupported",
        message=f"{normalized} credential testing is not implemented yet.",
    )


def test_labguru_credential(
    session: Session,
    *,
    credential: ExternalUserCredential,
    token: str,
) -> ExternalUserCredentialTestResult:
    settings = get_settings()
    if not settings.labguru_base_url.strip():
        return update_credential_test_status(
            session,
            credential=credential,
            status="invalid",
            message="Labguru base URL is not configured.",
        )
    client = LabguruClient(base_url=settings.labguru_base_url, token=token, timeout_seconds=10)
    try:
        client._request_json("/api/v2/experiments", fallback_endpoint="/api/v1/experiments.json", page=1, page_size=1)
    except requests.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else "unknown"
        return update_credential_test_status(
            session,
            credential=credential,
            status="invalid",
            message=f"Labguru rejected the token ({status_code}).",
        )
    except requests.RequestException as exc:
        return update_credential_test_status(
            session,
            credential=credential,
            status="invalid",
            message=f"Labguru request failed: {exc}",
        )
    return update_credential_test_status(
        session,
        credential=credential,
        status="connected",
        message="Labguru token verified.",
    )


def update_credential_test_status(
    session: Session,
    *,
    credential: ExternalUserCredential,
    status: str,
    message: str,
) -> ExternalUserCredentialTestResult:
    now = datetime.now(timezone.utc)
    credential.status = status
    credential.last_verified_at = now
    credential.last_error = None if status == "connected" else message
    credential.updated_at = now
    session.flush()
    return ExternalUserCredentialTestResult(
        system_key=credential.system_key,
        status=credential_status(credential),
        message=message,
        expires_at=credential.expires_at,
        last_verified_at=credential.last_verified_at,
    )


def credential_status(credential: ExternalUserCredential) -> str:
    if credential.expires_at is not None and credential.expires_at <= datetime.now(timezone.utc):
        return "expired"
    return credential.status or "stored"


def days_until_expiry(expires_at: datetime | None) -> int | None:
    if expires_at is None:
        return None
    delta = expires_at - datetime.now(timezone.utc)
    return max(0, delta.days)


def encrypt_external_token(token: str) -> str:
    return credential_fernet().encrypt(token.encode("utf-8")).decode("ascii")


def decrypt_external_token(encrypted_token: str) -> str:
    return credential_fernet().decrypt(str(encrypted_token).encode("ascii")).decode("utf-8")


def credential_fernet() -> Fernet:
    settings = get_settings()
    secret = settings.external_credentials_secret.strip() or settings.database_url
    digest = hashlib.sha256(secret.encode("utf-8")).digest()
    key = base64.urlsafe_b64encode(digest)
    return Fernet(key)
