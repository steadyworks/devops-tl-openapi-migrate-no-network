# app/webhook.py
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, Optional
from uuid import UUID

from fastapi import HTTPException, Request
from pydantic import BaseModel

from backend.db.dal import DALEntitlements
from backend.db.dal.base import FilterOp, safe_transaction
from backend.db.dal.schemas import DAOEntitlementsCreate, DAOEntitlementsUpdate
from backend.env_loader import EnvLoader
from backend.route_handler.base import (
    RouteHandler,
    enforce_response_model,
    unauthenticated_route,
)

if TYPE_CHECKING:
    from backend.db.data_models import DAOEntitlements


class WebhookAck(BaseModel):
    received: bool = True


AUTH_HEADER_NAME = EnvLoader.get(
    "REVENUECAT_WEBHOOK_AUTH_HEADER", "Authorization"
)
REVENUECAT_WEBHOOK_AUTH = EnvLoader.get("REVENUECAT_WEBHOOK_AUTH")


def verify_authorization(request: Request) -> None:
    """
    Verifies the Authorization header matches the configured secret.
    401 if missing or mismatched.
    """
    incoming = request.headers.get(AUTH_HEADER_NAME, "")
    if not REVENUECAT_WEBHOOK_AUTH:
        # Misconfiguration on our side
        raise HTTPException(
            status_code=500, detail="webhook auth not configured"
        )
    print(incoming, REVENUECAT_WEBHOOK_AUTH)
    if incoming.strip() != f"Bearer {REVENUECAT_WEBHOOK_AUTH}":
        raise HTTPException(status_code=401, detail="unauthorized webhook")


def _ms_to_dt(ms: Optional[int]) -> Optional[datetime]:
    if ms is None:
        return None
    # ms since epoch → aware UTC dt
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def _event_to_state(
    payload: Dict[str, Any],
) -> tuple[bool, Optional[datetime]]:
    """
    Translate RevenueCat event types into (active, expires_at).
    Adjust if your webhook version differs.
    """
    event = payload.get("event", {})
    typ = (event.get("type") or "").upper()

    expires_at = _ms_to_dt(event.get("expiration_at_ms"))

    # Reasonable defaults:
    # INITIAL_PURCHASE / RENEWAL / UNCANCELLATION -> active
    # EXPIRATION / CANCELLATION (effective at period end) -> active until expiration; after that it will be false
    # PRODUCT_CHANGE -> still active (carries a new expiration) unless downgrade at period end
    # TRANSFER -> no change to active itself (still respect expiration)

    if typ in {"INITIAL_PURCHASE", "RENEWAL", "UNCANCELLATION"}:
        return True, expires_at

    if typ in {"CANCELLATION"}:
        # Usually access remains until expiration
        return True, expires_at

    if typ in {"EXPIRATION"}:
        # Sub actually expired
        return False, expires_at

    if typ in {"PRODUCT_CHANGE", "BILLING_ISSUE"}:
        # Keep active and let expires_at speak
        return True, expires_at

    # Fallback: keep conservative
    return (
        bool(expires_at and expires_at > datetime.now(timezone.utc)),
        expires_at,
    )


class RevenueCatWebhookAPIHandler(RouteHandler):
    def register_routes(self) -> None:
        self.route("/api/webhooks/revenuecat", "rc_webhook", methods=["POST"])

    @unauthenticated_route
    @enforce_response_model
    async def rc_webhook(self, request: Request) -> WebhookAck:
        verify_authorization(request)

        payload = await request.json()
        # Here you would process the payload as needed
        print(f"Received RevenueCat webhook: {payload}")
        event = payload.get("event", {})
        app_user_id = event.get("app_user_id")
        product_id = event.get("product_id")
        try:
            user_id = UUID(str(app_user_id))
        except Exception:
            # If your app_user_id isn't a UUID, adapt here (e.g., lookup by external id)
            # For now, skip gracefully
            # logger.warning("Invalid app_user_id for webhook", extra={"app_user_id": app_user_id})
            return WebhookAck()
        active, expires_at = _event_to_state(payload)
        async with self.app.db_session_factory.new_session() as session:
            async with safe_transaction(
                session,
                "entitlement update from revenuecat webhook",
                raise_on_fail=True,
            ):
                # 1. find or create entitlement record
                ents: list[DAOEntitlements] = await DALEntitlements.list_all(
                    session, filters={"user_id": (FilterOp.EQ, user_id)}
                )
                exists = len(ents) > 0
                if exists:
                    await DALEntitlements.update_by_id(
                        session,
                        ents[0].id,
                        DAOEntitlementsUpdate(
                            key=product_id,  # "pro-yearly"
                            active=active,
                            expires_at=expires_at,
                        ),
                    )
                else:
                    await DALEntitlements.create(
                        session,
                        DAOEntitlementsCreate(
                            user_id=user_id,
                            key=product_id,  # "pro-yearly"
                            active=active,
                            expires_at=expires_at,
                        ),
                    )

        return WebhookAck()


"""
Test with:
curl -i -X POST http://localhost:8000/api/webhooks/revenuecat \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer 456596f9a30b7b2dcfbd495f89c37ce3cd5e9ce91558de71c5a0b79d4a50b9cb' \
  --data '{"event":{"type":"INITIAL_PURCHASE","app_user_id":"e02908e3-6dbb-4ada-bee3-884ec98f733c","product_id":"pro_yearly","expiration_at_ms":1893456000000}}'
"""
