from __future__ import annotations

import json
from typing import Any

from asgiref.sync import async_to_sync
from django.core.cache import cache
from django.core.serializers.json import DjangoJSONEncoder
from django.utils import timezone
from channels.layers import get_channel_layer


REALTIME_METRICS_CACHE_KEY = "realtime.runtime.metrics"
REALTIME_METRICS_TIMEOUT = 60 * 60 * 24 * 30
DEFAULT_REALTIME_METRICS = {
    "connection_count": 0,
    "active_connections": 0,
    "subscription_count": 0,
    "active_subscriptions": 0,
    "message_count": 0,
    "failed_send_count": 0,
    "last_group": "",
    "last_event_type": "",
    "last_error": "",
    "last_event_at": "",
}


def make_channel_payload_safe(payload: dict[str, Any]) -> dict[str, Any]:
    return json.loads(json.dumps(payload, cls=DjangoJSONEncoder))


def get_realtime_metrics() -> dict[str, Any]:
    try:
        metrics = cache.get(REALTIME_METRICS_CACHE_KEY) or {}
    except Exception:
        metrics = {}
    return {
        **DEFAULT_REALTIME_METRICS,
        **{key: metrics.get(key) for key in DEFAULT_REALTIME_METRICS if key in metrics},
    }


def reset_realtime_metrics() -> dict[str, Any]:
    metrics = DEFAULT_REALTIME_METRICS.copy()
    try:
        cache.set(REALTIME_METRICS_CACHE_KEY, metrics, REALTIME_METRICS_TIMEOUT)
    except Exception:
        return metrics
    return metrics


def record_realtime_connection(delta: int = 1) -> None:
    metrics = get_realtime_metrics()
    if delta > 0:
        metrics["connection_count"] = int(metrics.get("connection_count", 0) or 0) + delta
    metrics["active_connections"] = max(0, int(metrics.get("active_connections", 0) or 0) + delta)
    metrics["last_event_at"] = timezone.now().isoformat()
    try:
        cache.set(REALTIME_METRICS_CACHE_KEY, metrics, REALTIME_METRICS_TIMEOUT)
    except Exception:
        return None


def record_realtime_subscription(delta: int = 1) -> None:
    metrics = get_realtime_metrics()
    if delta > 0:
        metrics["subscription_count"] = int(metrics.get("subscription_count", 0) or 0) + delta
    metrics["active_subscriptions"] = max(0, int(metrics.get("active_subscriptions", 0) or 0) + delta)
    metrics["last_event_at"] = timezone.now().isoformat()
    try:
        cache.set(REALTIME_METRICS_CACHE_KEY, metrics, REALTIME_METRICS_TIMEOUT)
    except Exception:
        return None


def record_realtime_send(group_name: str, payload: dict[str, Any], error: str = "") -> None:
    metrics = get_realtime_metrics()
    if error:
        metrics["failed_send_count"] = int(metrics.get("failed_send_count", 0) or 0) + 1
        metrics["last_error"] = error
    else:
        metrics["message_count"] = int(metrics.get("message_count", 0) or 0) + 1
        metrics["last_error"] = ""
    metrics["last_group"] = group_name
    metrics["last_event_type"] = str(payload.get("type") or "")
    metrics["last_event_at"] = timezone.now().isoformat()
    try:
        cache.set(REALTIME_METRICS_CACHE_KEY, metrics, REALTIME_METRICS_TIMEOUT)
    except Exception:
        return None


class WebSocketService:
    @staticmethod
    def _group_send(group_name: str, payload: dict[str, Any]) -> None:
        channel_layer = get_channel_layer()
        if channel_layer is None:
            return

        try:
            async_to_sync(channel_layer.group_send)(group_name, make_channel_payload_safe(payload))
        except Exception as exc:
            record_realtime_send(group_name, payload, error=str(exc))
            raise
        record_realtime_send(group_name, payload)

    @staticmethod
    def send_notification_to_user(user_id: int, notification_data: dict[Any, Any]) -> None:
        WebSocketService._group_send(
            f"notifications_{user_id}",
            {
                "type": "notification_message",
                "notification": notification_data,
            },
        )

    @staticmethod
    def broadcast_user_status(user_id: int, username: str, status: str) -> None:
        WebSocketService._group_send(
            "online_users",
            {
                "type": "user_status",
                "user_id": user_id,
                "username": username,
                "status": status,
            },
        )

    @staticmethod
    def send_typing_indicator(discussion_id: int, user_id: int, username: str, is_typing: bool) -> None:
        WebSocketService._group_send(
            f"discussion_{discussion_id}",
            {
                "type": "typing_indicator",
                "discussion_id": discussion_id,
                "user_id": user_id,
                "username": username,
                "is_typing": is_typing,
            },
        )

    @staticmethod
    def broadcast_discussion_event(discussion_id: int, event_type: str, payload: dict[str, Any]) -> None:
        WebSocketService._group_send(
            f"discussion_{discussion_id}",
            {
                "type": "forum_event_message",
                "event": {
                    "scope": "discussion",
                    "discussion_id": discussion_id,
                    "event_type": event_type,
                    "payload": payload,
                },
            },
        )
