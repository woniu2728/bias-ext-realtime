from __future__ import annotations

from bias_ext_realtime.backend.websocket_service import WebSocketService


def get_runtime_notification_service(*args, **kwargs):
    from bias_core.extensions.runtime import get_runtime_notification_service as runtime_get_notification_service

    return runtime_get_notification_service(*args, **kwargs)


def dispatch_notification_batch(
    event,
    *,
    notification_service=None,
    websocket_service=WebSocketService,
) -> None:
    notification_ids = tuple(int(item) for item in event.notification_ids if item)
    if not notification_ids:
        return

    service = notification_service
    if service is None:
        service = get_runtime_notification_service()
    if service is None:
        return
    loader = _runtime_method(service, "load_realtime_notifications")
    serializer = _runtime_method(service, "serialize_realtime_notification")
    if loader is None or serializer is None:
        return
    for notification in loader(list(notification_ids)) or []:
        try:
            websocket_service.send_notification_to_user(
                user_id=notification.user_id,
                notification_data=serializer(notification),
            )
        except Exception:
            continue


def _runtime_method(service, name: str):
    if isinstance(service, dict):
        method = service.get(name)
    else:
        method = getattr(service, name, None)
    return method if callable(method) else None
