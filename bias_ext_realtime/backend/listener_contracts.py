from __future__ import annotations

from bias_core.extensions import ExtensionEventListenerDefinition

from bias_ext_realtime.backend.notification_dispatch import dispatch_notification_batch


def event_listener_definitions():
    return (
        ExtensionEventListenerDefinition(
            event_type="notifications.notification.created",
            handler=dispatch_notification_batch,
            description="通知创建后通过 WebSocket 批量推送到前端。",
        ),
    )
