from __future__ import annotations

from bias_core.extensions import RealtimeExtender

from bias_ext_realtime.backend.websocket_service import WebSocketService


def realtime_extender():
    return RealtimeExtender().discussion_transport(
        "realtime.discussion.websocket",
        WebSocketService.broadcast_discussion_event,
        description="通过 WebSocket 广播讨论实时事件。",
    )
