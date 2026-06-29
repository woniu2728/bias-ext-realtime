from __future__ import annotations

from bias_core.extensions import WebSocketRoutesExtender

from bias_ext_realtime.backend.consumers import (
    DiscussionConsumer,
    ForumRealtimeConsumer,
    NotificationConsumer,
    OnlineUsersConsumer,
)


def websocket_routes_extender():
    return (
        WebSocketRoutesExtender()
        .route(r"ws/online/$", "realtime.online", OnlineUsersConsumer)
        .route(r"ws/forum/$", "realtime.forum", ForumRealtimeConsumer)
        .route(r"ws/discussions/(?P<discussion_id>\d+)/$", "realtime.discussion", DiscussionConsumer)
    )


def notification_websocket_routes_extender():
    return (
        WebSocketRoutesExtender()
        .route(r"ws/notifications/$", "realtime.notifications", NotificationConsumer)
    )
