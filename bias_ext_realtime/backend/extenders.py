from __future__ import annotations

from bias_core.extensions import ApiRoutesExtender, ConditionalExtender, EventListenersExtender, LifecycleExtender

from bias_ext_realtime.backend.admin_api import router as realtime_admin_router
from bias_ext_realtime.backend.frontend import frontend_extender
from bias_ext_realtime.backend.listener_contracts import event_listener_definitions
from bias_ext_realtime.backend.realtime_contracts import realtime_extender
from bias_ext_realtime.backend.settings_contracts import settings_extender
from bias_ext_realtime.backend.websocket_contracts import notification_websocket_routes_extender, websocket_routes_extender


def frontend_extenders():
    return (frontend_extender(),)


def admin_extenders():
    return (
        ApiRoutesExtender(
            mounts=(("/admin", realtime_admin_router),),
            tags=("Admin",),
        ),
    )


def settings_extenders():
    return (settings_extender(),)


def event_extenders():
    return (
        realtime_extender(),
        websocket_routes_extender(),
    )


def notification_integration_extenders():
    return (
        EventListenersExtender(
            listeners=event_listener_definitions(),
        ),
        notification_websocket_routes_extender(),
    )


def optional_integration_extenders():
    return (
        ConditionalExtender().when_extension_enabled("notifications", notification_integration_extenders),
    )


def lifecycle_extenders():
    return (LifecycleExtender(),)
