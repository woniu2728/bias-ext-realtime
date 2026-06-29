from __future__ import annotations

from bias_core.extensions import FrontendExtender


def frontend_extender():
    return FrontendExtender(
        admin_entry="extensions/realtime/frontend/admin/index.js",
        forum_entry="extensions/realtime/frontend/forum/index.js",
    )
