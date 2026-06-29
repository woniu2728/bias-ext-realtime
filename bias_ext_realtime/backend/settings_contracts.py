from __future__ import annotations

from bias_core.extensions import SettingsExtender, setting_field


def settings_extender():
    return (
        SettingsExtender(fields=setting_definitions())
        .default("typing_enabled", True)
        .serialize_to_forum("realtime_typing_enabled", "typing_enabled", bool)
    )


def setting_definitions():
    return (
        setting_field({
            "key": "typing_enabled",
            "label": "回复输入提示",
            "type": "boolean",
            "default": True,
            "help_text": "关闭后，讨论详情页不再广播正在输入状态；其他实时通知与更新不受影响。",
            "order": 10,
        }),
    )
