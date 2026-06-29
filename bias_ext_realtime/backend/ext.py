from bias_ext_realtime.backend.extenders import (
    admin_extenders,
    event_extenders,
    frontend_extenders,
    lifecycle_extenders,
    optional_integration_extenders,
    settings_extenders,
)


def extend():
    return [
        *frontend_extenders(),
        *admin_extenders(),
        *settings_extenders(),
        *event_extenders(),
        *optional_integration_extenders(),
        *lifecycle_extenders(),
    ]
