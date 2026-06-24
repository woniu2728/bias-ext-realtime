from django.apps import AppConfig

class RealtimeExtensionConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "bias_ext_realtime.backend"
    label = "realtime"
