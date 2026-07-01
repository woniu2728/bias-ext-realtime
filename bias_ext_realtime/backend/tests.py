from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch

from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from channels.testing import WebsocketCommunicator
from django.core.cache import cache
from django.test import TestCase, override_settings
from ninja_jwt.tokens import RefreshToken

from bias_ext_realtime.backend.consumers import ForumRealtimeConsumer
from bias_core.extensions.platform import DomainEvent
from bias_core.extensions.testing import (
    AuditLog,
    ExtensionRuntimeTestMixin,
    OnlineUserService,
    QueueService,
    Setting,
    build_extension_test_host,
    clear_runtime_setting_caches,
    get_enabled_extension_runtime_entries,
)
from bias_ext_realtime.backend.notification_dispatch import dispatch_notification_batch
from bias_ext_realtime.backend.websocket_service import WebSocketService
from bias_ext_realtime.backend.websocket_service import get_realtime_metrics, reset_realtime_metrics


def _runtime_facade(name: str):
    from importlib import import_module

    return getattr(import_module("bias_core.extensions.runtime"), name)


def get_runtime_user_model(*args, **kwargs):
    return _runtime_facade("get_runtime_user_model")(*args, **kwargs)


class RuntimeModelProxy:
    def __init__(self, resolver):
        self._resolver = resolver

    def __getattr__(self, name):
        return getattr(self._resolver(), name)


class NotificationCreatedEvent(DomainEvent):
    def __init__(self, notification_ids):
        self.notification_ids = tuple(notification_ids)


User = RuntimeModelProxy(get_runtime_user_model)


class RealtimeExtensionDiagnosticsTests(ExtensionRuntimeTestMixin, TestCase):
    def test_notification_integration_is_optional(self):
        application = build_extension_test_host("realtime")
        listener_names = {
            listener.handler.__name__
            for listener in application.events.get_listeners(extension_id="realtime")
        }
        websocket_route_names = {
            route.name
            for route in application.websocket_routes.get_routes(extension_id="realtime")
        }

        self.assertIsNone(application.get_service("notifications.service"))
        self.assertNotIn("dispatch_notification_batch", listener_names)
        self.assertNotIn("realtime.notifications", websocket_route_names)
        self.assertIn("realtime.online", websocket_route_names)
        self.assertIn("realtime.forum", websocket_route_names)
        self.assertIn("realtime.discussion", websocket_route_names)

    def test_notification_integration_registers_when_notifications_enabled(self):
        application = build_extension_test_host("notifications", "realtime")

        listeners = application.events.get_listeners(extension_id="realtime")
        listener_names = {
            listener.handler.__name__
            for listener in listeners
        }
        event_type_names = {
            getattr(listener.event_type, "__name__", str(listener.event_type))
            for listener in listeners
        }
        websocket_route_names = {
            route.name
            for route in application.websocket_routes.get_routes(extension_id="realtime")
        }

        self.assertIsNotNone(application.get_service("notifications.service"))
        self.assertIn("dispatch_notification_batch", listener_names)
        self.assertIn("NotificationCreatedEvent", event_type_names)
        self.assertIn("realtime.notifications", websocket_route_names)


class RealtimeWebSocketPayloadTests(TestCase):
    def setUp(self):
        reset_realtime_metrics()

    def test_discussion_event_payload_converts_datetime_before_channel_send(self):
        class DummyChannelLayer:
            def __init__(self):
                self.calls = []

            async def group_send(self, group_name, payload):
                self.calls.append((group_name, payload))

        channel_layer = DummyChannelLayer()
        created_at = datetime(2026, 6, 11, 15, 19, 37, tzinfo=timezone.utc)

        with patch("bias_ext_realtime.backend.websocket_service.get_channel_layer", return_value=channel_layer):
            WebSocketService.broadcast_discussion_event(
                7,
                "post.created",
                {"post": {"id": 3, "created_at": created_at}},
            )

        self.assertEqual(channel_layer.calls[0][0], "discussion_7")
        payload = channel_layer.calls[0][1]
        self.assertEqual(payload["event"]["payload"]["post"]["created_at"], "2026-06-11T15:19:37Z")

        metrics = get_realtime_metrics()
        self.assertEqual(metrics["message_count"], 1)
        self.assertEqual(metrics["failed_send_count"], 0)
        self.assertEqual(metrics["last_group"], "discussion_7")
        self.assertEqual(metrics["last_event_type"], "forum_event_message")


@override_settings(CHANNEL_LAYERS={"default": {"BACKEND": "channels.layers.InMemoryChannelLayer"}})
class RealtimeForumWebSocketConsumerSmokeTests(TestCase):
    def setUp(self):
        reset_realtime_metrics()
        self.user = User.objects.create_user(
            username="forum-ws-user",
            email="forum-ws-user@example.com",
            password="password123",
            is_email_confirmed=True,
        )

    def test_forum_websocket_subscribes_and_receives_discussion_channel_event(self):
        async_to_sync(self._run_forum_websocket_smoke)()

    async def _run_forum_websocket_smoke(self):
        communicator = WebsocketCommunicator(ForumRealtimeConsumer.as_asgi(), "/ws/forum/")
        communicator.scope["user"] = self.user

        with patch(
            "bias_ext_realtime.backend.consumers.resolve_realtime_visible_discussion_ids",
            return_value=[101],
        ):
            connected, _ = await communicator.connect()
            self.assertTrue(connected)

            initial = await communicator.receive_json_from()
            self.assertEqual(initial["type"], "connection_established")

            await communicator.send_json_to({
                "type": "subscribe_discussions",
                "discussion_ids": [101, 999],
            })
            subscribed = await communicator.receive_json_from()
            self.assertEqual(subscribed, {
                "type": "subscribed",
                "discussion_ids": [101],
            })
            self.assertEqual(get_realtime_metrics()["active_connections"], 1)
            self.assertEqual(get_realtime_metrics()["active_subscriptions"], 1)

            channel_layer = get_channel_layer()
            await channel_layer.group_send(
                "discussion_101",
                {
                    "type": "forum_event_message",
                    "event": {
                        "scope": "discussion",
                        "discussion_id": 101,
                        "event_type": "post.created",
                        "payload": {
                            "post": {
                                "id": 704,
                                "number": 3,
                                "content": "Realtime consumer smoke reply",
                            },
                        },
                    },
                },
            )
            event_message = await communicator.receive_json_from()
            self.assertEqual(event_message["type"], "forum_event")
            self.assertEqual(event_message["event"]["discussion_id"], 101)
            self.assertEqual(event_message["event"]["event_type"], "post.created")
            self.assertEqual(event_message["event"]["payload"]["post"]["content"], "Realtime consumer smoke reply")

        await communicator.disconnect()
        self.assertEqual(get_realtime_metrics()["active_connections"], 0)
        self.assertEqual(get_realtime_metrics()["active_subscriptions"], 0)


class RealtimeNotificationDispatchTests(TestCase):
    def test_notification_created_event_is_serialized_and_sent_to_user_channel(self):
        notification = SimpleNamespace(id=10, user_id=7)
        service = {
            "load_realtime_notifications": Mock(return_value=[notification]),
            "serialize_realtime_notification": Mock(return_value={
                "id": 10,
                "type": "discussionReply",
                "is_read": False,
            }),
        }

        with patch("bias_ext_realtime.backend.notification_dispatch.get_notification_service", return_value=service):
            with patch("bias_ext_realtime.backend.notification_dispatch.WebSocketService.send_notification_to_user") as send:
                dispatch_notification_batch(NotificationCreatedEvent(notification_ids=(10,)))

        service["load_realtime_notifications"].assert_called_once_with([10])
        service["serialize_realtime_notification"].assert_called_once_with(notification)
        send.assert_called_once_with(
            user_id=7,
            notification_data={
                "id": 10,
                "type": "discussionReply",
                "is_read": False,
            },
        )


class RealtimeForumSettingsTests(TestCase):
    def tearDown(self):
        clear_runtime_setting_caches()
        super().tearDown()

    def test_public_forum_settings_expose_realtime_typing_toggle(self):
        Setting.objects.update_or_create(
            key="extensions.realtime.typing_enabled",
            defaults={"value": json.dumps(False)},
        )
        clear_runtime_setting_caches()

        response = self.client.get("/api/forum")

        self.assertEqual(response.status_code, 200, response.content)
        self.assertFalse(response.json()["realtime_typing_enabled"])

    def test_runtime_entry_exposes_realtime_settings_schema(self):
        entries = get_enabled_extension_runtime_entries(product_visible_only=True)
        realtime = next(item for item in entries if item["id"] == "realtime")

        self.assertEqual(realtime["settings_values"]["typing_enabled"], True)
        self.assertEqual(realtime["forum_settings"]["realtime_typing_enabled"], True)


@override_settings(
    CACHES={"default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache", "LOCATION": "queue-reset-test"}},
    CHANNEL_LAYERS={"default": {"BACKEND": "channels.layers.InMemoryChannelLayer"}},
    CELERY_BROKER_URL="memory://",
)
class RealtimeQueueMetricsAdminApiTests(TestCase):
    def setUp(self):
        self.admin = User.objects.create_superuser(
            username="queue-admin",
            email="queue-admin@example.com",
            password="password123",
        )

    def auth_header(self, user=None):
        token = RefreshToken.for_user(user or self.admin).access_token
        return {"HTTP_AUTHORIZATION": f"Bearer {token}"}

    def test_admin_can_reset_queue_metrics(self):
        class DummyTask:
            name = "tests.reset_metric_task"

            def delay(self):
                raise AssertionError("queue should be disabled")

        QueueService.reset_metrics()
        QueueService.dispatch_celery_task(DummyTask(), fallback=lambda: "done")
        self.assertEqual(QueueService.get_metrics()["sync_count"], 1)

        response = self.client.post("/api/admin/queue/metrics/reset", **self.auth_header())

        self.assertEqual(response.status_code, 200, response.content)
        payload = response.json()
        self.assertEqual(payload["message"], "队列运行指标已重置")
        self.assertEqual(payload["metrics"]["sync_count"], 0)
        self.assertEqual(payload["metrics"]["enqueued_count"], 0)
        self.assertEqual(payload["metrics"]["fallback_count"], 0)
        audit_log = AuditLog.objects.get(action="admin.queue_metrics.reset")
        self.assertEqual(audit_log.user_id, self.admin.id)
        self.assertEqual(audit_log.target_type, "")

    def test_non_staff_cannot_reset_queue_metrics(self):
        member = User.objects.create_user(
            username="queue-reset-member",
            email="queue-reset-member@example.com",
            password="password123",
        )

        response = self.client.post(
            "/api/admin/queue/metrics/reset",
            **self.auth_header(member),
        )

        self.assertEqual(response.status_code, 403, response.content)


@override_settings(CACHES={
    "default": {
        "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
        "LOCATION": "bias-online-tests",
    }
})
class OnlineUserServiceTests(TestCase):
    def setUp(self):
        cache.clear()
        self.user = User.objects.create_user(
            username="online-user",
            email="online-user@example.com",
            password="password123",
            is_email_confirmed=True,
        )
        self.other_user = User.objects.create_user(
            username="online-other",
            email="online-other@example.com",
            password="password123",
            is_email_confirmed=True,
        )

    def test_multiple_connections_only_go_offline_after_last_disconnect(self):
        self.assertTrue(OnlineUserService.mark_user_online(self.user.id))
        self.assertFalse(OnlineUserService.mark_user_online(self.user.id))
        self.assertEqual(OnlineUserService.get_online_user_ids(), [self.user.id])

        self.assertFalse(OnlineUserService.mark_user_offline(self.user.id))
        self.assertEqual(OnlineUserService.get_online_user_ids(), [self.user.id])

        self.assertTrue(OnlineUserService.mark_user_offline(self.user.id))
        self.assertEqual(OnlineUserService.get_online_user_ids(), [])

    def test_touch_extends_presence_ttl(self):
        with patch.object(OnlineUserService, "_now_ts", return_value=100):
            OnlineUserService.mark_user_online(self.user.id)

        with patch.object(OnlineUserService, "_now_ts", return_value=150):
            self.assertTrue(OnlineUserService.touch_user_online(self.user.id))

        with patch.object(OnlineUserService, "_now_ts", return_value=200):
            self.assertEqual(OnlineUserService.get_online_user_ids(), [self.user.id])

        with patch.object(OnlineUserService, "_now_ts", return_value=241):
            self.assertEqual(OnlineUserService.get_online_user_ids(), [])

    def test_get_online_users_returns_only_marked_users(self):
        OnlineUserService.mark_user_online(self.other_user.id)
        OnlineUserService.mark_user_online(self.user.id)

        users = OnlineUserService.get_online_users(limit=10)

        self.assertEqual({item["id"] for item in users}, {self.user.id, self.other_user.id})
        self.assertTrue(all("username" in item for item in users))

