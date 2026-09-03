import io
import base64
import json
import os
import struct
import tempfile
import threading
import time
import urllib.error
import urllib.request
import unittest
from concurrent.futures import ThreadPoolExecutor

import backend_server as backend
from backend_server import (
    Handler,
    SAND_MACHINE,
    TRANSCRIPT_CHANGED,
    TRANSCRIPTS,
    AGENT_INDEX,
    PROMPT_ACCEPTANCE,
    RUNTIME_LOG,
    SSE_SUBSCRIBERS,
    _PROTO_SCHEMAS,
    _broadcast_transcript_event,
    _first_agent_id,
    _resolve_agent_id,
    _observe_sand_machine,
    _proto_decode_message,
    _proto_encode_message,
    _redact_log_body,
    _gateway_transcript_entry,
    _transcript_event,
)


GATEWAY_AGENT_REQUIRED_FIELDS_036 = {
    "avatarDataUrl",
    "awaitingUserResponse",
    "createdAt",
    "description",
    "hasUnread",
    "id",
    "isActive",
    "isComposingMessage",
    "isGroup",
    "isRunning",
    "lastEntry",
    "lastMessageId",
    "memberIds",
    "name",
    "notificationsEnabled",
    "notifyOnUpdatesEnabled",
    "origin",
    "path",
    "updatedAt",
}

GATEWAY_AGENT_ALLOWED_FIELDS_036 = GATEWAY_AGENT_REQUIRED_FIELDS_036 | {
    "activeGroupMemberId",
    "avatarColor",
    "avatarShape",
    "avatarVersion",
    "boxHandoff",
    "currentActivity",
    "harness",
    "isHiddenFromSidebar",
    "isRetrying",
    "isRunningTurn",
    "lastActivityAt",
    "lastMessageAuthorId",
    "lastMessagePreview",
    "lastMessagePreviewSource",
    "lastTurnSettlement",
    "newestEntryId",
    "purpose",
    "pushMessageContent",
    "runningSessionIds",
    "serverId",
    "slackConnected",
    "snapshotEpoch",
    "snapshotSeq",
    "teamId",
    "title",
    "unreadCount",
    "viewerIsOwner",
    "viewerSessionId",
    "visibility",
    "voiceId",
}


class ConnectStreamFramingTests(unittest.TestCase):
    def test_reply_ignores_client_disconnect_during_body_write(self):
        class FailingWriter:
            def write(self, data):
                raise ConnectionAbortedError(10053, "client closed")

        handler = object.__new__(Handler)
        handler.wfile = FailingWriter()
        handler._request_is_proto = lambda: False
        handler.send_response = lambda status: None
        handler.send_header = lambda key, value: None
        handler.end_headers = lambda: None

        Handler._reply(handler, 200, {"ok": True})

    def test_runtime_and_request_logs_are_separate(self):
        self.assertNotEqual(backend.LOG, RUNTIME_LOG)

    def test_log_redacts_sensitive_request_bodies(self):
        oauth = _redact_log_body(
            "/oauth/token",
            b'{"refresh_token":"refresh-secret","client_id":"bridge"}',
        )
        self.assertNotIn("refresh-secret", oauth)
        self.assertIn("redacted", oauth)

        ordinary = _redact_log_body(
            "/api/health",
            b'{"access_token":"access-secret","prompt":"hello"}',
        )
        self.assertNotIn("access-secret", ordinary)
        self.assertIn("<redacted>", ordinary)

    def test_end_stream_writes_explicit_connect_end_envelope(self):
        handler = object.__new__(Handler)
        handler.wfile = io.BytesIO()

        Handler._connect_stream_end(handler)

        raw = handler.wfile.getvalue()
        size_text, framed = raw.split(b"\r\n", 1)
        size = int(size_text, 16)
        envelope = framed[:size]
        self.assertEqual(size, 7)
        self.assertEqual(envelope[0], 0x02)
        self.assertEqual(struct.unpack(">I", envelope[1:5])[0], 2)
        self.assertEqual(envelope[5:], b"{}")
        self.assertEqual(framed[size:], b"\r\n0\r\n\r\n")

    def test_machine_id_is_bound_from_cursor_checksum_suffix(self):
        machine_id = "af366c21-2c0a-4545-a4a6-96f737935e70"
        SAND_MACHINE["machineId"] = ""

        observed = _observe_sand_machine({"x-cursor-checksum": "12345678" + machine_id})

        self.assertEqual(observed, machine_id)

    def test_machine_roster_proto_round_trip(self):
        machine = {
            "machineId": "af366c21-2c0a-4545-a4a6-96f737935e70",
            "label": "local-machine",
            "localToolPermission": "ask",
        }
        encoded = _proto_encode_message({"machines": [machine]}, _PROTO_SCHEMAS["list_machines_resp"])

        decoded = _proto_decode_message(encoded, _PROTO_SCHEMAS["list_machines_resp"])

        self.assertEqual(decoded, {"machines": [machine]})

    def test_transcript_replay_advances_cursor_before_live_wait(self):
        original_condition = backend.TRANSCRIPT_CHANGED
        original_time = backend.time
        original_log = backend.log
        original_agents = backend.AGENT_INDEX
        original_transcripts = backend.TRANSCRIPTS

        class FakeClock:
            def __init__(self):
                self.value = 0

            def monotonic(self):
                return self.value

            def time(self):
                return self.value

            def strftime(self, *_args):
                return "00:00:00"

        class ImmediateCondition:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def wait(self, timeout=None):
                del timeout
                clock.value = 15

        class StopStream(Exception):
            pass

        clock = FakeClock()
        frames = []
        agent_id = "agent-replay"
        entry = {
            "seq": 1,
            "updatedSeq": 1,
            "entryKind": "message",
            "entryId": "entry-replay",
            "body": backend.b64(
                json.dumps(
                    {
                        "kind": "message",
                        "role": "assistant",
                        "content": "replay",
                        "id": "entry-replay",
                    }
                )
            ),
        }
        backend.AGENT_INDEX = {agent_id: {}}
        backend.TRANSCRIPTS = {
            agent_id: {"generation": 1, "next_seq": 2, "entries": [entry]}
        }
        backend.TRANSCRIPT_CHANGED = ImmediateCondition()
        backend.time = clock
        backend.log = lambda _message: None

        handler = object.__new__(Handler)
        handler._request_is_stream_proto = lambda: False
        handler._connect_stream_start = lambda: None

        def write_frame(frame):
            frames.append(frame)
            if "heartbeat" in frame:
                raise StopStream()

        handler._connect_stream_write = write_frame

        try:
            with self.assertRaises(StopStream):
                Handler._watch_transcripts(
                    handler,
                    b'{"cursors":[],"includeUnlistedAgents":true}',
                )
        finally:
            backend.TRANSCRIPT_CHANGED = original_condition
            backend.time = original_time
            backend.log = original_log
            backend.AGENT_INDEX = original_agents
            backend.TRANSCRIPTS = original_transcripts

        rows = [frame["rows"] for frame in frames if "rows" in frame]
        self.assertEqual(len(rows), 1)
        self.assertTrue(all(row["replay"] is True for row in rows))
        self.assertEqual([row["agentId"] for row in rows], [agent_id])

    def test_restored_agent_id_is_read_from_json_and_query_shapes(self):
        agent_id = "01d0a188-7ac0-4508-b0a4-1d95e155fae4"

        self.assertEqual(_first_agent_id({"id": agent_id}), agent_id)
        self.assertEqual(_first_agent_id({"agentId": [agent_id]}), agent_id)
        self.assertEqual(_first_agent_id({"agent_id": [agent_id]}), agent_id)
        self.assertEqual(_first_agent_id({}), "")


class LocalGatewayChatContractTests(unittest.TestCase):
    def setUp(self):
        self.original_persistence_file = backend.PERSISTENCE_FILE
        self.original_avatar_dir = backend.AVATAR_DIR
        self.original_host_settings = backend._host_settings_snapshot()
        self.persistence_dir = tempfile.TemporaryDirectory()
        backend.PERSISTENCE_FILE = os.path.join(self.persistence_dir.name, "backend_transcript_state.json")
        backend.AVATAR_DIR = os.path.join(self.persistence_dir.name, "agent_avatars")
        with TRANSCRIPT_CHANGED:
            AGENT_INDEX.clear()
            TRANSCRIPTS.clear()
            PROMPT_ACCEPTANCE.clear()
            SSE_SUBSCRIBERS.clear()
            backend.USER_COMPUTER_PRESENCE.clear()
        self.original_call_model = backend.call_model
        backend.call_model = lambda prompt: f"local echo: {prompt}"
        self.server = backend.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        with backend.AGENT_EXECUTORS_LOCK:
            executors = list(backend.AGENT_EXECUTORS.values())
            backend.AGENT_EXECUTORS.clear()
        for executor in executors:
            executor.shutdown(wait=True, cancel_futures=True)
        backend.call_model = self.original_call_model
        backend.AVATAR_DIR = self.original_avatar_dir
        with backend.HOST_SETTINGS_LOCK:
            backend.HOST_SETTINGS.clear()
            backend.HOST_SETTINGS.update(self.original_host_settings)
        with TRANSCRIPT_CHANGED:
            AGENT_INDEX.clear()
            TRANSCRIPTS.clear()
            PROMPT_ACCEPTANCE.clear()
            SSE_SUBSCRIBERS.clear()
            backend.USER_COMPUTER_PRESENCE.clear()
        backend.PERSISTENCE_FILE = self.original_persistence_file
        self.persistence_dir.cleanup()

    def test_transcript_sse_event_uses_decoded_gateway_entry_and_ordered_cursor(self):
        entry = {
            "seq": 7,
            "entryKind": "message",
            "body": backend.b64(
                json.dumps(
                    {
                        "kind": "message",
                        "role": "user",
                        "content": "hello",
                        "clientNonce": "nonce-event-test",
                    }
                )
            ),
            "updatedSeq": 7,
            "entryId": "raw-entry-id",
        }

        event = _transcript_event("agent-event-test", entry, 3)

        self.assertEqual(event["channel"], "transcript")
        payload = event["payload"]
        self.assertEqual(payload["type"], "appended")
        self.assertEqual(payload["agentId"], "agent-event-test")
        self.assertEqual(payload["entry"]["id"], "raw-entry-id")
        self.assertEqual(payload["entry"]["role"], "user")
        self.assertNotIn("body", payload["entry"])
        self.assertNotIn("entryId", payload["entry"])
        self.assertEqual(payload["ordered"]["replicaKey"], "transcript:agent-event-test")
        self.assertEqual(payload["ordered"]["sequence"], 7)
        self.assertTrue(payload["ordered"]["epoch"].endswith(":3"))

    def test_native_dev_login_mints_camel_case_local_session(self):
        session = self._get("/auth/cursor_dev_session_token?plan=ultra&email=bridge%40local")

        self.assertTrue(session["accessToken"])
        self.assertTrue(session["refreshToken"])
        self.assertEqual(session["email"], "bridge@local")
        self.assertEqual(session["plan"], "ultra")
        self.assertNotIn("access_token", session)
        self.assertNotIn("refresh_token", session)

        payload = json.loads(backend.b64url_dec(session["accessToken"].split(".")[1]).decode("utf-8"))
        self.assertEqual(payload["sub"], "grok|bridge-user")
        self.assertEqual(payload["email"], "bridge@local")

        refreshed = self._post(
            "/oauth/token",
            {"refresh_token": session["refreshToken"]},
        )
        self.assertTrue(refreshed["access_token"])
        refreshed_payload = json.loads(backend.b64url_dec(refreshed["access_token"].split(".")[1]).decode("utf-8"))
        self.assertEqual(refreshed_payload["sub"], "grok|bridge-user")
        self.assertEqual(refreshed_payload["email"], "bridge@local")

    def test_gateway_transcript_entry_strips_group_private_nonce_for_renderer(self):
        entry = {
            "seq": 2,
            "entryKind": "message",
            "body": backend.b64(
                json.dumps(
                    {
                        "kind": "message",
                        "role": "assistant",
                        "content": "member reply",
                        "clientNonce": "user-nonce",
                        "groupPromptNonce": "group-nonce",
                        "memberAgentId": "member-a",
                        "authorId": "member-a",
                        "fromAgent": {"id": "member-a", "name": "Member A"},
                    }
                )
            ),
            "updatedSeq": 2,
            "entryId": "group-reply",
        }

        decoded = _gateway_transcript_entry("group-a", entry)

        self.assertEqual(decoded["content"], "member reply")
        self.assertEqual(decoded["fromAgent"], {"id": "member-a", "name": "Member A"})
        self.assertNotIn("clientNonce", decoded)
        self.assertNotIn("groupPromptNonce", decoded)

    def test_native_wire_entry_rehydrates_legacy_group_member_identity(self):
        member = self._post(
            "/api/createAgent",
            {"name": "Legacy Member", "clientNonce": "legacy-wire-member"},
        )["agent"]
        group = self._post(
            "/api/createGroup",
            {"name": "Legacy Wire Channel", "memberAgentIds": [member["id"]]},
        )["agent"]
        raw = backend.append_entry(
            group["id"],
            "message",
            {
                "kind": "message",
                "role": "assistant",
                "content": "old row",
                "memberAgentId": member["id"],
                "authorId": member["id"],
            },
        )

        wire = backend._transcript_wire_entry(group["id"], raw)
        decoded = json.loads(base64.b64decode(wire["body"]).decode("utf-8"))
        self.assertEqual(decoded["fromAgent"], {"id": member["id"], "name": member["name"]})
        self.assertNotIn("groupPromptNonce", decoded)

    def test_append_broadcasts_transcript_event_to_registered_sse_client(self):
        received = []

        class FakeSseClient:
            def _sse_write_unlocked(self, data):
                received.append(data)

        client = FakeSseClient()
        with backend.LOCK:
            SSE_SUBSCRIBERS[client] = threading.Lock()

        entry = backend.append_entry(
            "agent-event-test",
            "message",
            {"kind": "message", "role": "assistant", "content": "reply"},
        )
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0]["channel"], "transcript")
        self.assertEqual(received[0]["payload"]["entry"]["id"], entry["entryId"])
        self.assertEqual(received[0]["payload"]["ordered"]["sequence"], entry["seq"])

    def _get(self, path):
        with urllib.request.urlopen(self.base_url + path, timeout=5) as response:
            return json.loads(response.read().decode("utf-8"))

    def _post(self, path, payload):
        request = urllib.request.Request(
            self.base_url + path,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            return json.loads(response.read().decode("utf-8"))

    def _post_proto(self, path, payload, request_schema, response_schema):
        request = urllib.request.Request(
            self.base_url + path,
            data=_proto_encode_message(payload, request_schema),
            headers={"Content-Type": "application/proto"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            self.assertEqual(response.headers.get_content_type(), "application/proto")
            return _proto_decode_message(response.read(), response_schema)

    def assert_gateway_agent_summary_036(self, agent):
        self.assertIsInstance(agent, dict)
        keys = set(agent)
        self.assertFalse(
            GATEWAY_AGENT_REQUIRED_FIELDS_036 - keys,
            f"missing 0.36 fields: {sorted(GATEWAY_AGENT_REQUIRED_FIELDS_036 - keys)}",
        )
        self.assertFalse(
            keys - GATEWAY_AGENT_ALLOWED_FIELDS_036,
            f"unexpected 0.36 fields: {sorted(keys - GATEWAY_AGENT_ALLOWED_FIELDS_036)}",
        )
        self.assertIsInstance(agent["avatarDataUrl"], (str, type(None)))
        self.assertIsInstance(agent["isActive"], bool)
        self.assertIsInstance(agent["isComposingMessage"], bool)
        self.assertIsInstance(agent["isRunning"], bool)
        self.assertIn(agent["origin"], {"dev", "user"})
        if "harness" in agent:
            self.assertEqual(agent["harness"], "temporal")

    def test_create_agent_returns_valid_box_harness_and_is_listed(self):
        agent_id = "agent-create-contract-test"
        response = self._post_proto(
            "/aiserver.v1.GrokBotService/CreateGrokBotAgent",
            {
                "agentId": agent_id,
                "legacyAgentId": agent_id,
                "name": "Created Bot",
                "description": "created for the local contract test",
                "title": "Created Bot",
                "harness": 1,
            },
            _PROTO_SCHEMAS["create_agent_req"],
            _PROTO_SCHEMAS["create_agent_resp"],
        )
        self.assertEqual(response["harness"], 1)
        self.assertEqual(response["agent"]["id"], agent_id)
        self.assertEqual(response["agent"]["agentId"], agent_id)
        self.assertEqual(response["agent"]["name"], "Created Bot")
        self.assertEqual(response["agent"]["harness"], "box")

        listed = self._post_proto(
            "/aiserver.v1.GrokBotService/ListGrokBotAgents",
            {},
            {},
            _PROTO_SCHEMAS["list_agents_resp"],
        )
        self.assertEqual([agent["id"] for agent in listed["agents"]], [agent_id])

    def test_temporal_create_alias_returns_local_box_agent_response(self):
        response = self._post_proto(
            "/aiserver.v1.GrokBotService/CreateGrokBotTemporalAgent",
            {"name": "Temporal Alias Bot"},
            _PROTO_SCHEMAS["create_agent_req"],
            _PROTO_SCHEMAS["create_agent_resp"],
        )
        self.assertEqual(response["harness"], 1)
        self.assertEqual(response["agent"]["name"], "Temporal Alias Bot")
        self.assertEqual(response["agent"]["harness"], "box")

    def test_runtime_capabilities_advertise_supported_creation_path(self):
        response = self._post_proto(
            "/aiserver.v1.GrokBotService/GetGrokBotRuntimeCapabilities",
            {},
            {},
            _PROTO_SCHEMAS["runtime_caps_resp"],
        )
        capabilities = response["capabilities"]
        self.assertTrue(capabilities["durableIdentityEnabled"])
        self.assertTrue(capabilities["durableIdentityWritesEnabled"])
        self.assertFalse(capabilities.get("temporalCreationEnabled", False))
        self.assertTrue(capabilities["agentMessagingEnabled"])

    def test_gateway_host_settings_match_036_contract_and_accept_updates(self):
        settings = self._post("/api/getHostSettings", {})

        self.assertEqual(
            set(settings),
            {
                "autoReviewInstructions",
                "hasSeenOnboarding",
                "localToolPermission",
                "mcpBoxServers",
                "mcpCustomInstructions",
                "mcpCustomInstructionsByServerId",
                "mcpDisabledToolsByServerId",
                "mcpHeldAccountScopes",
                "notifications",
                "pinnedAgentIds",
                "sidebarSections",
                "webauthnProxyEnabled",
            },
        )
        self.assertEqual(
            settings["autoReviewInstructions"],
            {"allowInstructions": [], "blockInstructions": [], "isEnabled": False},
        )
        self.assertEqual(
            set(settings["notifications"]),
            {"allowedApps", "isEnabled", "maxPerWindow", "minIntervalMs", "windowMs"},
        )

        updated = self._post(
            "/api/setHostSettings",
            {
                "autoReviewInstructions": {
                    "allowInstructions": ["safe local command"],
                    "blockInstructions": ["external provider"],
                    "isEnabled": True,
                },
                "notifications": {"isEnabled": True},
                "pinnedAgentIds": ["agent-settings-test"],
            },
        )
        self.assertTrue(updated["autoReviewInstructions"]["isEnabled"])
        self.assertEqual(updated["notifications"]["allowedApps"], [])
        self.assertTrue(updated["notifications"]["isEnabled"])
        self.assertEqual(updated["pinnedAgentIds"], ["agent-settings-test"])

    def test_native_user_computer_unary_contracts_are_typed_and_empty(self):
        machine_id = "local-machine-contract"
        hello = {
            "label": "Local contract test",
            "localRoot": "C:/workspace",
            "standing": "supervised",
            "supervised": True,
        }
        with backend.LOCK:
            backend.USER_COMPUTER_PRESENCE[machine_id] = {
                "machineId": machine_id,
                "hello": hello,
                "lastSeenAtMs": 123,
            }

        listed = self._post_proto(
            "/aiserver.v1.GrokBotService/ListGrokBotUserComputers",
            {},
            {},
            _PROTO_SCHEMAS["computer_list_resp"],
        )
        self.assertEqual(listed["computers"][0]["machineId"], machine_id)
        self.assertEqual(listed["computers"][0]["hello"]["label"], hello["label"])

        poll = self._post_proto(
            "/aiserver.v1.GrokBotService/PollGrokBotUserComputerRequests",
            {
                "machineId": machine_id,
                "credential": backend.LOCAL_EXEC_CREDENTIAL,
                "limit": 20,
            },
            _PROTO_SCHEMAS["computer_poll_req"],
            _PROTO_SCHEMAS["computer_poll_resp"],
        )
        self.assertEqual(poll.get("requests", []), [])

        submit = self._post_proto(
            "/aiserver.v1.GrokBotService/SubmitGrokBotUserComputerResponses",
            {
                "machineId": machine_id,
                "credential": backend.LOCAL_EXEC_CREDENTIAL,
                "frames": [],
            },
            _PROTO_SCHEMAS["computer_submit_req"],
            _PROTO_SCHEMAS["computer_submit_resp"],
        )
        self.assertEqual(submit.get("acceptedCount", 0), 0)

        cancelled = self._post_proto(
            "/aiserver.v1.GrokBotService/CancelGrokBotUserComputerRequest",
            {"machineId": machine_id, "requestId": "missing"},
            _PROTO_SCHEMAS["computer_cancel_req"],
            _PROTO_SCHEMAS["computer_cancel_resp"],
        )
        self.assertEqual(cancelled, {})

    def test_stale_computer_watch_cannot_remove_successor_presence(self):
        machine_id = "same-machine-watch"
        old_presence = backend._register_user_computer_presence(
            machine_id,
            {"label": "Old watch"},
        )
        new_presence = backend._register_user_computer_presence(
            machine_id,
            {"label": "New watch"},
        )

        self.assertIs(backend.USER_COMPUTER_PRESENCE[machine_id], new_presence)
        self.assertFalse(backend._remove_user_computer_presence(machine_id, old_presence))
        self.assertIs(backend.USER_COMPUTER_PRESENCE[machine_id], new_presence)
        self.assertEqual(
            backend.USER_COMPUTER_PRESENCE[machine_id]["hello"]["label"],
            "New watch",
        )
        self.assertTrue(backend._remove_user_computer_presence(machine_id, new_presence))
        self.assertNotIn(machine_id, backend.USER_COMPUTER_PRESENCE)

    def test_sand_access_status_uses_native_entitlement_fields(self):
        response = self._post_proto(
            "/aiserver.v1.DashboardService/GetSandAccessStatus",
            {},
            {},
            _PROTO_SCHEMAS["access_resp"],
        )

        self.assertEqual(response["state"], 1)
        self.assertEqual(response["purchaseChannel"], 3)
        self.assertEqual(response["blockReason"], 1)
        self.assertTrue(response["canSkipOnboarding"])
        self.assertTrue(response["proAndSuperGrokPlansGrantAccess"])

    def test_gateway_create_agent_returns_gui_agent_record(self):
        response = self._post(
            "/api/createAgent",
            {
                "name": "GUI Created Bot",
                "description": "created through the 0.30 local gateway",
                "origin": "user",
                "isKickstartRequested": True,
                "clientNonce": "gui-create-contract-test",
            },
        )

        self.assertIsInstance(response.get("agent"), dict)
        self.assertTrue(response["agent"]["id"])
        self.assertEqual(response["agent"]["name"], "GUI Created Bot")
        self.assertEqual(response["agent"]["description"], "created through the 0.30 local gateway")
        self.assertEqual(response["agent"]["origin"], "user")
        self.assert_gateway_agent_summary_036(response["agent"])

        replay = self._post(
            "/api/createAgent",
            {
                "clientNonce": "gui-create-contract-test",
            },
        )
        self.assertEqual(replay["agent"]["id"], response["agent"]["id"])
        self.assertEqual(replay["agent"], response["agent"])

        listed = self._post("/api/listAgents", {})
        self.assertEqual([agent["id"] for agent in listed], [response["agent"]["id"]])
        self.assert_gateway_agent_summary_036(listed[0])

    def test_gateway_create_agent_defaults_to_user_origin(self):
        response = self._post(
            "/api/createAgent",
            {
                "name": "Default Origin Bot",
                "description": "origin default contract",
                "clientNonce": "default-origin-contract-test",
            },
        )

        self.assertEqual(response["agent"]["origin"], "user")
        self.assert_gateway_agent_summary_036(response["agent"])

    def test_gateway_channel_view_endpoints_return_valid_empty_channels_view(self):
        agent = self._post(
            "/api/createAgent",
            {"name": "Channel View Owner", "clientNonce": "channel-view-owner"},
        )["agent"]

        expected = {"manifests": [], "connections": []}
        requests = [
            ("/api/getAgentChannels", {"id": agent["id"]}),
            ("/api/connectChannel", {"id": agent["id"], "platform": "discord", "token": "ignored"}),
            ("/api/disconnectChannel", {"id": agent["id"], "platform": "discord"}),
            ("/api/refreshChannel", {"id": agent["id"], "platform": "discord"}),
        ]

        for path, request in requests:
            with self.subTest(path=path):
                self.assertEqual(self._post(path, request), expected)

    def test_gateway_create_group_returns_durable_group_agent(self):
        first = self._post(
            "/api/createAgent",
            {
                "name": "Planner",
                "clientNonce": "group-member-planner",
            },
        )["agent"]
        second = self._post(
            "/api/createAgent",
            {
                "name": "Researcher",
                "clientNonce": "group-member-researcher",
            },
        )["agent"]

        response = self._post(
            "/api/createGroup",
            {
                "name": "Research Channel",
                "memberAgentIds": [first["id"], second["id"]],
            },
        )

        group = response["agent"]
        self.assertTrue(group["id"])
        self.assertEqual(group["name"], "Research Channel")
        self.assertTrue(group["isGroup"])
        self.assertEqual(group["memberIds"], [first["id"], second["id"]])
        self.assertEqual(group["origin"], "user")
        self.assert_gateway_agent_summary_036(group)

        replay = self._post(
            "/api/createGroup",
            {
                "name": "Research Channel",
                "memberAgentIds": [first["id"], second["id"]],
            },
        )
        self.assertEqual(replay["agent"]["id"], group["id"])

        listed = self._post("/api/listAgents", {})
        listed_group = next(agent for agent in listed if agent["id"] == group["id"])
        self.assertTrue(listed_group["isGroup"])
        self.assertEqual(listed_group["memberIds"], [first["id"], second["id"]])

        with TRANSCRIPT_CHANGED:
            AGENT_INDEX.clear()
            TRANSCRIPTS.clear()
            PROMPT_ACCEPTANCE.clear()
        backend._load_persisted_state()

        reloaded = self._post("/api/listAgents", {})
        reloaded_group = next(agent for agent in reloaded if agent["id"] == group["id"])
        self.assertTrue(reloaded_group["isGroup"])
        self.assertEqual(reloaded_group["memberIds"], [first["id"], second["id"]])

    def test_gateway_create_group_rejects_invalid_members_without_side_effects(self):
        existing = self._post(
            "/api/createAgent",
            {
                "name": "Existing Bot",
                "clientNonce": "group-existing-member",
            },
        )["agent"]

        with self.assertRaises(urllib.error.HTTPError) as raised:
            self._post(
                "/api/createGroup",
                {
                    "name": "Broken Channel",
                    "memberAgentIds": [existing["id"], "missing-agent"],
                },
            )

        self.assertEqual(raised.exception.code, 400)
        error = json.loads(raised.exception.read().decode("utf-8"))
        self.assertEqual(error["code"], "UNKNOWN_GROUP_MEMBER")
        self.assertEqual(error["memberAgentId"], "missing-agent")
        self.assertFalse(any(meta.get("isGroup") for meta in AGENT_INDEX.values()))

    def test_gateway_set_group_members_replaces_roster_and_survives_reload(self):
        first = self._post(
            "/api/createAgent",
            {"name": "First", "clientNonce": "set-members-first"},
        )["agent"]
        second = self._post(
            "/api/createAgent",
            {"name": "Second", "clientNonce": "set-members-second"},
        )["agent"]
        group = self._post(
            "/api/createGroup",
            {"name": "Editable Channel", "memberAgentIds": [first["id"], second["id"]]},
        )["agent"]

        updated = self._post(
            "/api/setGroupMembers",
            {"id": group["id"], "memberAgentIds": [second["id"]]},
        )
        self.assertEqual(updated["agent"]["id"], group["id"])
        self.assertEqual(updated["agent"]["memberIds"], [second["id"]])

        with TRANSCRIPT_CHANGED:
            AGENT_INDEX.clear()
            TRANSCRIPTS.clear()
            PROMPT_ACCEPTANCE.clear()
        backend._load_persisted_state()

        listed = self._post("/api/listAgents", {})
        reloaded = next(agent for agent in listed if agent["id"] == group["id"])
        self.assertEqual(reloaded["memberIds"], [second["id"]])

    def test_gateway_set_group_members_rejects_unknown_nested_and_non_group_targets(self):
        member = self._post(
            "/api/createAgent",
            {"name": "Member", "clientNonce": "set-members-validation-member"},
        )["agent"]
        group = self._post(
            "/api/createGroup",
            {"name": "Validation Channel", "memberAgentIds": [member["id"]]},
        )["agent"]

        with self.assertRaises(urllib.error.HTTPError) as raised:
            self._post(
                "/api/setGroupMembers",
                {"id": group["id"], "memberAgentIds": ["missing-member"]},
            )
        self.assertEqual(raised.exception.code, 400)
        error = json.loads(raised.exception.read().decode("utf-8"))
        self.assertEqual(error["code"], "UNKNOWN_GROUP_MEMBER")

        with self.assertRaises(urllib.error.HTTPError) as raised:
            self._post(
                "/api/setGroupMembers",
                {"id": group["id"], "memberAgentIds": [group["id"]]},
            )
        self.assertEqual(raised.exception.code, 400)
        error = json.loads(raised.exception.read().decode("utf-8"))
        self.assertEqual(error["code"], "GROUP_MEMBER_NOT_ALLOWED")

        with self.assertRaises(urllib.error.HTTPError) as raised:
            self._post(
                "/api/setGroupMembers",
                {"id": member["id"], "memberAgentIds": [member["id"]]},
            )
        self.assertEqual(raised.exception.code, 400)
        error = json.loads(raised.exception.read().decode("utf-8"))
        self.assertEqual(error["code"], "NOT_A_GROUP")

    def test_gateway_set_group_members_does_not_materialize_unknown_target(self):
        member = self._post(
            "/api/createAgent",
            {"name": "Known Member", "clientNonce": "set-members-unknown-target-member"},
        )["agent"]
        unknown_id = "missing-group-target"

        with self.assertRaises(urllib.error.HTTPError) as raised:
            self._post(
                "/api/setGroupMembers",
                {"id": unknown_id, "memberAgentIds": [member["id"]]},
            )

        self.assertEqual(raised.exception.code, 400)
        error = json.loads(raised.exception.read().decode("utf-8"))
        self.assertEqual(error["code"], "UNKNOWN_AGENT")
        self.assertNotIn(unknown_id, AGENT_INDEX)
        self.assertNotIn(unknown_id, TRANSCRIPTS)

    def test_group_send_prompt_fans_out_once_per_member_and_records_acceptance(self):
        first = self._post(
            "/api/createAgent",
            {"name": "Fanout First", "clientNonce": "fanout-first"},
        )["agent"]
        second = self._post(
            "/api/createAgent",
            {"name": "Fanout Second", "clientNonce": "fanout-second"},
        )["agent"]
        group = self._post(
            "/api/createGroup",
            {"name": "Fanout Channel", "memberAgentIds": [first["id"], second["id"]]},
        )["agent"]
        calls = []
        original_call_model = backend.call_model

        def fake_call_model(prompt):
            calls.append(prompt)
            return f"reply-{len(calls)}"

        backend.call_model = fake_call_model
        try:
            nonce = "fanout-prompt-once"
            self.assertEqual(
                self._post(
                    "/api/sendPrompt",
                    {"agentId": group["id"], "prompt": "fan out", "clientNonce": nonce},
                ),
                {"accepted": True},
            )
            self.assertEqual(
                self._post(
                    "/api/sendPrompt",
                    {"agentId": group["id"], "prompt": "fan out", "clientNonce": nonce},
                ),
                {"accepted": True},
            )

            deadline = time.time() + 2
            tail = {"entries": []}
            while time.time() < deadline:
                tail = self._post(
                    "/api/getAgentTranscriptTail",
                    {"id": group["id"], "limit": 10},
                )
                if len(tail["entries"]) >= 3:
                    break
                time.sleep(0.05)

            self.assertEqual(len(calls), 2)
            self.assertEqual(len(tail["entries"]), 3)
            self.assertEqual(tail["entries"][0]["role"], "user")
            replies = tail["entries"][1:]
            self.assertEqual([entry["content"] for entry in replies], ["reply-1", "reply-2"])
            self.assertEqual({entry["memberAgentId"] for entry in replies}, {first["id"], second["id"]})
            self.assertEqual(
                {entry["fromAgent"]["id"] for entry in replies},
                {first["id"], second["id"]},
            )
            self.assertEqual(
                {entry["fromAgent"]["name"] for entry in replies},
                {first["name"], second["name"]},
            )
            self.assertTrue(all("clientNonce" not in entry for entry in replies))
            self.assertTrue(all("groupPromptNonce" not in entry for entry in replies))
            self.assertEqual({entry["replyTo"] for entry in replies}, {tail["entries"][0]["id"]})

            acceptance = self._post(
                "/api/promptAcceptanceStatus",
                {"agentId": group["id"], "clientNonce": nonce},
            )
            self.assertEqual(acceptance["record"]["status"], "accepted")
            self.assertEqual(
                set(acceptance["record"]["groupMemberResults"]),
                {first["id"], second["id"]},
            )
        finally:
            backend.call_model = original_call_model

    def test_group_send_prompt_keeps_other_members_when_one_call_fails(self):
        first = self._post(
            "/api/createAgent",
            {"name": "Failure First", "clientNonce": "fanout-failure-first"},
        )["agent"]
        second = self._post(
            "/api/createAgent",
            {"name": "Failure Second", "clientNonce": "fanout-failure-second"},
        )["agent"]
        group = self._post(
            "/api/createGroup",
            {"name": "Partial Failure Channel", "memberAgentIds": [first["id"], second["id"]]},
        )["agent"]
        calls = []
        original_call_model = backend.call_model

        def fake_call_model(prompt):
            calls.append(prompt)
            if len(calls) == 1:
                raise RuntimeError("synthetic member failure")
            return "surviving reply"

        backend.call_model = fake_call_model
        try:
            nonce = "fanout-partial-failure"
            self.assertEqual(
                self._post(
                    "/api/sendPrompt",
                    {"agentId": group["id"], "prompt": "partial", "clientNonce": nonce},
                ),
                {"accepted": True},
            )

            deadline = time.time() + 2
            tail = {"entries": []}
            while time.time() < deadline:
                tail = self._post(
                    "/api/getAgentTranscriptTail",
                    {"id": group["id"], "limit": 10},
                )
                if len(tail["entries"]) >= 3:
                    break
                time.sleep(0.05)

            self.assertEqual(len(calls), 2)
            self.assertEqual(len(tail["entries"]), 3)
            self.assertTrue(tail["entries"][1].get("isError"))
            self.assertEqual(tail["entries"][2]["content"], "surviving reply")
            self.assertEqual(tail["entries"][1]["fromAgent"]["id"], first["id"])
            self.assertEqual(tail["entries"][2]["fromAgent"]["id"], second["id"])
            self.assertTrue(all("clientNonce" not in entry for entry in tail["entries"][1:]))
            self.assertTrue(all("groupPromptNonce" not in entry for entry in tail["entries"][1:]))
            self.assertEqual({entry["replyTo"] for entry in tail["entries"][1:]}, {tail["entries"][0]["id"]})
            acceptance = {"record": {}}
            deadline = time.time() + 2
            while time.time() < deadline:
                acceptance = self._post(
                    "/api/promptAcceptanceStatus",
                    {"agentId": group["id"], "clientNonce": nonce},
                )
                if acceptance.get("record", {}).get("completedAtMs"):
                    break
                time.sleep(0.05)
            self.assertEqual(acceptance["record"]["status"], "failed")
            self.assertEqual(acceptance["record"]["failedMemberIds"], [first["id"]])
            self.assertEqual(
                set(acceptance["record"]["groupMemberResults"]),
                {first["id"], second["id"]},
            )
        finally:
            backend.call_model = original_call_model

    def test_group_resume_uses_private_nonce_and_skips_completed_member(self):
        first = self._post(
            "/api/createAgent",
            {"name": "Resume First", "clientNonce": "group-resume-first"},
        )["agent"]
        second = self._post(
            "/api/createAgent",
            {"name": "Resume Second", "clientNonce": "group-resume-second"},
        )["agent"]
        group = self._post(
            "/api/createGroup",
            {"name": "Resume Channel", "memberAgentIds": [first["id"], second["id"]]},
        )["agent"]
        nonce = "group-resume-prompt"
        backend.claim_user_prompt(group["id"], "resume group", nonce)
        backend._commit_prompt_result(
            group["id"],
            "",
            {
                "kind": "message",
                "role": "assistant",
                "content": "already completed",
                "isStreaming": False,
                "groupPromptNonce": nonce,
                "memberAgentId": first["id"],
                "authorId": first["id"],
                "fromAgent": {"id": first["id"], "name": first["name"]},
                "timestampMs": int(time.time() * 1000),
            },
        )

        with TRANSCRIPT_CHANGED:
            AGENT_INDEX.clear()
            TRANSCRIPTS.clear()
            PROMPT_ACCEPTANCE.clear()
        backend._load_persisted_state()

        calls = []
        original_call_model = backend.call_model
        backend.call_model = lambda prompt: calls.append(prompt) or "resumed second"
        try:
            self.assertEqual(backend.resume_pending_prompts(), 1)

            deadline = time.time() + 2
            entries = []
            while time.time() < deadline:
                with TRANSCRIPT_CHANGED:
                    entries = list(TRANSCRIPTS.get(group["id"], {}).get("entries", []))
                if len(entries) >= 3:
                    break
                time.sleep(0.05)

            self.assertEqual(len(calls), 1)
            raw_replies = [backend._entry_body_obj(entry) for entry in entries[1:]]
            self.assertEqual(
                {reply["memberAgentId"] for reply in raw_replies},
                {first["id"], second["id"]},
            )
            self.assertEqual(raw_replies[0]["content"], "already completed")
            self.assertEqual(raw_replies[1]["content"], "resumed second")
            self.assertTrue(all(reply.get("groupPromptNonce") == nonce for reply in raw_replies))
            self.assertTrue(all("clientNonce" not in reply for reply in raw_replies))

            tail = self._post(
                "/api/getAgentTranscriptTail",
                {"id": group["id"], "limit": 10},
            )
            self.assertEqual(
                {reply["fromAgent"]["id"] for reply in tail["entries"][1:]},
                {first["id"], second["id"]},
            )
            self.assertTrue(all("groupPromptNonce" not in reply for reply in tail["entries"][1:]))
            self.assertTrue(all("clientNonce" not in reply for reply in tail["entries"][1:]))
        finally:
            backend.call_model = original_call_model

    def test_group_reply_batches_expose_distinct_reply_to_ids(self):
        first = self._post(
            "/api/createAgent",
            {"name": "Batch First", "clientNonce": "batch-first"},
        )["agent"]
        second = self._post(
            "/api/createAgent",
            {"name": "Batch Second", "clientNonce": "batch-second"},
        )["agent"]
        group = self._post(
            "/api/createGroup",
            {"name": "Batch Channel", "memberAgentIds": [first["id"], second["id"]]},
        )["agent"]
        original_call_model = backend.call_model
        backend.call_model = lambda prompt: f"reply-{prompt.splitlines()[-1]}"
        try:
            self._post(
                "/api/sendPrompt",
                {"agentId": group["id"], "prompt": "first prompt", "clientNonce": "batch-prompt-1"},
            )
            self._post(
                "/api/sendPrompt",
                {"agentId": group["id"], "prompt": "second prompt", "clientNonce": "batch-prompt-2"},
            )
            deadline = time.time() + 3
            tail = {"entries": []}
            while time.time() < deadline:
                tail = self._post("/api/getAgentTranscriptTail", {"id": group["id"], "limit": 10})
                if len(tail["entries"]) >= 6:
                    break
                time.sleep(0.05)
            self.assertEqual(len(tail["entries"]), 6)
            users = [entry for entry in tail["entries"] if entry["role"] == "user"]
            replies = [entry for entry in tail["entries"] if entry["role"] == "assistant"]
            self.assertEqual([entry["content"] for entry in users], ["first prompt", "second prompt"])
            self.assertEqual(len(replies), 4)
            first_user, second_user = users
            first_replies = [entry for entry in replies if entry["replyTo"] == first_user["id"]]
            second_replies = [entry for entry in replies if entry["replyTo"] == second_user["id"]]
            self.assertEqual(len(first_replies), 2)
            self.assertEqual(len(second_replies), 2)
            self.assertNotEqual(first_user["id"], second_user["id"])
        finally:
            backend.call_model = original_call_model

    def test_legacy_channel_name_resolves_to_unique_group_uuid(self):
        member = self._post(
            "/api/createAgent",
            {"name": "Alias Member", "clientNonce": "alias-member"},
        )["agent"]
        group = self._post(
            "/api/createGroup",
            {"name": "555", "memberAgentIds": [member["id"]]},
        )["agent"]

        self.assertEqual(_resolve_agent_id("555"), group["id"])
        self.assertEqual(_first_agent_id({"id": "555"}), group["id"])

        backend.ensure_agent("555")
        listed = self._post("/api/listAgents", {})
        self.assertTrue(any(agent["id"] == group["id"] and agent["isGroup"] for agent in listed))
        self.assertFalse(any(agent["id"] == "555" for agent in listed))

        backend.append_entry(
            group["id"],
            "message",
            {"kind": "message", "role": "assistant", "content": "alias-visible"},
        )
        tail = self._post("/api/getAgentTranscriptTail", {"id": "555", "limit": 8})
        self.assertEqual([entry["content"] for entry in tail["entries"]], ["alias-visible"])

    def test_native_roster_preserves_group_identity_fields(self):
        member = self._post(
            "/api/createAgent",
            {"name": "Native Member", "clientNonce": "native-group-member"},
        )["agent"]
        group = self._post(
            "/api/createGroup",
            {"name": "Native Channel", "memberAgentIds": [member["id"]]},
        )["agent"]

        listed = self._post_proto(
            "/aiserver.v1.GrokBotService/ListGrokBotAgents",
            {},
            {},
            _PROTO_SCHEMAS["list_agents_resp"],
        )
        native_group = next(agent for agent in listed["agents"] if agent["id"] == group["id"])
        self.assertEqual(native_group["kind"], 2)
        self.assertEqual(native_group["memberAgentIds"], [member["id"]])

    def test_native_room_create_and_member_update_round_trip(self):
        first = self._post(
            "/api/createAgent",
            {"name": "Native Room Member A", "clientNonce": "native-room-member-a"},
        )["agent"]
        second = self._post(
            "/api/createAgent",
            {"name": "Native Room Member B", "clientNonce": "native-room-member-b"},
        )["agent"]
        room_id = "native-room-contract"

        created = self._post_proto(
            "/aiserver.v1.GrokBotService/CreateGrokBotRoom",
            {
                "agentId": room_id,
                "name": "Native Room",
                "description": "created through the native room contract",
                "memberAgentIds": [first["id"], second["id"]],
            },
            _PROTO_SCHEMAS["room_create_req"],
            _PROTO_SCHEMAS["room_resp"],
        )
        self.assertEqual(created["agent"]["id"], room_id)
        self.assertEqual(created["agent"]["kind"], 2)
        self.assertEqual(created["agent"]["memberAgentIds"], [first["id"], second["id"]])

        updated = self._post_proto(
            "/aiserver.v1.GrokBotService/SetGrokBotRoomMembers",
            {"agentId": room_id, "memberAgentIds": [second["id"]]},
            _PROTO_SCHEMAS["room_set_members_req"],
            _PROTO_SCHEMAS["room_resp"],
        )
        self.assertEqual(updated["agent"]["id"], room_id)
        self.assertEqual(updated["agent"]["kind"], 2)
        self.assertEqual(updated["agent"]["memberAgentIds"], [second["id"]])

    def test_gateway_update_agent_applies_profile_and_persists(self):
        created = self._post(
            "/api/createAgent",
            {
                "name": "Original Name",
                "description": "original description",
                "clientNonce": "update-profile-contract-test",
            },
        )["agent"]

        updated = self._post(
            "/api/updateAgent",
            {
                "id": created["id"],
                "profile": {
                    "name": "Renamed Bot",
                    "description": "updated description",
                    "title": "Renamed title",
                    "avatarShape": "hexagon",
                    "avatarColor": "violet",
                },
            },
        )
        self.assertEqual(updated["agent"]["id"], created["id"])
        self.assertEqual(updated["agent"]["name"], "Renamed Bot")
        self.assertEqual(updated["agent"]["description"], "updated description")
        self.assertEqual(updated["agent"]["title"], "Renamed title")
        self.assertEqual(updated["agent"]["avatarShape"], "hexagon")
        self.assertEqual(updated["agent"]["avatarColor"], "violet")

        with TRANSCRIPT_CHANGED:
            AGENT_INDEX.clear()
            TRANSCRIPTS.clear()
            PROMPT_ACCEPTANCE.clear()
        backend._load_persisted_state()
        listed = self._post("/api/listAgents", {})
        reloaded = next(agent for agent in listed if agent["id"] == created["id"])
        self.assertEqual(reloaded["name"], "Renamed Bot")
        self.assertEqual(reloaded["description"], "updated description")

        with self.assertRaises(urllib.error.HTTPError) as raised:
            self._post(
                "/api/updateAgent",
                {"id": created["id"], "profile": {"name": "   "}},
            )
        self.assertEqual(raised.exception.code, 400)
        error = json.loads(raised.exception.read().decode("utf-8"))
        self.assertEqual(error["code"], "INVALID_AGENT_NAME")

    def test_gateway_update_agent_does_not_materialize_unknown_target(self):
        unknown_id = "missing-update-target"

        with self.assertRaises(urllib.error.HTTPError) as raised:
            self._post(
                "/api/updateAgent",
                {"id": unknown_id, "profile": {"name": "Should Not Exist"}},
            )

        self.assertEqual(raised.exception.code, 400)
        error = json.loads(raised.exception.read().decode("utf-8"))
        self.assertEqual(error["code"], "UNKNOWN_AGENT")
        self.assertNotIn(unknown_id, AGENT_INDEX)
        self.assertNotIn(unknown_id, TRANSCRIPTS)

    def test_gateway_delete_agents_removes_batch_and_keeps_group_members_safe(self):
        first = self._post(
            "/api/createAgent",
            {"name": "Delete First", "clientNonce": "delete-first"},
        )["agent"]
        second = self._post(
            "/api/createAgent",
            {"name": "Delete Second", "clientNonce": "delete-second"},
        )["agent"]
        group = self._post(
            "/api/createGroup",
            {"name": "Delete Guard Channel", "memberAgentIds": [first["id"], second["id"]]},
        )["agent"]

        with self.assertRaises(urllib.error.HTTPError) as raised:
            self._post("/api/deleteAgents", {"ids": [first["id"]]})
        self.assertEqual(raised.exception.code, 400)
        error = json.loads(raised.exception.read().decode("utf-8"))
        self.assertEqual(error["code"], "AGENT_IN_USE")
        self.assertIn(first["id"], AGENT_INDEX)

        deleted = self._post("/api/deleteAgents", {"ids": [group["id"], first["id"]]})
        self.assertEqual(deleted["deletedIds"], [group["id"], first["id"]])
        self.assertNotIn(group["id"], AGENT_INDEX)
        self.assertNotIn(first["id"], AGENT_INDEX)
        self.assertIn(second["id"], AGENT_INDEX)

        empty = self._post("/api/deleteAgents", {"ids": []})
        self.assertEqual(empty, {"deletedIds": [], "agents": []})

    def test_gateway_create_agent_rejects_unimplemented_template_import(self):
        with self.assertRaises(urllib.error.HTTPError) as raised:
            self._post(
                "/api/createAgent",
                {
                    "name": "Template Bot",
                    "description": "should not claim a remote template was applied",
                    "templateId": "remote-template-1",
                    "clientNonce": "template-contract-test",
                },
            )

        self.assertEqual(raised.exception.code, 501)
        error = json.loads(raised.exception.read().decode("utf-8"))
        self.assertEqual(error["code"], "LOCAL_TEMPLATE_UNSUPPORTED")
        self.assertIn("cannot apply remote bot templates", error["message"])
        self.assertEqual(AGENT_INDEX, {})

    def test_gateway_avatar_round_trip_uses_versioned_local_storage(self):
        created = self._post(
            "/api/createAgent",
            {
                "name": "Avatar Bot",
                "description": "avatar contract",
                "avatarShape": "hexagon",
                "avatarColor": "violet",
                "clientNonce": "avatar-create-contract-test",
            },
        )
        agent_id = created["agent"]["id"]
        png_bytes = b"\x89PNG\r\n\x1a\nlocal-avatar-test"
        encoded = base64.b64encode(png_bytes).decode("ascii")

        updated = self._post(
            "/api/setAgentAvatarBytes",
            {"id": agent_id, "pngBase64": encoded},
        )
        self.assertEqual(updated["agent"]["id"], agent_id)
        self.assertTrue(updated["version"])
        self.assertEqual(updated["agent"]["avatarVersion"], updated["version"])
        self.assertNotIn(encoded, json.dumps(updated))

        avatar = self._post("/api/getAgentAvatar", {"id": agent_id})
        self.assertEqual(avatar["version"], updated["version"])
        self.assertEqual(avatar["dataUrl"], f"data:image/png;base64,{encoded}")

        listed = self._post("/api/listAgents", {})
        listed_agent = next(agent for agent in listed if agent["id"] == agent_id)
        self.assertEqual(listed_agent["avatarVersion"], updated["version"])
        self.assertIn("avatarDataUrl", listed_agent)
        self.assertIsNone(listed_agent["avatarDataUrl"])

    def test_gateway_avatar_rejects_non_png_payload(self):
        with self.assertRaises(urllib.error.HTTPError) as raised:
            self._post(
                "/api/setAgentAvatarBytes",
                {"id": "avatar-invalid-agent", "pngBase64": base64.b64encode(b"not-png").decode("ascii")},
            )

        self.assertEqual(raised.exception.code, 400)
        error = json.loads(raised.exception.read().decode("utf-8"))
        self.assertEqual(error["code"], "INVALID_AVATAR")
        self.assertEqual(AGENT_INDEX, {})

    def test_call_model_supports_explicit_ollama_backend(self):
        captured = {}
        original_config = backend.bc.config
        original_post = backend.httpx.post

        class FakeResponse:
            def raise_for_status(self):
                return None

            def json(self):
                return {"message": {"content": "local default reply"}}

        def fake_post(url, **kwargs):
            captured["url"] = url
            captured["json"] = kwargs["json"]
            return FakeResponse()

        backend.bc.config = lambda: {
            "model_backend": "ollama",
            "ollama_url": "http://127.0.0.1:11434",
        }
        backend.httpx.post = fake_post
        backend.call_model = self.original_call_model
        try:
            self.assertEqual(backend.call_model("hello"), "local default reply")
        finally:
            backend.call_model = self.original_call_model
            backend.bc.config = original_config
            backend.httpx.post = original_post

        self.assertEqual(captured["url"], "http://127.0.0.1:11434/api/chat")
        self.assertEqual(captured["json"]["model"], "lfm2.5:8b-a1b")

    def test_native_local_send_acceptance_and_transcript_echo(self):
        self.assertEqual(self._get("/health"), {"ok": True})

        agent_id = "agent-contract-test"
        roster = self._post("/api/listAgents", {})
        self.assertTrue(roster)
        self.assert_gateway_agent_summary_036(roster[0])

        nonce = "nonce-contract-test"
        self.assertEqual(
            self._post(
                "/api/sendPrompt",
                {"agentId": agent_id, "prompt": "hello", "clientNonce": nonce},
            ),
            {"accepted": True},
        )
        self.assertEqual(
            self._post(
                "/api/sendPrompt",
                {"agentId": agent_id, "prompt": "hello", "clientNonce": nonce},
            ),
            {"accepted": True},
        )
        acceptance = self._post(
            "/api/promptAcceptanceStatus",
            {"accountSlot": "host", "agentId": agent_id, "clientNonce": nonce},
        )
        self.assertEqual(acceptance["outcome"], "found")
        self.assertEqual(acceptance["record"]["status"], "accepted")
        echo_entry_id = acceptance["record"]["echoEntryId"]
        self.assertTrue(echo_entry_id)

        deadline = time.time() + 2
        tail = {"entries": []}
        while time.time() < deadline:
            tail = self._post(
                "/api/getAgentTranscriptTail",
                {"id": agent_id, "limit": 10},
            )
            if len(tail["entries"]) >= 2:
                break
            time.sleep(0.05)
        self.assertGreaterEqual(len(tail["entries"]), 2)
        self.assertEqual(tail["entries"][0]["kind"], "message")
        self.assertTrue(tail["entries"][0]["id"])
        self.assertEqual(tail["entries"][0]["role"], "user")
        self.assertEqual(tail["entries"][0]["clientNonce"], nonce)
        self.assertEqual(tail["entries"][0]["id"], echo_entry_id)
        self.assertNotIn("body", tail["entries"][0])
        self.assertNotIn("entryId", tail["entries"][0])
        self.assertEqual(tail["entries"][1]["kind"], "message")
        self.assertTrue(tail["entries"][1]["id"])
        self.assertEqual(tail["entries"][1]["role"], "assistant")
        self.assertEqual(tail["entries"][1]["content"], "local echo: hello")

    def test_gateway_open_agent_tail_returns_transcript_page(self):
        agent_id = "agent-open-tail-contract-test"
        nonce = "nonce-open-tail-contract-test"
        self.assertEqual(
            self._post(
                "/api/sendPrompt",
                {"agentId": agent_id, "prompt": "hello", "clientNonce": nonce},
            ),
            {"accepted": True},
        )

        deadline = time.time() + 2
        tail = {"entries": []}
        while time.time() < deadline:
            tail = self._post("/api/getAgentTranscriptTail", {"id": agent_id, "limit": 10})
            if len(tail["entries"]) >= 2:
                break
            time.sleep(0.05)

        opened = self._post("/api/openAgentTail", {"id": agent_id, "limit": 10})
        self.assertEqual(opened, tail)
        self.assertEqual(opened["entries"][0]["role"], "user")
        self.assertEqual(opened["entries"][1]["role"], "assistant")

    def test_gateway_transcript_page_omits_terminal_null_cursor(self):
        tail = self._post(
            "/api/getAgentTranscriptTail",
            {"id": "agent-terminal-cursor-contract-test", "limit": 10},
        )

        self.assertEqual(tail, {"entries": []})

    def test_concurrent_duplicate_nonce_creates_one_user_entry(self):
        agent_id = "agent-concurrent-contract-test"
        nonce = "nonce-concurrent-contract-test"
        barrier = threading.Barrier(8)

        def send_once():
            barrier.wait(timeout=3)
            return self._post(
                "/api/sendPrompt",
                {"agentId": agent_id, "prompt": "concurrent hello", "clientNonce": nonce},
            )

        with ThreadPoolExecutor(max_workers=8) as pool:
            responses = list(pool.map(lambda _: send_once(), range(8)))

        self.assertEqual(responses, [{"accepted": True}] * 8)
        with TRANSCRIPT_CHANGED:
            entries = list(TRANSCRIPTS[agent_id]["entries"])
        user_entries = [
            entry for entry in entries if json.loads(base64.b64decode(entry["body"]).decode())["role"] == "user"
        ]
        self.assertEqual(len(user_entries), 1)

    def test_native_send_message_message_id_is_idempotent(self):
        agent_id = "agent-native-message-id-test"
        message_id = "message-id-contract-test"
        payload = {"agentId": agent_id, "messageId": message_id, "text": "hello"}
        self.assertTrue(
            self._post("/aiserver.v1.GrokBotService/SendGrokBotUserMessage", payload)["dispatched"]
        )
        self.assertTrue(
            self._post("/aiserver.v1.GrokBotService/SendGrokBotUserMessage", payload)["dispatched"]
        )
        with TRANSCRIPT_CHANGED:
            entries = list(TRANSCRIPTS[agent_id]["entries"])
        self.assertEqual(
            sum(1 for entry in entries if json.loads(base64.b64decode(entry["body"]).decode())["role"] == "user"),
            1,
        )

    def test_send_status_reflects_acceptance_and_missing_nonce(self):
        agent_id = "agent-send-status-test"
        message_id = "message-status-test"
        payload = {"agentId": agent_id, "messageId": message_id, "text": "hello"}
        self.assertTrue(self._post("/aiserver.v1.GrokBotService/SendGrokBotUserMessage", payload)["dispatched"])

        accepted = self._post(
            "/aiserver.v1.GrokBotService/GetGrokBotSendStatus",
            {"agentId": agent_id, "messageId": message_id},
        )
        self.assertEqual(accepted["status"], "GROK_BOT_SEND_STATUS_ACCEPTED")
        self.assertTrue(accepted["echoEntryId"])

        missing = self._post(
            "/aiserver.v1.GrokBotService/GetGrokBotSendStatus",
            {"agentId": agent_id, "messageId": "missing-status-test"},
        )
        self.assertEqual(missing["status"], "GROK_BOT_SEND_STATUS_NOT_FOUND")

    def test_prompt_state_survives_reload_from_atomic_snapshot(self):
        agent_id = "agent-reload-test"
        nonce = "nonce-reload-test"
        user_entry, record = backend.claim_user_prompt(agent_id, "persist me", nonce)
        self.assertEqual(record["status"], "accepted")
        self.assertTrue(os.path.exists(backend.PERSISTENCE_FILE))

        with TRANSCRIPT_CHANGED:
            AGENT_INDEX.clear()
            TRANSCRIPTS.clear()
            PROMPT_ACCEPTANCE.clear()
        backend._load_persisted_state()

        with TRANSCRIPT_CHANGED:
            self.assertIn(agent_id, AGENT_INDEX)
            self.assertEqual(len(TRANSCRIPTS[agent_id]["entries"]), 1)
            self.assertEqual(PROMPT_ACCEPTANCE[(agent_id, nonce)]["echoEntryId"], user_entry["entryId"])

    def test_reload_resumes_accepted_prompt_without_duplicate_echo(self):
        backend.call_model = lambda prompt: "resumed reply"
        agent_id = "agent-resume-test"
        nonce = "nonce-resume-test"
        user_entry, _ = backend.claim_user_prompt(agent_id, "resume me", nonce)

        with TRANSCRIPT_CHANGED:
            AGENT_INDEX.clear()
            TRANSCRIPTS.clear()
            PROMPT_ACCEPTANCE.clear()
        backend._load_persisted_state()
        self.assertEqual(backend.resume_pending_prompts(), 1)

        deadline = time.time() + 2
        while time.time() < deadline:
            with TRANSCRIPT_CHANGED:
                entries = list(TRANSCRIPTS.get(agent_id, {}).get("entries", []))
            if len(entries) >= 2:
                break
            time.sleep(0.05)
        self.assertEqual(len([e for e in entries if e["entryId"] == user_entry["entryId"]]), 1)
        self.assertEqual(
            json.loads(base64.b64decode(entries[1]["body"]).decode())["content"],
            "resumed reply",
        )

    def test_model_failure_is_rendered_and_status_is_rejected(self):
        backend.call_model = lambda prompt: (_ for _ in ()).throw(RuntimeError("synthetic failure"))
        agent_id = "agent-failure-test"
        nonce = "nonce-failure-test"
        self.assertEqual(
            self._post("/api/sendPrompt", {"agentId": agent_id, "prompt": "fail", "clientNonce": nonce}),
            {"accepted": True},
        )

        deadline = time.time() + 2
        tail = {"entries": []}
        while time.time() < deadline:
            tail = self._post("/api/getAgentTranscriptTail", {"id": agent_id, "limit": 10})
            if len(tail["entries"]) >= 2:
                break
            time.sleep(0.05)
        self.assertEqual(tail["entries"][1]["role"], "assistant")
        self.assertTrue(tail["entries"][1]["isError"])
        self.assertIn("synthetic failure", tail["entries"][1]["content"])
        acceptance = self._post(
            "/api/promptAcceptanceStatus", {"agentId": agent_id, "clientNonce": nonce}
        )
        self.assertEqual(acceptance["record"]["status"], "failed")
        self.assertEqual(acceptance["record"]["rejectionCode"], "LOCAL_MODEL_ERROR")

    def test_same_agent_model_calls_are_serial_and_later_turn_sees_history(self):
        active = 0
        max_active = 0
        observed = []
        guard = threading.Lock()

        def model(prompt):
            nonlocal active, max_active
            with guard:
                active += 1
                max_active = max(max_active, active)
                observed.append(prompt)
            time.sleep(0.1)
            with guard:
                active -= 1
            return f"reply-{len(observed)}"

        backend.call_model = model
        agent_id = "agent-serial-test"
        first = self._post("/api/sendPrompt", {"agentId": agent_id, "prompt": "first", "clientNonce": "nonce-1"})
        second = self._post("/api/sendPrompt", {"agentId": agent_id, "prompt": "second", "clientNonce": "nonce-2"})
        self.assertEqual(first, {"accepted": True})
        self.assertEqual(second, {"accepted": True})

        deadline = time.time() + 3
        while time.time() < deadline:
            with TRANSCRIPT_CHANGED:
                entries = list(TRANSCRIPTS.get(agent_id, {}).get("entries", []))
            if len(entries) >= 4:
                break
            time.sleep(0.05)
        self.assertGreaterEqual(len(entries), 4)
        self.assertEqual(max_active, 1)
        self.assertEqual(len(observed), 2)
        self.assertIn("Conversation history:", observed[1])


if __name__ == "__main__":
    unittest.main()
