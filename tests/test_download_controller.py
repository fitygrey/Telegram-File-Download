import tempfile
import unittest
import asyncio
import json
import os
import threading
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from telethon.errors import FileReferenceExpiredError

from telegram_download_controller import DownloadController, QRLoginManager, auto_import_session, clear_downloader_session, connect_client, import_session_file, import_string_session, qr_code_data_url
from telegram_media_core import MediaItem, append_ignore_record, append_skip_record, append_source_missing_record, media_target_path


class DashboardDefaultTests(unittest.TestCase):
    def post_dashboard_json(self, handler, path, payload):
        from http.server import ThreadingHTTPServer

        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            body = json.dumps(payload).encode("utf-8")
            request = urllib.request.Request(
                f"http://127.0.0.1:{server.server_port}{path}",
                data=body,
                headers={"content-type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=5) as response:
                return json.loads(response.read().decode("utf-8"))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

    def test_dashboard_uses_samba_volume_as_default_download_root(self):
        import telegram_dashboard

        self.assertEqual(telegram_dashboard.OUT_ROOT, Path("/Volumes/ZHITAI/telegram"))

    def test_dashboard_loads_persisted_download_settings(self):
        import telegram_dashboard

        with tempfile.TemporaryDirectory() as tmp:
            settings_path = Path(tmp) / "dashboard_settings.json"
            settings_path.write_text(
                json.dumps({"out_root": "/tmp/downloads", "max_mb": 250, "concurrency": 3}),
                encoding="utf-8",
            )
            with patch.object(telegram_dashboard, "SETTINGS_PATH", settings_path):
                self.assertEqual(telegram_dashboard.configured_out_root(), Path("/tmp/downloads"))
                self.assertEqual(telegram_dashboard.configured_max_mb(), 250)
                self.assertEqual(telegram_dashboard.configured_concurrency(), 3)

    def test_dashboard_setting_defaults_and_bounds_are_stable(self):
        import telegram_dashboard

        with tempfile.TemporaryDirectory() as tmp:
            settings_path = Path(tmp) / "dashboard_settings.json"
            settings_path.write_text(
                json.dumps({"max_mb": "bad", "concurrency": 99}),
                encoding="utf-8",
            )
            with patch.object(telegram_dashboard, "SETTINGS_PATH", settings_path):
                self.assertEqual(telegram_dashboard.configured_max_mb(), 0)
                self.assertEqual(telegram_dashboard.configured_concurrency(), 4)

    def test_download_routes_use_persisted_settings_as_defaults(self):
        import telegram_dashboard

        class FakeController:
            out_root = Path("/tmp/downloads")

            def start_months_result(self, months, max_mb, concurrency):
                return {"ok": True, "months": months, "max_mb": max_mb, "concurrency": concurrency}

        with tempfile.TemporaryDirectory() as tmp:
            settings_path = Path(tmp) / "dashboard_settings.json"
            settings_path.write_text(
                json.dumps({"out_root": "/tmp/downloads", "max_mb": 250, "concurrency": 3}),
                encoding="utf-8",
            )
            with patch.object(telegram_dashboard, "SETTINGS_PATH", settings_path), \
                    patch.object(telegram_dashboard, "controller", FakeController()):
                result = self.post_dashboard_json(
                    telegram_dashboard.DashboardHandler,
                    "/api/start-months",
                    {"months": ["2026-04"]},
                )

        self.assertTrue(result["ok"])
        self.assertEqual(result["max_mb"], 250)
        self.assertEqual(result["concurrency"], 3)

    def test_dashboard_clear_pending_downloads_route_uses_controller(self):
        import telegram_dashboard

        class FakeController:
            out_root = Path("/tmp/downloads")

            def clear_pending_downloads(self):
                return {"ok": True, "cleared": 2}

        with patch.object(telegram_dashboard, "controller", FakeController()):
            result = self.post_dashboard_json(
                telegram_dashboard.DashboardHandler,
                "/api/clear-pending-downloads",
                {},
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["cleared"], 2)

    def test_dashboard_sign_out_stops_downloads_before_clearing_session(self):
        import telegram_dashboard

        calls = []

        class FakeController:
            out_root = Path("/tmp/downloads")

            def request_stop(self):
                calls.append("stop")

        def fake_clear():
            calls.append("clear")
            return {"ok": True}

        class FakeQrLoginManager:
            def reset(self):
                calls.append("qr-reset")
                return {"ok": True}

        with patch.object(telegram_dashboard, "controller", FakeController()), \
                patch.object(telegram_dashboard, "qr_login_manager", FakeQrLoginManager()), \
                patch.object(telegram_dashboard, "clear_downloader_session", fake_clear):
            result = self.post_dashboard_json(
                telegram_dashboard.DashboardHandler,
                "/api/sign-out",
                {},
            )

        self.assertTrue(result["ok"])
        self.assertEqual(calls, ["stop", "qr-reset", "clear"])

    def test_dashboard_allows_zero_max_mb_as_unlimited_default(self):
        import telegram_dashboard

        with tempfile.TemporaryDirectory() as tmp:
            settings_path = Path(tmp) / "dashboard_settings.json"
            settings_path.write_text(json.dumps({"max_mb": 0}), encoding="utf-8")
            with patch.object(telegram_dashboard, "SETTINGS_PATH", settings_path):
                self.assertEqual(telegram_dashboard.configured_max_mb(), 0)

    def test_zero_max_mb_runs_without_size_limit(self):
        controller = DownloadController(Path("/tmp/unused"))
        calls = []

        async def fake_download_recent(days, max_bytes, concurrency):
            calls.append(("recent", days, max_bytes, concurrency))

        async def fake_download_message_ids(message_ids, max_bytes, concurrency):
            calls.append(("restore", message_ids, max_bytes, concurrency))

        controller._download_recent = fake_download_recent
        controller._download_message_ids = fake_download_message_ids

        asyncio.run(controller._run_recent(31, 0, 2))
        asyncio.run(controller._run_restore(["1"], 0, 2))

        self.assertEqual(calls[0], ("recent", 31, None, 2))
        self.assertEqual(calls[1], ("restore", ["1"], None, 2))

    def test_settings_save_failure_returns_json_error(self):
        import telegram_dashboard

        class FakeController:
            out_root = Path("/tmp/downloads")

            def set_out_root(self, out_root):
                return {"ok": True, "out_root": str(out_root)}

        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(telegram_dashboard, "SETTINGS_PATH", Path(tmp)), \
                    patch.object(telegram_dashboard, "controller", FakeController()):
                result = self.post_dashboard_json(
                    telegram_dashboard.DashboardHandler,
                    "/api/settings",
                    {"out_root": "/tmp/downloads", "max_mb": 250, "concurrency": 3},
                )

        self.assertFalse(result["ok"])
        self.assertIn("设置保存失败", result["error"])


class DownloadControllerTests(unittest.TestCase):
    def test_connect_client_uses_downloader_owned_session_path(self):
        created = []

        class FakeTelegramClient:
            def __init__(self, session, *args, **kwargs):
                created.append(session)

            async def connect(self):
                pass

            async def is_user_authorized(self):
                return True

            async def disconnect(self):
                pass

        async def run_check():
            with tempfile.TemporaryDirectory() as tmp, \
                    patch("telegram_download_controller.DOWNLOADER_SESSION_PATH", Path(tmp) / "telegram_downloader.session"), \
                    patch("telegram_download_controller.TelegramClient", FakeTelegramClient):
                client = await connect_client()
                await client.disconnect()

        asyncio.run(run_check())
        self.assertEqual(created, [str(Path(created[0]))])
        self.assertTrue(created[0].endswith("telegram_downloader.session"))

    def test_import_session_file_copies_to_downloader_session(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            source = tmp_path / "source.session"
            target = tmp_path / "telegram_downloader.session"
            string_target = tmp_path / "telegram_downloader.string_session"
            source.write_bytes(b"session")
            string_target.write_text("old", encoding="utf-8")

            with patch("telegram_download_controller.DOWNLOADER_SESSION_PATH", target), \
                    patch("telegram_download_controller.DOWNLOADER_STRING_SESSION_PATH", string_target):
                result = import_session_file(str(source))

            self.assertTrue(result["ok"])
            self.assertEqual(target.read_bytes(), b"session")
            self.assertFalse(string_target.exists())

    def test_import_string_session_writes_string_and_removes_file_session(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            target = tmp_path / "telegram_downloader.session"
            string_target = tmp_path / "telegram_downloader.string_session"
            target.write_bytes(b"old")

            with patch("telegram_download_controller.DOWNLOADER_SESSION_PATH", target), \
                    patch("telegram_download_controller.DOWNLOADER_STRING_SESSION_PATH", string_target):
                result = import_string_session("abc123")

            self.assertTrue(result["ok"])
            self.assertEqual(string_target.read_text(encoding="utf-8"), "abc123")
            self.assertFalse(target.exists())

    def test_auto_import_session_copies_newest_candidate(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old = root / "old.session"
            new = root / "new.session"
            target = root / "telegram_downloader.session"
            old.write_bytes(b"old")
            new.write_bytes(b"new")
            old.touch()
            new.touch()

            with patch("telegram_download_controller.DOWNLOADER_SESSION_PATH", target), \
                    patch("telegram_download_controller.AUTO_SESSION_ROOTS", [root]):
                result = auto_import_session()

            self.assertTrue(result["ok"])
            self.assertEqual(result["source"], str(new))
            self.assertEqual(target.read_bytes(), b"new")

    def test_clear_session_blocks_reimporting_same_session_until_it_changes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source.session"
            target = root / "telegram_downloader.session"
            revoked = root / "revoked_sessions.json"
            sign_out = root / "session_sign_out.json"
            source.write_bytes(b"old-session")
            target.write_bytes(b"old-session")

            with patch("telegram_download_controller.DOWNLOADER_SESSION_PATH", target), \
                    patch("telegram_download_controller.DOWNLOADER_STRING_SESSION_PATH", root / "telegram_downloader.string_session"), \
                    patch("telegram_download_controller.REVOKED_SESSION_FINGERPRINTS_PATH", revoked), \
                    patch("telegram_download_controller.SESSION_SIGN_OUT_PATH", sign_out):
                self.assertTrue(clear_downloader_session()["ok"])
                blocked = import_session_file(str(source))
                source.write_bytes(b"new-session")
                future = sign_out.stat().st_mtime_ns + 1_000_000
                os.utime(source, ns=(future, future))
                allowed = import_session_file(str(source))

            self.assertFalse(blocked["ok"])
            self.assertIn("已退出登录", blocked["error"])
            self.assertTrue(allowed["ok"])
            self.assertEqual(target.read_bytes(), b"new-session")

    def test_auto_import_session_ignores_signed_out_session_fingerprint(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source.session"
            target = root / "telegram_downloader.session"
            revoked = root / "revoked_sessions.json"
            sign_out = root / "session_sign_out.json"
            source.write_bytes(b"old-session")
            target.write_bytes(b"old-session")

            with patch("telegram_download_controller.DOWNLOADER_SESSION_PATH", target), \
                    patch("telegram_download_controller.DOWNLOADER_STRING_SESSION_PATH", root / "telegram_downloader.string_session"), \
                    patch("telegram_download_controller.REVOKED_SESSION_FINGERPRINTS_PATH", revoked), \
                    patch("telegram_download_controller.SESSION_SIGN_OUT_PATH", sign_out), \
                    patch("telegram_download_controller.AUTO_SESSION_ROOTS", [root]):
                self.assertTrue(clear_downloader_session()["ok"])
                blocked = auto_import_session()
                source.write_bytes(b"new-session")
                future = sign_out.stat().st_mtime_ns + 1_000_000
                os.utime(source, ns=(future, future))
                allowed = auto_import_session()

            self.assertFalse(blocked["ok"])
            self.assertIn("已退出登录", blocked["error"])
            self.assertTrue(allowed["ok"])
            self.assertEqual(target.read_bytes(), b"new-session")

    def test_auto_import_session_blocks_old_session_from_other_directory_after_sign_out(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            current = root / "current"
            other = root / "other"
            current.mkdir()
            other.mkdir()
            target = current / "telegram_downloader.session"
            old_external = other / "telegram_downloader.session"
            revoked = current / "revoked_sessions.json"
            sign_out = current / "session_sign_out.json"
            target.write_bytes(b"current-session")
            old_external.write_bytes(b"other-old-session")

            with patch("telegram_download_controller.DOWNLOADER_SESSION_PATH", target), \
                    patch("telegram_download_controller.DOWNLOADER_STRING_SESSION_PATH", current / "telegram_downloader.string_session"), \
                    patch("telegram_download_controller.REVOKED_SESSION_FINGERPRINTS_PATH", revoked), \
                    patch("telegram_download_controller.SESSION_SIGN_OUT_PATH", sign_out), \
                    patch("telegram_download_controller.AUTO_SESSION_ROOTS", [other]):
                self.assertTrue(clear_downloader_session()["ok"])
                blocked = auto_import_session()

            self.assertFalse(blocked["ok"])
            self.assertIn("已退出登录", blocked["error"])
            self.assertFalse(target.exists())

    def test_qr_code_data_url_returns_png_data_url(self):
        data_url = qr_code_data_url("tg://login?token=test")

        self.assertTrue(data_url.startswith("data:image/png;base64,"))

    def test_qr_password_submission_requires_password_state(self):
        manager = QRLoginManager()

        self.assertFalse(manager.submit_password("secret")["ok"])
        manager._set_state(state="password_required")
        result = manager.submit_password("secret")

        self.assertTrue(result["ok"])
        self.assertEqual(manager.status()["state"], "password_submitted")

    def test_qr_reset_clears_authorized_state_and_session_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            session_file = root / "telegram_downloader.session"
            string_file = root / "telegram_downloader.string_session"
            session_file.write_bytes(b"session")
            string_file.write_text("string", encoding="utf-8")
            manager = QRLoginManager()
            manager._set_state(state="authorized")

            with patch("telegram_download_controller.DOWNLOADER_SESSION_PATH", session_file), \
                    patch("telegram_download_controller.DOWNLOADER_STRING_SESSION_PATH", string_file), \
                    patch("telegram_download_controller.REVOKED_SESSION_FINGERPRINTS_PATH", root / "revoked_sessions.json"), \
                    patch("telegram_download_controller.SESSION_SIGN_OUT_PATH", root / "session_sign_out.json"):
                result = manager.reset()

            self.assertTrue(result["ok"])
            self.assertEqual(manager.status(), {"state": "idle"})
            self.assertFalse(session_file.exists())
            self.assertFalse(string_file.exists())

    def test_auth_status_returns_json_for_unauthorized_session(self):
        async def run_check():
            with tempfile.TemporaryDirectory() as tmp:
                target = Path(tmp) / "telegram_downloader.session"
                target.write_bytes(b"not authorized")
                with patch("telegram_download_controller.DOWNLOADER_SESSION_PATH", target), \
                        patch("telegram_download_controller.connect_client", side_effect=RuntimeError("not authorized")):
                    from telegram_download_controller import downloader_auth_status

                    return await downloader_auth_status()

        status = asyncio.run(run_check())

        self.assertFalse(status["authorized"])
        self.assertTrue(status["session_exists"])
        self.assertIn("not authorized", status["error"])

    def test_auth_status_returns_busy_when_download_client_lock_is_held(self):
        async def run_check():
            with tempfile.TemporaryDirectory() as tmp:
                target = Path(tmp) / "telegram_downloader.session"
                target.write_bytes(b"session")
                with patch("telegram_download_controller.DOWNLOADER_SESSION_PATH", target), \
                        patch("telegram_download_controller.DOWNLOADER_STRING_SESSION_PATH", Path(tmp) / "telegram_downloader.string_session"):
                    from telegram_download_controller import TELEGRAM_CLIENT_LOCK, downloader_auth_status

                    TELEGRAM_CLIENT_LOCK.acquire()
                    try:
                        return await asyncio.wait_for(downloader_auth_status(), timeout=0.2)
                    finally:
                        TELEGRAM_CLIENT_LOCK.release()

        status = asyncio.run(run_check())

        self.assertTrue(status["authorized"])
        self.assertTrue(status["session_exists"])
        self.assertTrue(status["busy"])

    def test_connect_client_serializes_parallel_telegram_connections(self):
        created = []

        class FakeTelegramClient:
            def __init__(self, *args, **kwargs):
                created.append(self)

            async def connect(self):
                pass

            async def is_user_authorized(self):
                return True

            async def disconnect(self):
                pass

        async def run_check():
            with patch("telegram_download_controller.TelegramClient", FakeTelegramClient):
                first = await connect_client()
                second_task = asyncio.create_task(connect_client())
                await asyncio.sleep(0.05)
                self.assertEqual(len(created), 1)

                await first.disconnect()
                second = await asyncio.wait_for(second_task, timeout=1)
                self.assertEqual(len(created), 2)
                await second.disconnect()

        asyncio.run(run_check())

    def test_pause_and_stop_are_recorded_in_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            controller = DownloadController(Path(tmp))

            controller.request_pause()
            self.assertTrue(controller.status()["pause_requested"])

            controller.request_stop()
            status = controller.status()
            self.assertTrue(status["stop_requested"])
            self.assertEqual(status["state"], "stopping")

    def test_partial_large_file_stays_restorable(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = root / "2026-04-20/file.mp4"
            target.parent.mkdir(parents=True)
            target.write_bytes(b"partial")
            item = MediaItem(
                message_id=6,
                date=datetime(2026, 4, 20, 1, 2, 3, tzinfo=timezone.utc),
                size_bytes=200 * 1024 * 1024,
                file_name="large.mp4",
                extension=".mp4",
                kind="video",
            )
            append_skip_record(root / "skipped_over_100mb.csv", item, target)
            controller = DownloadController(root)

            records = controller.restorable_records()

            self.assertEqual(len(records), 1)
            self.assertEqual(records[0]["message_id"], "6")

    def test_complete_large_file_is_not_restorable(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = root / "2026-04-20/file.mp4"
            target.parent.mkdir(parents=True)
            target.write_bytes(b"complete")
            item = MediaItem(
                message_id=7,
                date=datetime(2026, 4, 20, 1, 2, 3, tzinfo=timezone.utc),
                size_bytes=len(b"complete"),
                file_name="large.mp4",
                extension=".mp4",
                kind="video",
            )
            append_skip_record(root / "skipped_over_100mb.csv", item, target)
            controller = DownloadController(root)

            self.assertEqual(controller.restorable_records(), [])

    def test_ignored_skip_record_is_not_restorable(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = root / "2026-04-20/file.mp4"
            item = MediaItem(
                message_id=71,
                date=datetime(2026, 4, 20, 1, 2, 3, tzinfo=timezone.utc),
                size_bytes=200 * 1024 * 1024,
                file_name="large.mp4",
                extension=".mp4",
                kind="video",
            )
            append_skip_record(root / "skipped_over_100mb.csv", item, target)
            append_ignore_record(root / "ignored_downloads.csv", item, target, "deleted by user")
            controller = DownloadController(root)

            self.assertEqual(controller.restorable_records(), [])

    def test_source_missing_skip_record_is_not_restorable(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = root / "2026-04-20/file.mp4"
            item = MediaItem(
                message_id=74,
                date=datetime(2026, 4, 20, 1, 2, 3, tzinfo=timezone.utc),
                size_bytes=200 * 1024 * 1024,
                file_name="large.mp4",
                extension=".mp4",
                kind="video",
            )
            append_skip_record(root / "skipped_over_100mb.csv", item, target)
            append_source_missing_record(
                root / "source_missing_downloads.csv",
                {
                    "message_id": "74",
                    "date": item.date.isoformat(),
                    "size_bytes": str(item.size_bytes),
                    "size_mb": "200.00",
                    "target_path": str(target),
                },
                "message has no media",
            )
            controller = DownloadController(root)

            self.assertEqual(controller.restorable_records(), [])
            self.assertEqual(controller.status()["stats"]["source_missing"], 1)

    def test_failed_download_record_is_restorable(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            failed_log = root / "failed_downloads.csv"
            failed_log.write_text(
                "message_id,date,size_bytes,size_mb,target_path,failed_at,error\n"
                "73,2026-04-20T09:02:03+08:00,209715200,200.00,"
                f"{root / '2026-04-20/file.mp4'},2026-05-30T12:00:00+08:00,TimeoutError\n",
                encoding="utf-8",
            )
            controller = DownloadController(root)

            records = controller.restorable_records()

            self.assertEqual([record["message_id"] for record in records], ["73"])

    def test_resumed_skipped_records_move_from_skipped_to_pending_downloads(self):
        class LiveThread:
            def is_alive(self):
                return True

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = MediaItem(
                message_id=71,
                date=datetime(2026, 4, 20, 1, 2, 3, tzinfo=timezone.utc),
                size_bytes=200 * 1024 * 1024,
                file_name="first.mp4",
                extension=".mp4",
                kind="video",
            )
            second = MediaItem(
                message_id=72,
                date=datetime(2026, 4, 21, 1, 2, 3, tzinfo=timezone.utc),
                size_bytes=210 * 1024 * 1024,
                file_name="second.mp4",
                extension=".mp4",
                kind="video",
            )
            append_skip_record(root / "skipped_over_100mb.csv", first, root / "2026-04-20/first.mp4")
            append_skip_record(root / "skipped_over_100mb.csv", second, root / "2026-04-21/second.mp4")
            controller = DownloadController(root)
            controller._download_thread = LiveThread()

            self.assertTrue(controller.resume_skipped(["71"]))
            status = controller.status()

            self.assertEqual([record["message_id"] for record in status["skipped"]], ["72"])
            self.assertEqual([record["message_id"] for record in status["pending_downloads"]], ["71"])
            self.assertEqual(status["stats"]["restorable"], 1)

    def test_resuming_all_skipped_records_clears_visible_skipped_list(self):
        class LiveThread:
            def is_alive(self):
                return True

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for message_id in (81, 82):
                item = MediaItem(
                    message_id=message_id,
                    date=datetime(2026, 4, 20, 1, 2, 3, tzinfo=timezone.utc),
                    size_bytes=200 * 1024 * 1024,
                    file_name=f"{message_id}.mp4",
                    extension=".mp4",
                    kind="video",
                )
                append_skip_record(root / "skipped_over_100mb.csv", item, root / f"2026-04-20/{message_id}.mp4")
            controller = DownloadController(root)
            controller._download_thread = LiveThread()

            self.assertTrue(controller.resume_skipped(["81", "82"]))
            status = controller.status()

            self.assertEqual(status["skipped"], [])
            self.assertEqual([record["message_id"] for record in status["pending_downloads"]], ["81", "82"])
            self.assertEqual(status["stats"]["restorable"], 0)

    def test_clear_pending_downloads_returns_skipped_records_to_skipped_list(self):
        class LiveThread:
            def is_alive(self):
                return True

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            item = MediaItem(
                message_id=81,
                date=datetime(2026, 4, 20, 1, 2, 3, tzinfo=timezone.utc),
                size_bytes=200 * 1024 * 1024,
                file_name="81.mp4",
                extension=".mp4",
                kind="video",
            )
            append_skip_record(root / "skipped_over_100mb.csv", item, root / "2026-04-20/81.mp4")
            controller = DownloadController(root)
            controller._download_thread = LiveThread()

            self.assertTrue(controller.resume_skipped(["81"]))
            result = controller.clear_pending_downloads()
            status = controller.status()

            self.assertEqual(result["cleared"], 1)
            self.assertEqual(status["pending_downloads"], [])
            self.assertEqual([record["message_id"] for record in status["skipped"]], ["81"])

    def test_clear_pending_downloads_returns_failed_records_to_failed_list(self):
        class LiveThread:
            def is_alive(self):
                return True

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            item = MediaItem(
                message_id=83,
                date=datetime(2026, 4, 20, 1, 2, 3, tzinfo=timezone.utc),
                size_bytes=200 * 1024 * 1024,
                file_name="83.mp4",
                extension=".mp4",
                kind="video",
            )
            append_skip_record(root / "failed_downloads.csv", item, root / "2026-04-20/83.mp4")
            controller = DownloadController(root)
            controller._download_thread = LiveThread()

            self.assertTrue(controller.resume_skipped(["83"]))
            result = controller.clear_pending_downloads()
            status = controller.status()

            self.assertEqual(result["cleared"], 1)
            self.assertEqual(status["pending_downloads"], [])
            self.assertEqual([record["message_id"] for record in status["failed_downloads"]], ["83"])

    def test_status_exposes_failed_records_that_are_still_retryable(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            complete = root / "2026-04-20/complete.mp4"
            complete.parent.mkdir(parents=True)
            complete.write_bytes(b"ok")
            failed_log = root / "failed_downloads.csv"
            failed_log.write_text(
                "message_id,date,size_bytes,size_mb,target_path,failed_at,error\n"
                f"73,2026-04-20T09:02:03+08:00,209715200,200.00,{root / '2026-04-20/file.mp4'},2026-05-30T12:00:00+08:00,TimeoutError\n"
                f"74,2026-04-20T09:02:03+08:00,2,0.00,{complete},2026-05-30T12:00:00+08:00,TimeoutError\n",
                encoding="utf-8",
            )
            controller = DownloadController(root)

            status = controller.status()

            self.assertEqual([record["message_id"] for record in status["failed_downloads"]], ["73"])

    def test_windows_skip_path_is_checked_under_download_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = root / "2026-04-20/file.mp4"
            target.parent.mkdir(parents=True)
            target.write_bytes(b"complete")
            item = MediaItem(
                message_id=70,
                date=datetime(2026, 4, 20, 1, 2, 3, tzinfo=timezone.utc),
                size_bytes=len(b"complete"),
                file_name="large.mp4",
                extension=".mp4",
                kind="video",
            )
            append_skip_record(root / "skipped_over_100mb.csv", item, Path(r"Z:\telegram\2026-04-20\file.mp4"))
            controller = DownloadController(root)

            self.assertEqual(controller.restorable_records(), [])

    def test_status_reuses_restorable_records_for_count_and_list(self):
        with tempfile.TemporaryDirectory() as tmp:
            controller = DownloadController(Path(tmp))
            calls = []

            def fake_restorable_records():
                calls.append(True)
                return [{"message_id": "1"}]

            controller.restorable_records = fake_restorable_records

            status = controller.status()

            self.assertEqual(status["stats"]["restorable"], 1)
            self.assertEqual(status["skipped"], [{"message_id": "1"}])
            self.assertEqual(len(calls), 1)

    def test_restorable_records_are_cached_between_status_polls(self):
        with tempfile.TemporaryDirectory() as tmp:
            controller = DownloadController(Path(tmp))
            calls = []

            def compute_records():
                calls.append(True)
                return [{"message_id": "1"}]

            controller._compute_restorable_records = compute_records

            self.assertEqual(controller.restorable_records(), [{"message_id": "1"}])
            self.assertEqual(controller.restorable_records(), [{"message_id": "1"}])

            self.assertEqual(len(calls), 1)

    def test_month_scan_state_is_persisted(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            controller = DownloadController(root)
            controller._months = {
                "2026-04": {"total": 3, "downloaded": 1, "skipped": 1, "pending": 1}
            }
            item = MediaItem(
                message_id=42,
                date=datetime(2026, 4, 20, 8, 30, tzinfo=timezone.utc),
                size_bytes=1024,
                file_name="clip.mp4",
                extension=".mp4",
                kind="video",
            )
            controller._month_items = {"2026-04": [controller._public_item(item)]}
            controller._selected_month = "2026-04"
            controller._scan_progress = {"messages": 10, "media": 3, "months": 1}

            controller._write_state_unlocked()
            restored = DownloadController(root)
            status = restored.status()

            self.assertEqual(
                status["months"],
                {"2026-04": {"total": 3, "downloaded": 1, "skipped": 1, "pending": 1}},
            )
            self.assertEqual(status["selected_month"], "2026-04")
            self.assertEqual(status["scan_progress"], {"messages": 10, "media": 3, "months": 1})
            self.assertEqual(status["pending_downloads"], [])
            self.assertEqual(restored._month_items["2026-04"][0]["message_id"], "42")

    def test_set_out_root_switches_logs_and_loads_new_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            first = base / "first"
            second = base / "second"
            second.mkdir()
            (second / "download_state.json").write_text(
                json.dumps(
                    {
                        "months": {
                            "2026-05": {
                                "total": 2,
                                "downloaded": 1,
                                "skipped": 0,
                                "pending": 1,
                            }
                        },
                        "scan_progress": {"messages": 8, "media": 2, "months": 1},
                        "selected_month": "2026-05",
                    }
                ),
                encoding="utf-8",
            )
            controller = DownloadController(first)

            result = controller.set_out_root(second)
            status = controller.status()

            self.assertTrue(result["ok"])
            self.assertEqual(controller.skip_log, second / "skipped_over_100mb.csv")
            self.assertEqual(status["out_root"], str(second))
            self.assertEqual(status["months"]["2026-05"]["pending"], 1)
            self.assertEqual(status["selected_month"], "2026-05")

    def test_current_progress_tracks_downloaded_bytes_percent_and_speed(self):
        with tempfile.TemporaryDirectory() as tmp:
            controller = DownloadController(Path(tmp))
            item = MediaItem(
                message_id=8,
                date=datetime(2026, 4, 20, 1, 2, 3, tzinfo=timezone.utc),
                size_bytes=1000,
                file_name="clip.mp4",
                extension=".mp4",
                kind="video",
            )

            controller._set_current_progress(
                item=item,
                target=Path(tmp) / "clip.mp4",
                downloaded_bytes=250,
                total_bytes=1000,
                started_at=10.0,
                now=12.0,
            )

            current = controller.status()["current"]
            self.assertEqual(current["message_id"], 8)
            self.assertEqual(current["downloaded_bytes"], 250)
            self.assertEqual(current["total_bytes"], 1000)
            self.assertEqual(current["percent"], 25.0)
            self.assertEqual(current["speed_bytes_per_sec"], 125.0)

    def test_current_progress_tracks_multiple_active_downloads(self):
        with tempfile.TemporaryDirectory() as tmp:
            controller = DownloadController(Path(tmp))
            first = MediaItem(
                message_id=81,
                date=datetime(2026, 4, 20, 1, 2, 3, tzinfo=timezone.utc),
                size_bytes=1000,
                file_name="first.mp4",
                extension=".mp4",
                kind="video",
            )
            second = MediaItem(
                message_id=82,
                date=datetime(2026, 4, 20, 1, 3, 3, tzinfo=timezone.utc),
                size_bytes=2000,
                file_name="second.mp4",
                extension=".mp4",
                kind="video",
            )

            controller._set_current_progress(first, Path(tmp) / "first.mp4", 250, 1000, 10.0, now=12.0)
            controller._set_current_progress(second, Path(tmp) / "second.mp4", 1000, 2000, 10.0, now=12.0)

            status = controller.status()
            self.assertEqual([item["message_id"] for item in status["currents"]], [81, 82])
            self.assertEqual(status["current"]["message_id"], 81)

    def test_start_month_records_download_concurrency(self):
        class LiveThread:
            def is_alive(self):
                return True

        with tempfile.TemporaryDirectory() as tmp:
            controller = DownloadController(Path(tmp))
            controller._download_thread = LiveThread()
            controller._state = "running"
            controller._months = {
                "2026-04": {"total": 1, "downloaded": 0, "skipped": 0, "pending": 1},
            }
            item = MediaItem(
                message_id=101,
                date=datetime(2026, 4, 20, 8, 30, tzinfo=timezone.utc),
                size_bytes=1024,
                file_name="clip.mp4",
                extension=".mp4",
                kind="video",
            )
            controller._month_items = {"2026-04": [controller._public_item(item)]}

            accepted = controller.start_month("2026-04", max_mb=100, concurrency=3)

            status = controller.status()
            self.assertTrue(accepted)
            self.assertEqual(status["pending_jobs"][0]["concurrency"], 3)

    def test_start_month_expands_to_file_pending_item_when_download_is_already_running(self):
        class LiveThread:
            def is_alive(self):
                return True

        with tempfile.TemporaryDirectory() as tmp:
            controller = DownloadController(Path(tmp))
            controller._download_thread = LiveThread()
            controller._state = "running"
            controller._months = {
                "2026-04": {"total": 1, "downloaded": 0, "skipped": 0, "pending": 1},
            }
            item = MediaItem(
                message_id=101,
                date=datetime(2026, 4, 20, 8, 30, tzinfo=timezone.utc),
                size_bytes=1024,
                file_name="clip.mp4",
                extension=".mp4",
                kind="video",
            )
            controller._month_items = {"2026-04": [controller._public_item(item)]}

            accepted = controller.start_month("2026-04", max_mb=100)

            status = controller.status()
            self.assertTrue(accepted)
            self.assertEqual(status["pending_jobs"][0]["kind"], "restore")
            self.assertEqual(status["pending_jobs"][0]["label"], "下载 1 项")
            self.assertEqual(status["pending_downloads"][0]["message_id"], "101")

    def test_start_months_requires_scanned_file_index(self):
        class LiveThread:
            def is_alive(self):
                return True

        with tempfile.TemporaryDirectory() as tmp:
            controller = DownloadController(Path(tmp))
            controller._download_thread = LiveThread()
            controller._state = "running"
            controller._months = {
                "2026-04": {"total": 1, "downloaded": 0, "skipped": 0, "pending": 1},
                "2026-03": {"total": 1, "downloaded": 0, "skipped": 0, "pending": 1},
                "2026-02": {"total": 1, "downloaded": 0, "skipped": 0, "pending": 1},
            }

            queued = controller.start_months(["2026-03", "2026-02"], max_mb=100)

            self.assertEqual(queued, 0)
            self.assertEqual(controller.status()["pending_jobs"], [])

    def test_start_months_result_reports_missing_file_index(self):
        with tempfile.TemporaryDirectory() as tmp:
            controller = DownloadController(Path(tmp))
            controller._months = {
                "2026-04": {"total": 1, "downloaded": 0, "skipped": 0, "pending": 1},
            }

            result = controller.start_months_result(["2026-04"], max_mb=100)

            self.assertFalse(result["ok"])
            self.assertEqual(result["queued"], 0)
            self.assertIn("重新扫描月份", result["error"])

    def test_start_months_expands_scanned_months_to_file_pending_items(self):
        class LiveThread:
            def is_alive(self):
                return True

        with tempfile.TemporaryDirectory() as tmp:
            controller = DownloadController(Path(tmp))
            controller._download_thread = LiveThread()
            controller._state = "running"
            controller._months = {
                "2026-04": {"total": 2, "downloaded": 0, "skipped": 0, "pending": 2},
            }
            first = MediaItem(
                message_id=101,
                date=datetime(2026, 4, 20, 8, 30, tzinfo=timezone.utc),
                size_bytes=1024,
                file_name="first.mp4",
                extension=".mp4",
                kind="video",
            )
            second = MediaItem(
                message_id=102,
                date=datetime(2026, 4, 21, 8, 30, tzinfo=timezone.utc),
                size_bytes=2048,
                file_name="second.mp4",
                extension=".mp4",
                kind="video",
            )
            controller._month_items = {
                "2026-04": [controller._public_item(first), controller._public_item(second)]
            }

            queued = controller.start_months(["2026-04"], max_mb=100)
            status = controller.status()

            self.assertEqual(queued, 2)
            self.assertEqual(status["pending_jobs"][0]["kind"], "restore")
            self.assertEqual(status["pending_jobs"][0]["label"], "下载 2 项")
            self.assertEqual(
                [record["message_id"] for record in status["pending_downloads"]],
                ["101", "102"],
            )
            self.assertIn("first.mp4", status["pending_downloads"][0]["target_path"])

    def test_clear_pending_downloads_returns_month_records_to_month_pending(self):
        class LiveThread:
            def is_alive(self):
                return True

        with tempfile.TemporaryDirectory() as tmp:
            controller = DownloadController(Path(tmp))
            controller._download_thread = LiveThread()
            controller._state = "running"
            controller._months = {
                "2026-04": {"total": 1, "downloaded": 0, "skipped": 0, "pending": 1},
            }
            item = MediaItem(
                message_id=101,
                date=datetime(2026, 4, 20, 8, 30, tzinfo=timezone.utc),
                size_bytes=1024,
                file_name="first.mp4",
                extension=".mp4",
                kind="video",
            )
            controller._month_items = {"2026-04": [controller._public_item(item)]}

            self.assertEqual(controller.start_months(["2026-04"], max_mb=100), 1)
            result = controller.clear_pending_downloads()
            status = controller.status()

            self.assertEqual(result["cleared"], 1)
            self.assertEqual(status["pending_downloads"], [])
            self.assertEqual(status["months"]["2026-04"]["pending"], 1)

    def test_active_restore_job_keeps_unstarted_files_in_pending_downloads(self):
        with tempfile.TemporaryDirectory() as tmp:
            controller = DownloadController(Path(tmp))
            first = MediaItem(
                message_id=101,
                date=datetime(2026, 4, 20, 8, 30, tzinfo=timezone.utc),
                size_bytes=1024,
                file_name="first.mp4",
                extension=".mp4",
                kind="video",
            )
            second = MediaItem(
                message_id=102,
                date=datetime(2026, 4, 21, 8, 30, tzinfo=timezone.utc),
                size_bytes=2048,
                file_name="second.mp4",
                extension=".mp4",
                kind="video",
            )
            controller._active_job = {
                "kind": "restore",
                "label": "下载 2 项",
                "message_ids": ["101", "102"],
                "items": [controller._public_item(first), controller._public_item(second)],
            }
            controller._active_pending_ids = {"102"}
            controller._currents = {101: {"message_id": 101}}

            status = controller.status()

            self.assertEqual(
                [record["message_id"] for record in status["pending_downloads"]],
                ["102"],
            )
            self.assertIn("second.mp4", status["pending_downloads"][0]["target_path"])

    def test_paused_queue_keeps_active_job_and_pending_downloads(self):
        with tempfile.TemporaryDirectory() as tmp:
            controller = DownloadController(Path(tmp))
            item = MediaItem(
                message_id=102,
                date=datetime(2026, 4, 21, 8, 30, tzinfo=timezone.utc),
                size_bytes=2048,
                file_name="second.mp4",
                extension=".mp4",
                kind="video",
            )
            controller._queue.append(
                {
                    "kind": "restore",
                    "label": "下载 1 项",
                    "message_ids": ["102"],
                    "items": [controller._public_item(item)],
                    "source": "restore",
                    "max_mb": 0,
                    "concurrency": 1,
                }
            )

            async def fake_run_download_job(job):
                with controller._lock:
                    controller._pause_requested = True
                    controller._state = "paused"
                    controller._current = {
                        "message_id": 102,
                        "target": "second.mp4",
                        "downloaded_bytes": 128,
                        "total_bytes": 2048,
                        "percent": 6.25,
                    }
                    controller._currents = {102: dict(controller._current)}

            controller._run_download_job = fake_run_download_job

            asyncio.run(controller._run_queue())
            status = controller.status()

            self.assertEqual(status["state"], "paused")
            self.assertEqual(status["active_job"]["label"], "下载 1 项")
            self.assertEqual(status["pending_jobs"], [])
            self.assertEqual(status["pending_downloads"], [])
            self.assertEqual([record["message_id"] for record in status["currents"]], [102])

    def test_paused_active_job_shows_only_unstarted_pending_downloads(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            complete_target = root / "2026-04-20/complete.mp4"
            complete_target.parent.mkdir(parents=True)
            complete_target.write_bytes(b"done")
            partial_target = root / "2026-04-20/partial.mp4"
            partial_target.write_bytes(b"part")
            complete = {
                "message_id": "101",
                "date": "2026-04-20T08:30:00+00:00",
                "size_bytes": str(len(b"done")),
                "size_mb": "0.00",
                "target_path": str(complete_target),
            }
            partial = {
                "message_id": "102",
                "date": "2026-04-20T08:31:00+00:00",
                "size_bytes": "10",
                "size_mb": "0.00",
                "target_path": str(partial_target),
            }
            missing = {
                "message_id": "103",
                "date": "2026-04-20T08:32:00+00:00",
                "size_bytes": "10",
                "size_mb": "0.00",
                "target_path": str(root / "2026-04-20/missing.mp4"),
            }
            controller = DownloadController(root)
            controller._state = "paused"
            controller._pause_requested = True
            controller._active_job = {
                "kind": "restore",
                "label": "恢复 3 项",
                "message_ids": ["101", "102", "103"],
                "items": [complete, partial, missing],
                "source": "restore",
                "max_mb": 0,
                "concurrency": 2,
            }
            controller._active_pending_ids = {"102"}

            status = controller.status()

            self.assertEqual(
                [record["message_id"] for record in status["pending_downloads"]],
                ["102"],
            )

    def test_paused_status_reconstructs_current_downloads_when_progress_was_cleared(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            partial_target = root / "2026-04-20/partial.mp4"
            partial_target.parent.mkdir(parents=True)
            partial_target.write_bytes(b"part")
            controller = DownloadController(root)
            controller._state = "paused"
            controller._pause_requested = True
            controller._active_job = {
                "kind": "restore",
                "label": "恢复 2 项",
                "message_ids": ["102", "103"],
                "items": [
                    {
                        "message_id": "102",
                        "date": "2026-04-20T08:31:00+00:00",
                        "size_bytes": "10",
                        "size_mb": "0.00",
                        "target_path": str(partial_target),
                    },
                    {
                        "message_id": "103",
                        "date": "2026-04-20T08:32:00+00:00",
                        "size_bytes": "10",
                        "size_mb": "0.00",
                        "target_path": str(root / "2026-04-20/missing.mp4"),
                    },
                ],
                "source": "restore",
                "max_mb": 0,
                "concurrency": 2,
            }
            controller._active_pending_ids = {"103"}

            status = controller.status()

            self.assertEqual([record["message_id"] for record in status["currents"]], [102])
            self.assertEqual(status["currents"][0]["downloaded_bytes"], 4)
            self.assertEqual([record["message_id"] for record in status["pending_downloads"]], ["103"])

    def test_paused_status_syncs_currents_before_pending_downloads(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            partial_target = root / "2026-04-20/partial.mp4"
            partial_target.parent.mkdir(parents=True)
            partial_target.write_bytes(b"part")
            controller = DownloadController(root)
            controller._state = "paused"
            controller._pause_requested = True
            controller._active_job = {
                "kind": "restore",
                "label": "恢复 1 项",
                "message_ids": ["102"],
                "items": [{
                    "message_id": "102",
                    "date": "2026-04-20T08:31:00+00:00",
                    "size_bytes": "10",
                    "size_mb": "0.00",
                    "target_path": str(partial_target),
                }],
                "source": "restore",
                "max_mb": 0,
                "concurrency": 1,
            }
            controller._active_pending_ids = set()

            status = controller.status()

            self.assertEqual([record["message_id"] for record in status["currents"]], [102])
            self.assertEqual(status["pending_downloads"], [])

    def test_resume_continues_paused_active_job(self):
        class DeadThread:
            def is_alive(self):
                return False

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = root / "2026-04-20/missing.mp4"
            controller = DownloadController(root)
            controller._state = "paused"
            controller._pause_requested = True
            controller._download_thread = DeadThread()
            controller._active_job = {
                "kind": "restore",
                "label": "恢复 1 项",
                "message_ids": ["103"],
                "items": [{
                    "message_id": "103",
                    "date": "2026-04-20T08:32:00+00:00",
                    "size_bytes": "10",
                    "size_mb": "0.00",
                    "target_path": str(target),
                }],
                "source": "restore",
                "max_mb": 0,
                "concurrency": 1,
            }
            controller._active_pending_ids = {"103"}
            started = []

            def fake_start_queue_thread_unlocked():
                started.append(True)

            controller._start_queue_thread_unlocked = fake_start_queue_thread_unlocked

            self.assertTrue(controller.request_resume())

            status = controller.status()
            self.assertEqual(status["state"], "running")
            self.assertFalse(status["pause_requested"])
            self.assertEqual(started, [True])
            self.assertEqual([record["message_id"] for record in status["pending_downloads"]], ["103"])

    def test_resume_accepts_idle_active_job_left_after_restart(self):
        class DeadThread:
            def is_alive(self):
                return False

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            controller = DownloadController(root)
            controller._state = "idle"
            controller._pause_requested = True
            controller._download_thread = DeadThread()
            controller._active_job = {
                "kind": "restore",
                "label": "恢复 1 项",
                "message_ids": ["103"],
                "items": [{
                    "message_id": "103",
                    "date": "2026-04-20T08:32:00+00:00",
                    "size_bytes": "10",
                    "size_mb": "0.00",
                    "target_path": str(root / "2026-04-20/missing.mp4"),
                }],
                "source": "restore",
                "max_mb": 0,
                "concurrency": 1,
            }
            started = []

            def fake_start_queue_thread_unlocked():
                started.append(True)

            controller._start_queue_thread_unlocked = fake_start_queue_thread_unlocked

            self.assertTrue(controller.request_resume())

            status = controller.status()
            self.assertEqual(status["state"], "running")
            self.assertFalse(status["pause_requested"])
            self.assertEqual(started, [True])
            self.assertEqual([record["message_id"] for record in status["pending_downloads"]], ["103"])

    def test_resume_recomputes_paused_current_files_for_download(self):
        class DeadThread:
            def is_alive(self):
                return False

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            partial_target = root / "2026-04-20/partial.mp4"
            partial_target.parent.mkdir(parents=True)
            partial_target.write_bytes(b"part")
            controller = DownloadController(root)
            controller._state = "paused"
            controller._pause_requested = True
            controller._download_thread = DeadThread()
            controller._active_job = {
                "kind": "restore",
                "label": "恢复 2 项",
                "message_ids": ["102", "103"],
                "items": [
                    {
                        "message_id": "102",
                        "date": "2026-04-20T08:31:00+00:00",
                        "size_bytes": "10",
                        "size_mb": "0.00",
                        "target_path": str(partial_target),
                    },
                    {
                        "message_id": "103",
                        "date": "2026-04-20T08:32:00+00:00",
                        "size_bytes": "10",
                        "size_mb": "0.00",
                        "target_path": str(root / "2026-04-20/missing.mp4"),
                    },
                ],
                "source": "restore",
                "max_mb": 0,
                "concurrency": 1,
            }
            controller._active_pending_ids = {"103"}
            started = []

            def fake_start_queue_thread_unlocked():
                started.append(True)

            controller._start_queue_thread_unlocked = fake_start_queue_thread_unlocked

            self.assertEqual(
                [record["message_id"] for record in controller.status()["pending_downloads"]],
                ["103"],
            )
            self.assertEqual(
                [record["message_id"] for record in controller.status()["currents"]],
                [102],
            )
            self.assertTrue(controller.request_resume())

            self.assertEqual(controller._active_pending_ids, {"102", "103"})
            self.assertEqual(started, [True])
            self.assertEqual(
                [record["message_id"] for record in controller.status()["currents"]],
                [102],
            )

    def test_resume_active_job_keeps_paused_progress_until_new_progress_arrives(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = root / "2026-04-20/partial.mp4"
            target.parent.mkdir(parents=True)
            target.write_bytes(b"part")
            controller = DownloadController(root)
            controller._state = "running"
            controller._pause_requested = False
            controller._active_job = {
                "kind": "restore",
                "label": "恢复 1 项",
                "message_ids": ["102"],
                "items": [{
                    "message_id": "102",
                    "date": "2026-04-20T08:31:00+00:00",
                    "size_bytes": "10",
                    "size_mb": "0.00",
                    "target_path": str(target),
                }],
                "source": "restore",
                "max_mb": 0,
                "concurrency": 1,
            }
            controller._active_pending_ids = {"102"}
            controller._current = {
                "message_id": 102,
                "target": str(target),
                "downloaded_bytes": 4,
                "total_bytes": 10,
                "percent": 40.0,
            }
            controller._currents = {102: dict(controller._current)}

            async def fake_run_download_job(job):
                with controller._lock:
                    controller._state = "paused"

            controller._run_download_job = fake_run_download_job

            asyncio.run(controller._run_queue())
            status = controller.status()

            self.assertEqual([record["message_id"] for record in status["currents"]], [102])
            self.assertEqual(status["currents"][0]["percent"], 40.0)

    def test_state_reload_preserves_paused_active_job_details(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            controller = DownloadController(root)
            controller._state = "paused"
            controller._pause_requested = True
            controller._active_job = {
                "kind": "restore",
                "label": "恢复 1 项",
                "message_ids": ["103"],
                "items": [{
                    "message_id": "103",
                    "date": "2026-04-20T08:32:00+00:00",
                    "size_bytes": "10",
                    "size_mb": "0.00",
                    "target_path": str(root / "2026-04-20/missing.mp4"),
                }],
                "source": "restore",
                "max_mb": 0,
                "concurrency": 1,
            }
            controller._active_pending_ids = {"103"}
            with controller._lock:
                controller._write_state_unlocked()

            reloaded = DownloadController(root)
            status = reloaded.status()

            self.assertEqual(status["state"], "paused")
            self.assertTrue(status["pause_requested"])
            self.assertEqual(status["active_job"]["label"], "恢复 1 项")
            self.assertEqual([record["message_id"] for record in status["pending_downloads"]], ["103"])

    def test_state_reload_treats_idle_active_job_as_paused(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_file = root / "download_state.json"
            state_file.write_text(
                json.dumps({
                    "state": "idle",
                    "active_job": {
                        "kind": "restore",
                        "label": "恢复 1 项",
                        "message_ids": ["103"],
                        "items": [{
                            "message_id": "103",
                            "date": "2026-04-20T08:32:00+00:00",
                            "size_bytes": "10",
                            "size_mb": "0.00",
                            "target_path": str(root / "2026-04-20/missing.mp4"),
                        }],
                        "source": "restore",
                        "max_mb": 0,
                        "concurrency": 1,
                    },
                    "active_pending_ids": ["103"],
                }),
                encoding="utf-8",
            )

            controller = DownloadController(root)
            status = controller.status()

            self.assertEqual(status["state"], "paused")
            self.assertEqual(status["active_job"]["label"], "恢复 1 项")
            self.assertEqual([record["message_id"] for record in status["pending_downloads"]], ["103"])

    def test_state_reload_discards_legacy_active_job_without_message_ids(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_file = root / "download_state.json"
            state_file.write_text(
                json.dumps({
                    "state": "idle",
                    "active_job": {
                        "kind": "restore",
                        "label": "恢复 14 项",
                        "max_mb": 0,
                        "concurrency": 2,
                        "source": "restore",
                    },
                }),
                encoding="utf-8",
            )

            controller = DownloadController(root)
            status = controller.status()

            self.assertIsNone(status["active_job"])

    def test_start_months_skips_downloaded_and_skipped_file_items(self):
        class LiveThread:
            def is_alive(self):
                return True

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            controller = DownloadController(root)
            controller._download_thread = LiveThread()
            controller._state = "running"
            controller._months = {
                "2026-04": {"total": 3, "downloaded": 1, "skipped": 1, "pending": 1},
            }
            downloaded = MediaItem(
                message_id=101,
                date=datetime(2026, 4, 20, 8, 30, tzinfo=timezone.utc),
                size_bytes=1024,
                file_name="downloaded.mp4",
                extension=".mp4",
                kind="video",
            )
            skipped = MediaItem(
                message_id=102,
                date=datetime(2026, 4, 21, 8, 30, tzinfo=timezone.utc),
                size_bytes=2048,
                file_name="skipped.mp4",
                extension=".mp4",
                kind="video",
            )
            pending = MediaItem(
                message_id=103,
                date=datetime(2026, 4, 22, 8, 30, tzinfo=timezone.utc),
                size_bytes=4096,
                file_name="pending.mp4",
                extension=".mp4",
                kind="video",
            )
            target = media_target_path(downloaded, root)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(b"x" * downloaded.size_bytes)
            append_skip_record(root / "skipped_over_100mb.csv", skipped, media_target_path(skipped, root))
            controller._month_items = {
                "2026-04": [
                    controller._public_item(downloaded),
                    controller._public_item(skipped),
                    controller._public_item(pending),
                ]
            }

            queued = controller.start_months(["2026-04"], max_mb=100)
            status = controller.status()

            self.assertEqual(queued, 1)
            self.assertEqual(
                [record["message_id"] for record in status["pending_downloads"]],
                ["103"],
            )

    def test_start_months_starts_download_queue_while_scan_thread_is_alive(self):
        class LiveThread:
            def is_alive(self):
                return True

        with tempfile.TemporaryDirectory() as tmp:
            controller = DownloadController(Path(tmp))
            controller._scan_thread = LiveThread()
            controller._state = "scanning"
            controller._months = {
                "2026-04": {"total": 1, "downloaded": 0, "skipped": 0, "pending": 1},
            }
            started = []

            def start_queue():
                started.append(True)

            controller._start_queue_thread_unlocked = start_queue

            queued = controller.start_months(["2026-04"], max_mb=100)

            self.assertEqual(queued, 0)
            self.assertEqual(started, [])

    def test_queue_completion_keeps_scanning_state_when_scan_thread_is_alive(self):
        class LiveThread:
            def is_alive(self):
                return True

        with tempfile.TemporaryDirectory() as tmp:
            controller = DownloadController(Path(tmp))
            controller._scan_thread = LiveThread()
            controller._state = "running"

            import asyncio
            asyncio.run(controller._run_queue())

            self.assertEqual(controller.status()["state"], "scanning")

    def test_start_months_skips_unknown_and_duplicate_months(self):
        class LiveThread:
            def is_alive(self):
                return True

        with tempfile.TemporaryDirectory() as tmp:
            controller = DownloadController(Path(tmp))
            controller._download_thread = LiveThread()
            controller._state = "running"
            controller._months = {
                "2026-04": {"total": 1, "downloaded": 0, "skipped": 0, "pending": 1},
                "2026-03": {"total": 1, "downloaded": 0, "skipped": 0, "pending": 1},
            }

            queued = controller.start_months(["2026-04", "2026-03", "2026-01"], max_mb=100)

            self.assertEqual(queued, 0)
            self.assertEqual(controller.status()["pending_jobs"], [])

    def test_process_messages_retries_single_download_and_continues(self):
        class FlakyClient:
            def __init__(self):
                self.calls = 0
                self.disconnected = False

            async def download_media(self, msg, file, progress_callback=None):
                self.calls += 1
                if self.calls == 1:
                    raise TimeoutError("temporary")
                Path(file).write_bytes(b"ok")
                if progress_callback:
                    progress_callback(2, 2)

            async def disconnect(self):
                self.disconnected = True

        with tempfile.TemporaryDirectory() as tmp:
            controller = DownloadController(Path(tmp))
            item = MediaItem(
                message_id=9,
                date=datetime(2026, 4, 20, 1, 2, 3, tzinfo=timezone.utc),
                size_bytes=2,
                file_name="clip.mp4",
                extension=".mp4",
                kind="video",
            )
            client = FlakyClient()

            import asyncio
            asyncio.run(controller._process_messages(client, [(SimpleNamespace(id=9), item)], None))

            status = controller.status()
            self.assertEqual(client.calls, 2)
            self.assertEqual(status["state"], "idle")
            self.assertEqual(status["stats"]["downloaded"], 1)
            self.assertEqual(status["stats"]["failed"], 0)

    def test_pause_cancels_active_download_without_waiting_for_file_completion(self):
        class SlowClient:
            def __init__(self):
                self.cancelled = False
                self.disconnected = False

            async def download_media(self, msg, file, progress_callback=None):
                if progress_callback:
                    progress_callback(1, 100)
                try:
                    await asyncio.sleep(30)
                except asyncio.CancelledError:
                    self.cancelled = True
                    raise

            async def disconnect(self):
                self.disconnected = True

        async def run_check():
            with tempfile.TemporaryDirectory() as tmp:
                controller = DownloadController(Path(tmp))
                item = MediaItem(
                    message_id=91,
                    date=datetime(2026, 4, 20, 1, 2, 3, tzinfo=timezone.utc),
                    size_bytes=100,
                    file_name="slow.mp4",
                    extension=".mp4",
                    kind="video",
                )
                waiting = MediaItem(
                    message_id=92,
                    date=datetime(2026, 4, 20, 1, 3, 3, tzinfo=timezone.utc),
                    size_bytes=100,
                    file_name="waiting.mp4",
                    extension=".mp4",
                    kind="video",
                )
                controller._active_job = {
                    "kind": "restore",
                    "label": "恢复 2 项",
                    "message_ids": ["91", "92"],
                    "items": [controller._public_item(item), controller._public_item(waiting)],
                    "source": "restore",
                    "max_mb": 0,
                    "concurrency": 1,
                }
                controller._active_pending_ids = {"91", "92"}
                client = SlowClient()
                task = asyncio.create_task(controller._process_messages(client, [(SimpleNamespace(id=91), item)], None))
                await asyncio.sleep(0.05)
                controller.request_pause()
                await asyncio.wait_for(task, timeout=2)
                status = controller.status()

            self.assertTrue(client.cancelled)
            self.assertTrue(client.disconnected)
            self.assertEqual(status["state"], "paused")
            self.assertEqual(status["stats"]["downloaded"], 0)
            self.assertEqual(status["stats"]["failed"], 0)
            self.assertEqual([row["message_id"] for row in status["currents"]], [91])
            self.assertEqual([row["message_id"] for row in status["pending_downloads"]], ["92"])

        with patch("telegram_download_controller.DOWNLOAD_CONTROL_POLL_INTERVAL", 0.01):
            asyncio.run(run_check())

    def test_process_messages_skips_ignored_download(self):
        class Client:
            async def download_media(self, msg, file, progress_callback=None):
                raise AssertionError("ignored item should not download")

            async def disconnect(self):
                pass

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            controller = DownloadController(root)
            item = MediaItem(
                message_id=72,
                date=datetime(2026, 4, 20, 1, 2, 3, tzinfo=timezone.utc),
                size_bytes=2,
                file_name="clip.mp4",
                extension=".mp4",
                kind="video",
            )
            append_ignore_record(root / "ignored_downloads.csv", item, media_target_path(item, root), "deleted by user")

            asyncio.run(controller._process_messages(Client(), [(SimpleNamespace(id=72), item)], None))

            status = controller.status()
            self.assertEqual(status["stats"]["downloaded"], 0)
            self.assertEqual(status["stats"]["skipped"], 1)
            self.assertTrue(any("ignored 72" in event for event in status["events"]))

    def test_download_message_ids_marks_message_without_media_as_source_missing(self):
        class Client:
            async def get_messages(self, chat, ids):
                return [SimpleNamespace(id=75, photo=None, video=None, file=None)]

            async def disconnect(self):
                pass

        async def run_check(root):
            controller = DownloadController(root)
            item = MediaItem(
                message_id=75,
                date=datetime(2026, 4, 20, 1, 2, 3, tzinfo=timezone.utc),
                size_bytes=200 * 1024 * 1024,
                file_name="large.mp4",
                extension=".mp4",
                kind="video",
            )
            target = media_target_path(item, root)
            append_skip_record(root / "skipped_over_100mb.csv", item, target)
            with patch("telegram_download_controller.connect_client", return_value=Client()):
                await controller._download_message_ids(["75"], None)
            return controller

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            controller = asyncio.run(run_check(root))

            self.assertEqual(controller.restorable_records(), [])
            self.assertEqual(controller.status()["stats"]["source_missing"], 1)
            self.assertTrue(any("source missing 75" in event for event in controller.status()["events"]))

    def test_process_messages_downloads_with_requested_concurrency(self):
        class SlowClient:
            def __init__(self):
                self.active = 0
                self.max_active = 0

            async def download_media(self, msg, file, progress_callback=None):
                self.active += 1
                self.max_active = max(self.max_active, self.active)
                await asyncio.sleep(0.05)
                Path(file).write_bytes(b"ok")
                if progress_callback:
                    progress_callback(2, 2)
                self.active -= 1

            async def disconnect(self):
                pass

        with tempfile.TemporaryDirectory() as tmp:
            controller = DownloadController(Path(tmp))
            messages = []
            for message_id in (91, 92):
                item = MediaItem(
                    message_id=message_id,
                    date=datetime(2026, 4, 20, 1, 2, 3, tzinfo=timezone.utc),
                    size_bytes=2,
                    file_name=f"{message_id}.mp4",
                    extension=".mp4",
                    kind="video",
                )
                messages.append((SimpleNamespace(id=message_id), item))
            client = SlowClient()

            asyncio.run(controller._process_messages(client, messages, None, concurrency=2))

            self.assertEqual(client.max_active, 2)
            self.assertEqual(controller.status()["stats"]["downloaded"], 2)

    def test_process_messages_records_failure_after_retries_and_continues(self):
        class FailingClient:
            def __init__(self):
                self.calls = 0
                self.disconnected = False

            async def download_media(self, msg, file, progress_callback=None):
                self.calls += 1
                raise TimeoutError("still bad")

            async def disconnect(self):
                self.disconnected = True

        with tempfile.TemporaryDirectory() as tmp:
            controller = DownloadController(Path(tmp))
            item = MediaItem(
                message_id=10,
                date=datetime(2026, 4, 20, 1, 2, 3, tzinfo=timezone.utc),
                size_bytes=2,
                file_name="clip.mp4",
                extension=".mp4",
                kind="video",
            )
            client = FailingClient()

            import asyncio
            asyncio.run(controller._process_messages(client, [(SimpleNamespace(id=10), item)], None))

            status = controller.status()
            self.assertEqual(client.calls, 3)
            self.assertEqual(status["state"], "idle")
            self.assertEqual(status["stats"]["downloaded"], 0)
            self.assertEqual(status["stats"]["failed"], 1)

    def test_process_messages_keeps_partial_file_for_resume(self):
        class Client:
            def __init__(self):
                self.seen_existing_sizes = []

            async def download_media(self, msg, file, progress_callback=None):
                path = Path(file)
                self.seen_existing_sizes.append(path.stat().st_size if path.exists() else 0)
                path.write_bytes(b"ok")
                if progress_callback:
                    progress_callback(2, 2)

            async def disconnect(self):
                pass

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            controller = DownloadController(root)
            item = MediaItem(
                message_id=11,
                date=datetime(2026, 4, 20, 1, 2, 3, tzinfo=timezone.utc),
                size_bytes=2,
                file_name="clip.mp4",
                extension=".mp4",
                kind="video",
            )
            target = media_target_path(item, root)
            target.parent.mkdir(parents=True)
            target.write_bytes(b"x")
            client = Client()

            import asyncio
            asyncio.run(controller._process_messages(client, [(SimpleNamespace(id=11), item)], None))

            self.assertEqual(client.seen_existing_sizes, [1])

    def test_download_resumes_existing_partial_file(self):
        class Client:
            def __init__(self):
                self.offsets = []

            def _iter_download(self, msg, **kwargs):
                self.offsets.append(kwargs["offset"])

                class Chunks:
                    async def __aiter__(self):
                        yield b"tail"

                return Chunks()

        async def run_check():
            with tempfile.TemporaryDirectory() as tmp:
                controller = DownloadController(Path(tmp))
                target = Path(tmp) / "resume.mp4"
                target.write_bytes(b"head" * 1024)
                item = MediaItem(
                    message_id=91,
                    date=datetime(2026, 4, 20, 1, 2, 3, tzinfo=timezone.utc),
                    size_bytes=4100,
                    file_name="resume.mp4",
                    extension=".mp4",
                    kind="video",
                )
                client = Client()

                await controller._download_media_with_stall_timeout(
                    client,
                    SimpleNamespace(id=91, input_chat=None),
                    item,
                    target,
                    10.0,
                )

                self.assertEqual(client.offsets, [4096])
                self.assertEqual(target.read_bytes(), b"head" * 1024 + b"tail")

        asyncio.run(run_check())

    def test_process_messages_refreshes_message_when_file_reference_expires(self):
        class Client:
            def __init__(self):
                self.calls = 0
                self.refreshes = 0

            async def download_media(self, msg, file, progress_callback=None):
                self.calls += 1
                if getattr(msg, "stale", False):
                    raise FileReferenceExpiredError(request=None)
                Path(file).write_bytes(b"ok")
                if progress_callback:
                    progress_callback(2, 2)

            async def get_messages(self, entity, ids):
                self.refreshes += 1
                return SimpleNamespace(id=ids, photo=None, video=True, stale=False)

            async def disconnect(self):
                pass

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            controller = DownloadController(root)
            item = MediaItem(
                message_id=12,
                date=datetime(2026, 4, 20, 1, 2, 3, tzinfo=timezone.utc),
                size_bytes=2,
                file_name="clip.mp4",
                extension=".mp4",
                kind="video",
            )
            client = Client()

            import asyncio
            asyncio.run(controller._process_messages(client, [(SimpleNamespace(id=12, stale=True), item)], None))

            status = controller.status()
            self.assertEqual(client.calls, 2)
            self.assertEqual(client.refreshes, 1)
            self.assertEqual(status["stats"]["downloaded"], 1)
            self.assertEqual(status["stats"]["failed"], 0)


if __name__ == "__main__":
    unittest.main()
