#!/usr/bin/env python3
import json
import os
import shlex
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

from telegram_download_controller import (
    DownloadController,
    QRLoginManager,
    auto_import_session,
    clear_downloader_session,
    downloader_auth_status,
    import_session_file,
    import_string_session,
)


HOST = "127.0.0.1"
PORT = 8765
OUT_ROOT = Path("/Volumes/ZHITAI/telegram")
DEFAULT_MAX_MB = 0
DEFAULT_CONCURRENCY = 2
MAX_CONCURRENCY = 4
STATIC_ROOT = Path(__file__).with_name("static")
SETTINGS_PATH = Path(__file__).with_name("dashboard_settings.json")


def load_dashboard_settings() -> dict:
    try:
        payload = json.loads(SETTINGS_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def save_dashboard_settings(settings: dict):
    SETTINGS_PATH.write_text(json.dumps(settings, ensure_ascii=False, indent=2), encoding="utf-8")


def configured_out_root() -> Path:
    settings = load_dashboard_settings()
    raw = str(settings.get("out_root") or "").strip()
    return Path(raw).expanduser() if raw else OUT_ROOT


def _bounded_int(value, default: int, minimum: int, maximum=None) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return default
    if number < minimum:
        return minimum
    if maximum is not None and number > maximum:
        return maximum
    return number


def configured_max_mb() -> int:
    return _bounded_int(load_dashboard_settings().get("max_mb"), DEFAULT_MAX_MB, 0)


def configured_concurrency() -> int:
    return _bounded_int(load_dashboard_settings().get("concurrency"), DEFAULT_CONCURRENCY, 1, MAX_CONCURRENCY)


def dashboard_settings_payload() -> dict:
    return {
        "out_root": str(controller.out_root),
        "max_mb": configured_max_mb(),
        "concurrency": configured_concurrency(),
    }


controller = DownloadController(configured_out_root())
qr_login_manager = QRLoginManager()


def relaunch_in_terminal_if_needed() -> bool:
    if sys.stdin.isatty() or os.environ.get("TELEGRAM_DASHBOARD_IN_TERMINAL"):
        return False
    if sys.platform != "darwin":
        return False
    python = shlex.quote(sys.executable)
    script = shlex.quote(str(Path(__file__).resolve()))
    workdir = shlex.quote(str(Path(__file__).resolve().parent))
    command = f"cd {workdir} && TELEGRAM_DASHBOARD_IN_TERMINAL=1 {python} {script}"
    subprocess.Popen(
        [
            "osascript",
            "-e",
            'tell application "Terminal"',
            "-e",
            f'do script {json.dumps(command)}',
            "-e",
            "activate",
            "-e",
            "end tell",
        ]
    )
    return True


class BoundedThreadingHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 32

    def __init__(self, *args, max_workers=16, **kwargs):
        super().__init__(*args, **kwargs)
        self._request_slots = threading.BoundedSemaphore(max_workers)

    def process_request(self, request, client_address):
        if not self._request_slots.acquire(blocking=False):
            request.close()
            return
        try:
            super().process_request(request, client_address)
        except Exception:
            self._request_slots.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._request_slots.release()


class DashboardHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/":
            self._send_file(STATIC_ROOT / "dashboard.html", "text/html; charset=utf-8")
            return
        if path == "/api/status":
            status = controller.status()
            status["auth"] = {"session_exists": False}
            status["settings"] = dashboard_settings_payload()
            self._send_json(status)
            return
        if path == "/api/auth-status":
            import asyncio

            self._send_json(asyncio.run(downloader_auth_status()))
            return
        if path == "/api/qr-status":
            self._send_json(qr_login_manager.status())
            return
        self.send_error(404)

    def do_POST(self):
        path = urlparse(self.path).path
        payload = self._read_json()
        if path == "/api/start":
            started = controller.start(
                days=int(payload.get("days", 31)),
                max_mb=_bounded_int(payload.get("max_mb"), configured_max_mb(), 0),
                concurrency=_bounded_int(payload.get("concurrency"), configured_concurrency(), 1, MAX_CONCURRENCY),
            )
            self._send_json({"ok": started})
            return
        if path == "/api/auto-import-session":
            self._send_json(self._safe_login_call(auto_import_session))
            return
        if path == "/api/import-session":
            self._send_json(
                self._safe_login_call(
                    lambda: import_session_file(str(payload.get("session_path") or ""))
                )
            )
            return
        if path == "/api/import-string-session":
            self._send_json(
                self._safe_login_call(
                    lambda: import_string_session(str(payload.get("session_string") or ""))
                )
            )
            return
        if path == "/api/sign-out":
            controller.request_stop()
            qr_login_manager.reset()
            self._send_json(self._safe_login_call(clear_downloader_session))
            return
        if path == "/api/qr-start":
            self._send_json(self._safe_login_call(qr_login_manager.start))
            return
        if path == "/api/qr-password":
            self._send_json(self._safe_login_call(lambda: qr_login_manager.submit_password(str(payload.get("password") or ""))))
            return
        if path == "/api/choose-dir":
            self._send_json(self._choose_dir())
            return
        if path == "/api/settings":
            raw_out_root = str(payload.get("out_root") or "").strip()
            if not raw_out_root:
                self._send_json({"ok": False, "error": "download path required"})
                return
            target_out_root = Path(raw_out_root).expanduser()
            current_out_root = controller.out_root
            result = {"ok": True, "out_root": str(current_out_root)}
            if target_out_root != current_out_root:
                result = controller.set_out_root(target_out_root)
                if not result.get("ok"):
                    self._send_json(result)
                    return

            max_mb = _bounded_int(payload.get("max_mb"), DEFAULT_MAX_MB, 0)
            concurrency = _bounded_int(payload.get("concurrency"), DEFAULT_CONCURRENCY, 1, MAX_CONCURRENCY)
            settings = load_dashboard_settings()
            settings.update(
                {
                    "out_root": result["out_root"],
                    "max_mb": max_mb,
                    "concurrency": concurrency,
                }
            )
            try:
                save_dashboard_settings(settings)
            except OSError as exc:
                self._send_json({"ok": False, "error": f"设置保存失败：{exc}"})
                return
            self._send_json({"ok": True, "out_root": result["out_root"], "settings": dashboard_settings_payload()})
            return
        if path == "/api/scan-months":
            started = controller.scan_months()
            self._send_json({"ok": started})
            return
        if path == "/api/start-months":
            months = payload.get("months") or []
            result = controller.start_months_result(
                months,
                max_mb=_bounded_int(payload.get("max_mb"), configured_max_mb(), 0),
                concurrency=_bounded_int(payload.get("concurrency"), configured_concurrency(), 1, MAX_CONCURRENCY),
            )
            self._send_json(result)
            return
        if path == "/api/pause":
            controller.request_pause()
            self._send_json({"ok": True})
            return
        if path == "/api/resume":
            self._send_json({"ok": controller.request_resume()})
            return
        if path == "/api/stop":
            controller.request_stop()
            self._send_json({"ok": True})
            return
        if path == "/api/resume-skipped":
            ids = payload.get("message_ids") or []
            max_mb = payload.get("max_mb")
            started = controller.resume_skipped(
                ids,
                configured_max_mb() if max_mb in (None, "") else _bounded_int(max_mb, configured_max_mb(), 0),
                concurrency=_bounded_int(payload.get("concurrency"), configured_concurrency(), 1, MAX_CONCURRENCY),
            )
            self._send_json({"ok": started})
            return
        if path == "/api/clear-pending-downloads":
            self._send_json(controller.clear_pending_downloads())
            return
        self.send_error(404)

    def log_message(self, fmt, *args):
        return

    def _read_json(self):
        length = int(self.headers.get("content-length", "0") or "0")
        if not length:
            return {}
        raw = self.rfile.read(length)
        return json.loads(raw.decode("utf-8"))

    def _send_json(self, payload):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("content-type", "application/json; charset=utf-8")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _send_file(self, path: Path, content_type: str):
        body = path.read_bytes()
        self.send_response(200)
        self.send_header("content-type", content_type)
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _choose_dir(self):
        script = (
            'set selectedFolder to choose folder with prompt "选择 Telegram 下载文件夹" '
            'default location POSIX file "/Volumes"\n'
            "POSIX path of selectedFolder"
        )
        try:
            result = subprocess.run(
                ["osascript", "-e", script],
                check=False,
                capture_output=True,
                text=True,
                timeout=300,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            return {"ok": False, "error": f"打开文件夹选择器失败：{exc}"}
        if result.returncode != 0:
            error = (result.stderr or "").strip()
            if "User canceled" in error or result.returncode == 1:
                return {"ok": False, "cancelled": True}
            return {"ok": False, "error": error or "文件夹选择失败"}
        path = result.stdout.strip().rstrip("/")
        return {"ok": True, "path": path}

    def _safe_login_call(self, fn):
        try:
            return fn()
        except Exception as exc:
            return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def main():
    if relaunch_in_terminal_if_needed():
        return
    url = f"http://{HOST}:{PORT}"
    server = BoundedThreadingHTTPServer((HOST, PORT), DashboardHandler)
    print("", flush=True)
    print("Telegram File Download 已启动", flush=True)
    print(f"登录网址：{url}", flush=True)
    print("关闭这个控制台窗口，或按 Ctrl+C，即可结束程序。", flush=True)
    print("", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n正在关闭 Telegram File Download...", flush=True)
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
