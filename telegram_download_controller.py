import asyncio
import base64
import hashlib
import io
import json
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from telethon import TelegramClient
from telethon.client.downloads import MAX_CHUNK_SIZE, MIN_CHUNK_SIZE, AES, _CdnRedirect
from telethon.errors import AuthKeyDuplicatedError, FileReferenceExpiredError, SessionPasswordNeededError
from telethon.sessions import StringSession

from telegram_media_core import (
    MediaItem,
    ProgressState,
    append_source_missing_record,
    append_skip_record,
    media_target_path,
    month_key,
    read_ignore_records,
    read_skip_records,
    read_source_missing_records,
    summarize_months,
)


API_ID = 2496
API_HASH = "8da85b0d5bfe62527e5b244c209159c3"
MAX_DEFAULT_BYTES = 100 * 1024 * 1024
DOWNLOAD_RETRIES = 3
DOWNLOAD_STALL_TIMEOUT = 90
DOWNLOAD_CONTROL_POLL_INTERVAL = 1
DEFAULT_DOWNLOAD_CONCURRENCY = 2
MAX_DOWNLOAD_CONCURRENCY = 4
TELEGRAM_CLIENT_LOCK = threading.Lock()
DOWNLOADER_SESSION_PATH = Path(__file__).with_name("telegram_downloader.session")
DOWNLOADER_STRING_SESSION_PATH = Path(__file__).with_name("telegram_downloader.string_session")
REVOKED_SESSION_FINGERPRINTS_PATH = Path(__file__).with_name("revoked_sessions.json")
SESSION_SIGN_OUT_PATH = Path(__file__).with_name("session_sign_out.json")
QR_LOGIN_TIMEOUT = 120
AUTO_SESSION_ROOTS = [
    Path(__file__).parent,
    Path.home() / "Downloads",
    Path.home() / "Desktop",
    Path.home() / "Documents",
]
DC_ADDR = {
    1: "149.154.175.53",
    2: "149.154.167.51",
    3: "149.154.175.100",
    4: "149.154.167.91",
    5: "91.108.56.130",
}


def normalize_concurrency(value) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = DEFAULT_DOWNLOAD_CONCURRENCY
    return max(1, min(MAX_DOWNLOAD_CONCURRENCY, parsed))


class DownloadInterrupted(Exception):
    pass


class DownloadController:
    def __init__(self, out_root: Path):
        self._lock = threading.Lock()
        self._scan_thread: Optional[threading.Thread] = None
        self._download_thread: Optional[threading.Thread] = None
        self._state = "idle"
        self._pause_requested = False
        self._stop_requested = False
        self._stats = ProgressState()
        self._current: Optional[Dict] = None
        self._currents: Dict[int, Dict] = {}
        self._events: List[str] = []
        self._started_at: Optional[float] = None
        self._months: Dict[str, Dict[str, int]] = {}
        self._month_items: Dict[str, List[Dict[str, str]]] = {}
        self._selected_month: Optional[str] = None
        self._scan_progress: Dict[str, int] = {"messages": 0, "media": 0, "months": 0}
        self._last_progress_write = 0.0
        self._queue: List[Dict] = []
        self._active_job: Optional[Dict] = None
        self._active_pending_ids: set = set()
        self._restorable_cache_key = ("uncached",)
        self._restorable_cache_records: List[Dict[str, str]] = []
        self._set_paths(out_root)
        self._load_state()

    def _set_paths(self, out_root: Path):
        self.out_root = Path(out_root).expanduser()
        self.skip_log = self.out_root / "skipped_over_100mb.csv"
        self.failed_log = self.out_root / "failed_downloads.csv"
        self.ignore_log = self.out_root / "ignored_downloads.csv"
        self.source_missing_log = self.out_root / "source_missing_downloads.csv"
        self.state_file = self.out_root / "download_state.json"

    def set_out_root(self, out_root: Path) -> Dict:
        with self._lock:
            if self._scan_thread_alive_unlocked() or self._download_thread_alive_unlocked() or self._active_job:
                return {"ok": False, "error": "请先暂停或停止当前任务，再修改下载路径。"}
            target = Path(out_root).expanduser()
            try:
                target.mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                return {"ok": False, "error": f"无法创建下载路径：{exc}"}
            self._set_paths(target)
            self._stats = ProgressState()
            self._current = None
            self._currents = {}
            self._months = {}
            self._month_items = {}
            self._selected_month = None
            self._scan_progress = {"messages": 0, "media": 0, "months": 0}
            self._queue = []
            self._active_pending_ids = set()
            self.invalidate_restorable_cache()
            self._load_state()
            self._state = "idle"
            self._event(f"download path set {self.out_root}")
            return {"ok": True, "out_root": str(self.out_root)}

    def _load_state(self):
        try:
            payload = json.loads(self.state_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return
        state = payload.get("state")
        if state in ("idle", "paused", "stopped", "error"):
            self._state = state
        self._pause_requested = bool(payload.get("pause_requested")) if self._state == "paused" else False
        self._stop_requested = bool(payload.get("stop_requested")) if self._state == "stopped" else False
        months = payload.get("months")
        if isinstance(months, dict):
            self._months = {
                str(month): {
                    "total": int(row.get("total", 0)),
                    "downloaded": int(row.get("downloaded", 0)),
                    "skipped": int(row.get("skipped", 0)),
                    "pending": int(row.get("pending", 0)),
                }
                for month, row in months.items()
                if isinstance(row, dict)
            }
        scan_progress = payload.get("scan_progress")
        if isinstance(scan_progress, dict):
            self._scan_progress = {
                "messages": int(scan_progress.get("messages", 0)),
                "media": int(scan_progress.get("media", 0)),
                "months": int(scan_progress.get("months", len(self._months))),
            }
        selected_month = payload.get("selected_month")
        if isinstance(selected_month, str) and selected_month in self._months:
            self._selected_month = selected_month
        elif self._months:
            self._selected_month = sorted(self._months.keys(), reverse=True)[0]
        month_items = payload.get("month_items")
        if isinstance(month_items, dict):
            self._month_items = {
                str(month): [
                    dict(record)
                    for record in records
                    if isinstance(record, dict) and record.get("message_id")
                ]
                for month, records in month_items.items()
                if isinstance(records, list)
            }
        pending_jobs = payload.get("pending_jobs")
        if isinstance(pending_jobs, list):
            self._queue = [
                dict(job)
                for job in pending_jobs
                if isinstance(job, dict) and self._valid_persisted_job(job)
            ]
        active_job = payload.get("active_job")
        if isinstance(active_job, dict) and self._valid_persisted_job(active_job):
            self._active_job = dict(active_job)
            if self._state == "idle":
                self._state = "paused"
        active_pending_ids = payload.get("active_pending_ids")
        if isinstance(active_pending_ids, list):
            self._active_pending_ids = {str(message_id) for message_id in active_pending_ids}
        elif self._active_job and self._active_job.get("kind") == "restore":
            self._active_pending_ids = {
                str(message_id)
                for message_id in self._active_job.get("message_ids", [])
            }

    def _valid_persisted_job(self, job: Dict) -> bool:
        if not job.get("kind") or not job.get("label"):
            return False
        if job.get("kind") == "restore":
            return isinstance(job.get("message_ids"), list)
        if job.get("kind") == "recent":
            return "days" in job
        return False

    def status(self) -> Dict:
        with self._lock:
            self._ensure_paused_currents_unlocked()
            currents = list(self._currents.values())
            current = self._current
        restorable_records = self.restorable_records()
        source_missing_records = self.source_missing_records()
        pending_downloads = self.pending_download_records(restorable_records)
        restore_job_ids = self.restore_job_message_ids()
        visible_restorable_records = [
            record for record in restorable_records
            if str(record.get("message_id")) not in restore_job_ids
        ]
        failed_records = self.failed_records(visible_restorable_records)
        with self._lock:
            data = {
                "state": self._state,
                "pause_requested": self._pause_requested,
                "stop_requested": self._stop_requested,
                "out_root": str(self.out_root),
                "stats": {
                    "total": self._stats.total,
                    "downloaded": self._stats.downloaded,
                    "skipped": self._stats.skipped,
                    "pending": self._stats.pending,
                    "handled": self._stats.handled,
                    "restorable": len(visible_restorable_records),
                    "failed": self._stats.failed,
                    "source_missing": len(source_missing_records),
                },
                "current": current,
                "currents": currents,
                "events": list(self._events[-80:]),
                "skipped": visible_restorable_records,
                "failed_downloads": failed_records,
                "source_missing": source_missing_records,
                "months": self._months,
                "selected_month": self._selected_month,
                "scan_progress": self._scan_progress,
                "pending_jobs": [self._public_job(job) for job in self._queue],
                "pending_downloads": pending_downloads,
                "active_job": self._public_job(self._active_job) if self._active_job else None,
            }
        return data

    def request_pause(self):
        with self._lock:
            self._pause_requested = True
            if self._state in ("running", "scanning_running"):
                self._state = "pausing"
            self._event("pause requested")

    def request_resume(self):
        with self._lock:
            if not self._active_job or self._state in ("running", "scanning_running", "pausing", "stopping"):
                return False
            self._ensure_paused_currents_unlocked()
            self._active_pending_ids = self._job_pending_ids(self._active_job)
            self._pause_requested = False
            self._stop_requested = False
            self._state = self._activity_state_unlocked(downloading=True)
            self._event("resume requested")
            if not self._download_thread_alive_unlocked():
                self._start_queue_thread_unlocked()
            return True

    def request_stop(self):
        with self._lock:
            self._stop_requested = True
            self._state = "stopping"
            self._event("stop requested")

    def start(self, days: int = 31, max_mb: int = 0, concurrency: int = DEFAULT_DOWNLOAD_CONCURRENCY):
        with self._lock:
            return self._enqueue_download_job(
                {
                    "kind": "recent",
                    "label": f"最近 {days} 天",
                    "days": days,
                    "max_mb": max_mb,
                    "concurrency": normalize_concurrency(concurrency),
                }
            )

    def scan_months(self):
        with self._lock:
            if self._scan_thread and self._scan_thread.is_alive():
                return False
            self._stop_requested = False
            self._scan_progress = {"messages": 0, "media": 0, "months": 0}
            self._state = self._activity_state_unlocked(scanning=True)
            self._event("scan months")
            self._scan_thread = threading.Thread(
                target=lambda: asyncio.run(self._scan_months()),
                daemon=True,
            )
            self._scan_thread.start()
            return True

    def start_month(self, month: str, max_mb: int = 0, concurrency: int = DEFAULT_DOWNLOAD_CONCURRENCY):
        return self.start_months([month], max_mb=max_mb, concurrency=concurrency) > 0

    def start_months(
        self,
        months: Iterable[str],
        max_mb: int = 0,
        concurrency: int = DEFAULT_DOWNLOAD_CONCURRENCY,
    ) -> int:
        with self._lock:
            selected = []
            seen = set()
            for month in months:
                month = str(month)
                if month in seen or month not in self._months:
                    continue
                selected.append(month)
                seen.add(month)

            queued = self._enqueue_month_items_unlocked(selected, max_mb, normalize_concurrency(concurrency))
            self._event(f"queued selected months count={queued}")
            if queued and not self._download_thread_alive_unlocked():
                self._start_queue_thread_unlocked()
            return queued

    def start_months_result(
        self,
        months: Iterable[str],
        max_mb: int = 0,
        concurrency: int = DEFAULT_DOWNLOAD_CONCURRENCY,
    ) -> Dict:
        requested = [str(month) for month in months]
        queued = self.start_months(requested, max_mb=max_mb, concurrency=concurrency)
        if queued:
            return {"ok": True, "queued": queued}
        with self._lock:
            known = [month for month in requested if month in self._months]
            missing_index = bool(known) and not any(self._month_items.get(month) for month in known)
        if missing_index:
            return {
                "ok": False,
                "queued": 0,
                "error": "需要重新扫描月份后，才能添加到下载。",
            }
        return {
            "ok": False,
            "queued": 0,
            "error": "没有可添加到下载的文件。",
        }

    def resume_skipped(
        self,
        message_ids: Optional[Iterable[str]] = None,
        max_mb: Optional[int] = None,
        concurrency: int = DEFAULT_DOWNLOAD_CONCURRENCY,
    ):
        with self._lock:
            restorable_by_id = {
                str(record.get("message_id")): record
                for record in self.restorable_records()
                if record.get("message_id")
            }
            requested_ids = [str(message_id) for message_id in (message_ids or restorable_by_id.keys())]
            queued_ids = {
                str(message_id)
                for job in self._queue
                if job.get("kind") == "restore"
                for message_id in job.get("message_ids", [])
            }
            if self._active_job and self._active_job.get("kind") == "restore":
                queued_ids.update(str(message_id) for message_id in self._active_job.get("message_ids", []))
            ids = []
            records = []
            for message_id in requested_ids:
                if message_id in queued_ids or message_id not in restorable_by_id:
                    continue
                ids.append(message_id)
                records.append(dict(restorable_by_id[message_id]))
                queued_ids.add(message_id)
            if not ids:
                return False
            return self._enqueue_download_job(
                {
                    "kind": "restore",
                    "label": f"恢复 {len(ids) or '全部'} 项",
                    "message_ids": ids,
                    "items": records,
                    "source": "restore",
                    "max_mb": max_mb,
                    "concurrency": normalize_concurrency(concurrency),
                }
            )

    def failed_records(self, restorable_records: Optional[List[Dict[str, str]]] = None) -> List[Dict[str, str]]:
        failed_ids = {record.get("message_id") for record in read_skip_records(self.failed_log)}
        records = self.restorable_records() if restorable_records is None else restorable_records
        return [
            record for record in records
            if record.get("message_id") in failed_ids
        ]

    def pending_download_records(self, restorable_records: Optional[List[Dict[str, str]]] = None) -> List[Dict[str, str]]:
        restorable_by_id = {
            str(record.get("message_id")): record
            for record in (self.restorable_records() if restorable_records is None else restorable_records)
        }
        active_ids = set()
        with self._lock:
            for item in self._currents.values():
                if item.get("message_id"):
                    active_ids.add(str(item["message_id"]))
            if self._current and self._current.get("message_id"):
                active_ids.add(str(self._current["message_id"]))
            jobs = list(self._queue)
            active_job = dict(self._active_job) if self._active_job else None
            active_pending_ids = set(self._active_pending_ids)
            state = self._state
        records = []
        seen = set()
        all_jobs = []
        if active_job:
            all_jobs.append(active_job)
        all_jobs.extend(jobs)
        for job in all_jobs:
            if job.get("kind") == "restore":
                for message_id in job.get("message_ids", []):
                    message_id = str(message_id)
                    if message_id in seen or message_id in active_ids:
                        continue
                    job_items = {
                        str(record.get("message_id")): record
                        for record in job.get("items", [])
                        if record.get("message_id")
                    }
                    record = dict(job_items.get(message_id) or restorable_by_id.get(message_id) or {"message_id": message_id})
                    if job is active_job and message_id not in active_pending_ids:
                        continue
                    seen.add(message_id)
                    records.append(record)
        return records

    def clear_pending_downloads(self) -> Dict:
        with self._lock:
            queued_count = 0
            for job in self._queue:
                if job.get("kind") == "restore":
                    queued_count += len(job.get("message_ids", []))
                else:
                    queued_count += 1
            self._queue = []

            active_cleared = 0
            if self._active_job and self._active_job.get("kind") == "restore":
                current_ids = {
                    str(item.get("message_id"))
                    for item in self._currents.values()
                    if item.get("message_id")
                }
                if self._current and self._current.get("message_id"):
                    current_ids.add(str(self._current["message_id"]))

                cleared_ids = {
                    str(message_id)
                    for message_id in self._active_pending_ids
                    if str(message_id) not in current_ids
                }
                if cleared_ids:
                    active_cleared = len(cleared_ids)
                    self._active_pending_ids.difference_update(cleared_ids)
                    self._active_job["message_ids"] = [
                        str(message_id)
                        for message_id in self._active_job.get("message_ids", [])
                        if str(message_id) not in cleared_ids
                    ]
                    self._active_job["items"] = [
                        record
                        for record in self._active_job.get("items", [])
                        if str(record.get("message_id")) not in cleared_ids
                    ]

            cleared = queued_count + active_cleared
            if cleared:
                self._event(f"cleared pending downloads count={cleared}")
            else:
                self._write_state_unlocked()
            return {"ok": True, "cleared": cleared}

    def _paused_current_records_unlocked(self) -> List[Dict]:
        if not self._active_job or self._active_job.get("kind") != "restore":
            return []
        job_items = {
            str(record.get("message_id")): record
            for record in self._active_job.get("items", [])
            if record.get("message_id")
        }
        records = []
        for message_id in self._active_job.get("message_ids", []):
            message_id = str(message_id)
            if message_id in self._active_pending_ids:
                continue
            record = dict(job_items.get(message_id) or {"message_id": message_id})
            if not self._restore_record_needs_download(record):
                continue
            target = self._skip_record_target(record) if record.get("target_path") else None
            try:
                total = int(record.get("size_bytes") or 0)
            except (TypeError, ValueError):
                total = 0
            downloaded = target.stat().st_size if target and target.exists() else 0
            percent = round(min(100.0, downloaded / total * 100), 2) if total else 0.0
            records.append(
                {
                    "message_id": int(message_id) if message_id.isdigit() else message_id,
                    "target": str(target or record.get("target_path") or ""),
                    "size_bytes": total,
                    "downloaded_bytes": downloaded,
                    "total_bytes": total,
                    "percent": percent,
                    "speed_bytes_per_sec": 0,
                    "elapsed_seconds": 0,
                }
            )
        return records

    def _ensure_paused_currents_unlocked(self):
        if self._state != "paused" or not self._active_job or self._currents:
            return
        records = self._paused_current_records_unlocked()
        if not self._active_pending_ids and len(records) != 1:
            return
        self._currents = {record["message_id"]: record for record in records}
        self._current = records[0] if records else None

    def _restore_record_needs_download(self, record: Dict[str, str]) -> bool:
        message_id = str(record.get("message_id") or "")
        if message_id in self.ignored_message_ids() or message_id in self.source_missing_message_ids():
            return False
        target = self._skip_record_target(record) if record.get("target_path") else None
        if not target or not target.exists():
            return True
        try:
            expected_size = int(record.get("size_bytes") or 0)
        except (TypeError, ValueError):
            expected_size = 0
        return bool(expected_size and target.stat().st_size < expected_size)

    def _job_pending_ids(self, job: Dict) -> set:
        if job.get("kind") != "restore":
            return set()
        job_items = {
            str(record.get("message_id")): record
            for record in job.get("items", [])
            if record.get("message_id")
        }
        pending_ids = set()
        for message_id in job.get("message_ids", []):
            message_id = str(message_id)
            record = dict(job_items.get(message_id) or {"message_id": message_id})
            if self._restore_record_needs_download(record):
                pending_ids.add(message_id)
        return pending_ids

    def restore_job_message_ids(self) -> set:
        with self._lock:
            jobs = list(self._queue)
            active_job = dict(self._active_job) if self._active_job else None
        ids = set()
        all_jobs = []
        if active_job:
            all_jobs.append(active_job)
        all_jobs.extend(jobs)
        for job in all_jobs:
            if job.get("kind") == "restore":
                ids.update(str(message_id) for message_id in job.get("message_ids", []))
        return ids

    def restorable_records(self) -> List[Dict[str, str]]:
        cache_key = self._restorable_cache_key_for_skip_log()
        if self._restorable_cache_key == cache_key:
            return [dict(record) for record in self._restorable_cache_records]
        restorable = self._compute_restorable_records()
        self._restorable_cache_key = cache_key
        self._restorable_cache_records = [dict(record) for record in restorable]
        return restorable

    def invalidate_restorable_cache(self):
        self._restorable_cache_key = ("uncached",)
        self._restorable_cache_records = []

    def _restorable_cache_key_for_skip_log(self):
        parts = []
        try:
            stat = self.skip_log.stat()
        except OSError:
            parts.append(None)
        else:
            parts.append((stat.st_mtime_ns, stat.st_size))
        try:
            stat = self.ignore_log.stat()
        except OSError:
            parts.append(None)
        else:
            parts.append((stat.st_mtime_ns, stat.st_size))
        try:
            stat = self.failed_log.stat()
        except OSError:
            parts.append(None)
        else:
            parts.append((stat.st_mtime_ns, stat.st_size))
        try:
            stat = self.source_missing_log.stat()
        except OSError:
            parts.append(None)
        else:
            parts.append((stat.st_mtime_ns, stat.st_size))
        return tuple(parts)

    def _compute_restorable_records(self) -> List[Dict[str, str]]:
        records_by_id = {}
        for record in read_skip_records(self.skip_log) + read_skip_records(self.failed_log):
            message_id = record.get("message_id")
            if message_id:
                records_by_id[message_id] = record
        ignored_ids = self.ignored_message_ids()
        source_missing_ids = self.source_missing_message_ids()
        restorable = []
        for record in records_by_id.values():
            if record.get("message_id") in ignored_ids:
                continue
            if record.get("message_id") in source_missing_ids:
                continue
            target = self._skip_record_target(record)
            expected_size = int(record.get("size_bytes") or 0)
            if not target.exists():
                restorable.append(record)
                continue
            if expected_size and target.stat().st_size < expected_size:
                restorable.append(record)
        return restorable

    def _skip_record_target(self, record: Dict[str, str]) -> Path:
        raw_target = record.get("target_path", "")
        normalized = raw_target.replace("\\", "/")
        marker = "telegram/"
        if marker in normalized:
            return self.out_root / normalized.split(marker, 1)[1]
        return Path(raw_target)

    def ignored_message_ids(self) -> set:
        return {record.get("message_id") for record in read_ignore_records(self.ignore_log)}

    def source_missing_records(self) -> List[Dict[str, str]]:
        return read_source_missing_records(self.source_missing_log)

    def source_missing_message_ids(self) -> set:
        return {record.get("message_id") for record in self.source_missing_records()}

    def _public_job(self, job: Optional[Dict]) -> Dict:
        if not job:
            return {}
        public = {"kind": job["kind"], "label": job["label"]}
        for key in ("month", "days", "max_mb", "concurrency"):
            if key in job:
                public[key] = job[key]
        if "message_ids" in job:
            public["count"] = len(job["message_ids"])
        if "source" in job:
            public["source"] = job["source"]
        return public

    def _persisted_job(self, job: Optional[Dict]) -> Dict:
        if not job:
            return {}
        persisted = {"kind": job["kind"], "label": job["label"]}
        for key in ("month", "days", "max_mb", "concurrency", "message_ids", "items", "source"):
            if key in job:
                persisted[key] = job[key]
        return persisted

    def _public_item(self, item: MediaItem) -> Dict[str, str]:
        target = media_target_path(item, self.out_root)
        return {
            "message_id": str(item.message_id),
            "date": item.date.isoformat(),
            "size_bytes": str(item.size_bytes),
            "size_mb": f"{item.size_bytes / 1024 / 1024:.2f}",
            "target_path": str(target),
            "kind": item.kind,
        }

    def _record_target(self, record: Dict[str, str]) -> Path:
        raw_target = record.get("target_path", "")
        if raw_target:
            return self._skip_record_target(record)
        return self.out_root / str(record.get("message_id", ""))

    def _record_expected_size(self, record: Dict[str, str]) -> int:
        try:
            return int(record.get("size_bytes") or 0)
        except (TypeError, ValueError):
            return 0

    def _record_needs_month_download(self, record: Dict[str, str], skipped_ids: set, ignored_ids: set, source_missing_ids: set) -> bool:
        message_id = str(record.get("message_id") or "")
        if not message_id:
            return False
        if message_id in skipped_ids or message_id in ignored_ids or message_id in source_missing_ids:
            return False
        target = self._record_target(record)
        expected_size = self._record_expected_size(record)
        if target.exists() and (not expected_size or target.stat().st_size >= expected_size):
            return False
        return True

    def _items_by_month(self, items: List[MediaItem]) -> Dict[str, List[Dict[str, str]]]:
        grouped: Dict[str, List[Dict[str, str]]] = {}
        for item in items:
            grouped.setdefault(month_key(item.date), []).append(self._public_item(item))
        return {
            month: records
            for month, records in sorted(grouped.items(), reverse=True)
        }

    def _enqueue_download_job(self, job: Dict) -> bool:
        self._queue.append(job)
        self._event(f"queued {job['label']}")
        if not self._download_thread_alive_unlocked():
            self._start_queue_thread_unlocked()
        return True

    def _scan_thread_alive_unlocked(self) -> bool:
        return bool(self._scan_thread and self._scan_thread.is_alive())

    def _download_thread_alive_unlocked(self) -> bool:
        return bool(self._download_thread and self._download_thread.is_alive())

    def _activity_state_unlocked(self, scanning: Optional[bool] = None, downloading: Optional[bool] = None) -> str:
        scan_active = self._scan_thread_alive_unlocked() if scanning is None else scanning
        download_active = (
            self._download_thread_alive_unlocked() or bool(self._active_job)
            if downloading is None
            else downloading
        )
        if scan_active and download_active:
            return "scanning_running"
        if scan_active:
            return "scanning"
        if download_active:
            return "running"
        return "idle"

    def _enqueue_month_items_unlocked(self, months: Iterable[str], max_mb: int, concurrency: int) -> int:
        queued_ids = {
            str(message_id)
            for job in self._queue
            if job.get("kind") == "restore"
            for message_id in job.get("message_ids", [])
        }
        if self._active_job and self._active_job.get("kind") == "restore":
            queued_ids.update(str(message_id) for message_id in self._active_job.get("message_ids", []))

        selected_ids = []
        selected_records = []
        skipped_ids = {record.get("message_id") for record in read_skip_records(self.skip_log)}
        skipped_ids.update(record.get("message_id") for record in read_skip_records(self.failed_log))
        ignored_ids = self.ignored_message_ids()
        source_missing_ids = self.source_missing_message_ids()
        for month in months:
            records = self._month_items.get(str(month), [])
            if not records:
                continue
            for record in records:
                message_id = str(record.get("message_id"))
                if not self._record_needs_month_download(record, skipped_ids, ignored_ids, source_missing_ids):
                    continue
                if not message_id or message_id in queued_ids:
                    continue
                selected_ids.append(message_id)
                selected_records.append(dict(record))
                queued_ids.add(message_id)

        if selected_ids:
            self._queue.append(
                {
                    "kind": "restore",
                    "label": f"下载 {len(selected_ids)} 项",
                    "message_ids": selected_ids,
                    "items": selected_records,
                    "source": "months",
                    "max_mb": max_mb,
                    "concurrency": concurrency,
                }
            )
        return len(selected_ids)

    def _start_queue_thread_unlocked(self):
        self._pause_requested = False
        self._stop_requested = False
        self._download_thread = threading.Thread(
            target=lambda: asyncio.run(self._run_queue()),
            daemon=True,
        )
        self._download_thread.start()

    def _set_current_progress(
        self,
        item: MediaItem,
        target: Path,
        downloaded_bytes: int,
        total_bytes: int,
        started_at: float,
        now: Optional[float] = None,
    ):
        now = time.time() if now is None else now
        elapsed = max(0.001, now - started_at)
        total = int(total_bytes or item.size_bytes or 0)
        downloaded = int(downloaded_bytes or 0)
        percent = round(min(100.0, downloaded / total * 100), 2) if total else 0.0
        speed = round(downloaded / elapsed, 2)
        with self._lock:
            progress = {
                "message_id": item.message_id,
                "target": str(target),
                "size_bytes": item.size_bytes,
                "downloaded_bytes": downloaded,
                "total_bytes": total,
                "percent": percent,
                "speed_bytes_per_sec": speed,
                "elapsed_seconds": round(elapsed, 1),
            }
            self._currents[item.message_id] = progress
            self._current = next(iter(self._currents.values()), None)
            if now - self._last_progress_write >= 0.5 or (total and downloaded >= total):
                self._last_progress_write = now
                self._write_state_unlocked()

    def _clear_current_progress_unlocked(self, item: MediaItem):
        self._currents.pop(item.message_id, None)
        self._current = next(iter(self._currents.values()), None)

    def _event(self, message: str):
        self._events.append(f"{datetime.now().strftime('%H:%M:%S')} {message}")
        self._write_state_unlocked()

    def _write_state_unlocked(self):
        try:
            self.out_root.mkdir(parents=True, exist_ok=True)
            payload = {
                "state": self._state,
                "pause_requested": self._pause_requested,
                "stop_requested": self._stop_requested,
                "out_root": str(self.out_root),
                "stats": {
                    "total": self._stats.total,
                    "downloaded": self._stats.downloaded,
                    "skipped": self._stats.skipped,
                    "pending": self._stats.pending,
                    "handled": self._stats.handled,
                    "failed": self._stats.failed,
                    "source_missing": self._stats.source_missing,
                },
                "current": self._current,
                "currents": list(self._currents.values()),
                "events": self._events[-80:],
                "months": self._months,
                "month_items": self._month_items,
                "selected_month": self._selected_month,
                "scan_progress": self._scan_progress,
                "pending_jobs": [self._persisted_job(job) for job in self._queue],
                "active_job": self._persisted_job(self._active_job) if self._active_job else None,
                "active_pending_ids": sorted(self._active_pending_ids),
            }
            self.state_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        except OSError:
            pass

    async def _run_queue(self):
        while True:
            with self._lock:
                if self._stop_requested:
                    self._active_job = None
                    self._active_pending_ids = set()
                    self._current = None
                    self._currents = {}
                    self._state = "stopped"
                    self._event("stopped")
                    return
                if self._pause_requested:
                    self._state = "paused"
                    self._event("paused")
                    return
                if self._active_job:
                    active_job = self._active_job
                    if active_job.get("kind") == "restore":
                        if not self._active_pending_ids:
                            self._active_pending_ids = self._job_pending_ids(active_job)
                        job = dict(active_job)
                        job["message_ids"] = [
                            str(message_id)
                            for message_id in active_job.get("message_ids", [])
                            if str(message_id) in self._active_pending_ids
                        ]
                    else:
                        job = active_job
                    self._state = self._activity_state_unlocked(downloading=True)
                    self._started_at = time.time()
                    self._event(f"resume active {job['label']}")
                elif not self._queue:
                    self._active_job = None
                    self._active_pending_ids = set()
                    self._current = None
                    self._currents = {}
                    if self._state not in ("error", "paused", "stopped"):
                        self._state = self._activity_state_unlocked(downloading=False)
                        self._event("pending complete")
                    return

                else:
                    job = self._queue.pop(0)
                    self._active_job = job
                    self._active_pending_ids = set(str(message_id) for message_id in job.get("message_ids", []))
                    self._state = self._activity_state_unlocked(downloading=True)
                    self._stats = ProgressState()
                    self._current = None
                    self._currents = {}
                    self._started_at = time.time()
                    if job["kind"] == "recent":
                        self._event(f"start days={job['days']} max_mb={job['max_mb']}")
                    elif job["kind"] == "restore":
                        self._event(f"resume skipped count={len(job['message_ids']) or 'all'}")

            await self._run_download_job(job)

            with self._lock:
                if self._state in ("paused", "pausing"):
                    self._write_state_unlocked()
                    return
                if self._state in ("stopped", "stopping", "error"):
                    self._active_job = None
                    self._active_pending_ids = set()
                    self._write_state_unlocked()
                    return
                self._active_job = None
                self._active_pending_ids = set()

    async def _run_download_job(self, job: Dict):
        if job["kind"] == "recent":
            await self._run_recent(job["days"], job["max_mb"], job.get("concurrency", 1))
            return
        if job["kind"] == "restore":
            await self._run_restore(job["message_ids"], job["max_mb"], job.get("concurrency", 1))
            return

    async def _run_recent(self, days: int, max_mb: int, concurrency: int):
        limit = None if not max_mb else max_mb * 1024 * 1024
        await self._download_recent(days, limit, concurrency=concurrency)

    async def _run_restore(self, message_ids: List[str], max_mb: Optional[int], concurrency: int):
        limit = None if not max_mb else max_mb * 1024 * 1024
        await self._download_message_ids(message_ids, limit, concurrency)

    async def _scan_months(self):
        try:
            client = await connect_client()
            items = []
            seen_months = set()
            scanned = 0
            async for msg in client.iter_messages("me"):
                with self._lock:
                    if self._stop_requested:
                        self._state = "stopped"
                        self._event("month scan stopped")
                        await client.disconnect()
                        return
                scanned += 1
                if msg.photo or msg.video:
                    item = item_from_message(msg)
                    items.append(item)
                    seen_months.add(month_key(item.date))
                if scanned % 100 == 0:
                    with self._lock:
                        self._state = self._activity_state_unlocked(scanning=True)
                        self._scan_progress = {
                            "messages": scanned,
                            "media": len(items),
                            "months": len(seen_months),
                        }
                        self._months = summarize_months(items, self.out_root, self.skip_log)
                        self._month_items = self._items_by_month(items)
                        self._write_state_unlocked()
            await client.disconnect()
            with self._lock:
                self._months = summarize_months(items, self.out_root, self.skip_log)
                self._month_items = self._items_by_month(items)
                self._scan_progress = {
                    "messages": scanned,
                    "media": len(items),
                    "months": len(self._months),
                }
                self._state = self._activity_state_unlocked(scanning=False)
                self._event(f"months scanned {len(self._months)}")
        except Exception as exc:
            with self._lock:
                self._state = "error"
                self._event(f"scan error {type(exc).__name__}: {exc}")

    async def _download_message_ids(
        self,
        message_ids: List[str],
        max_bytes: Optional[int],
        concurrency: int = 1,
    ):
        try:
            ids = [int(message_id) for message_id in message_ids]
            restorable_by_id = {
                str(record.get("message_id")): record for record in self.restorable_records()
            }
            client = await connect_client()
            raw_messages = await client.get_messages("me", ids=ids)
            if not isinstance(raw_messages, list):
                raw_messages = [raw_messages]
            messages_by_requested_id = {}
            if len(raw_messages) == len(ids):
                messages_by_requested_id.update(zip(ids, raw_messages))
            for msg in raw_messages:
                if msg:
                    messages_by_requested_id[getattr(msg, "id", None)] = msg
            messages = []
            source_missing = []
            for message_id in ids:
                msg = messages_by_requested_id.get(message_id)
                if msg and (msg.photo or msg.video):
                    messages.append((msg, item_from_message(msg)))
                else:
                    source_missing.append(str(message_id))
            for message_id in source_missing:
                record = restorable_by_id.get(message_id)
                if record:
                    reason = "message has no media" if messages_by_requested_id.get(int(message_id)) else "message not found"
                    append_source_missing_record(self.source_missing_log, record, reason)
                    self.invalidate_restorable_cache()
                    with self._lock:
                        self._active_pending_ids.discard(str(message_id))
                        self._stats.source_missing += 1
                        self._event(f"source missing {message_id}")
            await self._process_messages(client, messages, max_bytes, concurrency, total_override=len(ids))
        except Exception as exc:
            with self._lock:
                self._stats.failed += 1
                self._event(f"restore error {type(exc).__name__}: {exc}")

    async def _download_recent(
        self,
        days: int,
        max_bytes: Optional[int],
        restore_ids: Optional[set] = None,
        concurrency: int = 1,
    ):
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        await self._download_range(cutoff, None, max_bytes, restore_ids, concurrency)

    async def _download_range(
        self,
        start_inclusive: datetime,
        end_exclusive: Optional[datetime],
        max_bytes: Optional[int],
        restore_ids: Optional[set] = None,
        concurrency: int = 1,
    ):
        try:
            client = await connect_client()

            messages = []
            async for msg in client.iter_messages("me"):
                if msg.date < start_inclusive:
                    break
                if end_exclusive and msg.date >= end_exclusive:
                    continue
                if msg.photo or msg.video:
                    item = item_from_message(msg)
                    if restore_ids is None or str(item.message_id) in restore_ids:
                        messages.append((msg, item))

            await self._process_messages(client, messages, max_bytes, concurrency)
        except Exception as exc:
            with self._lock:
                self._stats.failed += 1
                self._event(f"error {type(exc).__name__}: {exc}")

    async def _process_messages(
        self,
        client: TelegramClient,
        messages,
        max_bytes: Optional[int],
        concurrency: int = 1,
        total_override: Optional[int] = None,
    ):
        with self._lock:
            self._stats.total = len(messages) if total_override is None else total_override
            self._write_state_unlocked()

        pending = asyncio.Queue()
        for message in messages:
            pending.put_nowait(message)

        async def worker():
            while True:
                with self._lock:
                    if self._pause_requested or self._stop_requested:
                        return
                try:
                    msg, item = pending.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    with self._lock:
                        if (
                            self._active_job
                            and self._active_job.get("kind") == "restore"
                            and str(item.message_id) not in self._active_pending_ids
                        ):
                            continue
                        self._active_pending_ids.discard(str(item.message_id))
                    await self._process_one_message(client, msg, item, max_bytes)
                finally:
                    pending.task_done()

        workers = [asyncio.create_task(worker()) for _ in range(normalize_concurrency(concurrency))]
        try:
            await asyncio.gather(*workers)
        finally:
            for task in workers:
                if not task.done():
                    task.cancel()

        await client.disconnect()
        with self._lock:
            updated = summarize_months([item for _, item in messages], self.out_root, self.skip_log)
            self._months.update(updated)
            if self._state not in ("paused", "stopped", "stopping", "pausing", "error"):
                self._currents = {}
                self._current = None
                self._active_pending_ids = set()
                self._state = self._activity_state_unlocked(downloading=False)
                self._event("complete")
            elif self._state in ("stopped", "stopping", "error"):
                self._currents = {}
                self._current = None
                self._write_state_unlocked()
            else:
                self._write_state_unlocked()

    async def _process_one_message(self, client: TelegramClient, msg, item: MediaItem, max_bytes: Optional[int]):
        with self._lock:
            if self._stop_requested:
                self._state = "stopped"
                self._event("stopped")
                return
            if self._pause_requested:
                self._active_pending_ids.add(str(item.message_id))
                self._state = "paused"
                self._event("paused")
                return

        target = media_target_path(item, self.out_root)
        target.parent.mkdir(parents=True, exist_ok=True)

        if str(item.message_id) in self.ignored_message_ids():
            with self._lock:
                self._stats.skipped += 1
                self._event(f"ignored {item.message_id}")
            return

        if max_bytes is not None and item.size_bytes > max_bytes:
            append_skip_record(self.skip_log, item, target)
            self.invalidate_restorable_cache()
            with self._lock:
                self._stats.skipped += 1
                self._event(f"skipped >limit {item.message_id} {item.size_bytes / 1024 / 1024:.2f}MB")
            return

        if target.exists() and item.size_bytes and target.stat().st_size >= item.size_bytes:
            self.invalidate_restorable_cache()
            with self._lock:
                self._stats.downloaded += 1
                self._event(f"exists {item.message_id}")
            return

        downloaded = await self._download_item_with_retries(client, msg, item, target)
        if downloaded:
            self.invalidate_restorable_cache()
            with self._lock:
                self._stats.downloaded += 1
                self._clear_current_progress_unlocked(item)
                self._event(f"downloaded {item.message_id}")
        else:
            with self._lock:
                if not (self._stop_requested or self._pause_requested):
                    self._stats.failed += 1
                    self._clear_current_progress_unlocked(item)
                elif self._stop_requested:
                    self._clear_current_progress_unlocked(item)

    async def _download_item_with_retries(self, client: TelegramClient, msg, item: MediaItem, target: Path) -> bool:
        last_error = None
        for attempt in range(1, DOWNLOAD_RETRIES + 1):
            with self._lock:
                if self._stop_requested or self._pause_requested:
                    return False
            download_started_at = time.time()
            self._set_current_progress(item, target, 0, item.size_bytes, download_started_at)
            with self._lock:
                self._event(f"downloading {item.message_id} attempt={attempt}")
            try:
                await self._download_media_with_stall_timeout(client, msg, item, target, download_started_at)
                return True
            except DownloadInterrupted:
                with self._lock:
                    if self._pause_requested:
                        self._state = "paused"
                        self._event("paused")
                    elif self._stop_requested:
                        self._clear_current_progress_unlocked(item)
                        self._state = "stopped"
                        self._event("stopped")
                return False
            except Exception as exc:
                last_error = exc
                with self._lock:
                    self._clear_current_progress_unlocked(item)
                    self._event(f"download retry {item.message_id} attempt={attempt} error={type(exc).__name__}: {exc}")
                if isinstance(exc, FileReferenceExpiredError) and attempt < DOWNLOAD_RETRIES:
                    refreshed = await client.get_messages("me", ids=int(item.message_id))
                    if refreshed and (getattr(refreshed, "photo", None) or getattr(refreshed, "video", None)):
                        msg = refreshed
                if attempt < DOWNLOAD_RETRIES:
                    await asyncio.sleep(min(2 ** (attempt - 1), 5))
        with self._lock:
            self._event(f"download failed {item.message_id} {type(last_error).__name__}: {last_error}")
        return False

    async def _download_media_with_stall_timeout(
        self,
        client: TelegramClient,
        msg,
        item: MediaItem,
        target: Path,
        download_started_at: float,
    ):
        last_progress_at = time.monotonic()

        def progress(downloaded, total):
            nonlocal last_progress_at
            last_progress_at = time.monotonic()
            self._set_current_progress(item, target, downloaded, total, download_started_at)

        if hasattr(client, "_iter_download"):
            task = asyncio.create_task(self._resume_download_media(client, msg, item, target, progress))
        else:
            task = asyncio.create_task(client.download_media(msg, file=str(target), progress_callback=progress))
        try:
            while True:
                done, _ = await asyncio.wait({task}, timeout=DOWNLOAD_CONTROL_POLL_INTERVAL)
                if done:
                    return await task
                with self._lock:
                    interrupted = self._pause_requested or self._stop_requested
                if interrupted:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                    raise DownloadInterrupted("download interrupted by pause or stop request")
                if time.monotonic() - last_progress_at > DOWNLOAD_STALL_TIMEOUT:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                    raise TimeoutError(f"no download progress for {DOWNLOAD_STALL_TIMEOUT}s")
        finally:
            if not task.done():
                task.cancel()

    async def _resume_download_media(self, client: TelegramClient, msg, item: MediaItem, target: Path, progress):
        resume_offset = 0
        if target.exists():
            resume_offset = target.stat().st_size
        if item.size_bytes:
            resume_offset = min(resume_offset, item.size_bytes)
        if resume_offset:
            aligned_offset = resume_offset - (resume_offset % MIN_CHUNK_SIZE)
            if aligned_offset != resume_offset:
                with target.open("r+b") as f:
                    f.truncate(aligned_offset)
                resume_offset = aligned_offset
            progress(resume_offset, item.size_bytes)

        msg_data = (msg.input_chat, msg.id) if getattr(msg, "input_chat", None) else None
        cdn_redirect = None
        key = iv = None
        while True:
            try:
                with target.open("ab") as f:
                    async for chunk in client._iter_download(
                        msg,
                        offset=resume_offset,
                        request_size=MAX_CHUNK_SIZE,
                        file_size=item.size_bytes,
                        msg_data=msg_data,
                        cdn_redirect=cdn_redirect,
                    ):
                        if key and iv:
                            chunk = AES.decrypt_ige(chunk, key, iv)
                        f.write(chunk)
                        resume_offset += len(chunk)
                        progress(resume_offset, item.size_bytes)
                    f.flush()
                break
            except _CdnRedirect as exc:
                cdn_redirect = exc.cdn_redirect
                key = cdn_redirect.encryption_key
                iv = cdn_redirect.encryption_iv

        if item.size_bytes and target.stat().st_size < item.size_bytes:
            raise TimeoutError(
                f"download ended at {target.stat().st_size} of {item.size_bytes} bytes"
            )


def item_from_message(msg) -> MediaItem:
    ext = ".jpg"
    name = ""
    if msg.file:
        ext = msg.file.ext or ext
        name = getattr(msg.file, "name", "") or ""
    return MediaItem(
        message_id=msg.id,
        date=msg.date,
        size_bytes=int(getattr(msg.file, "size", 0) or 0),
        file_name=name,
        extension=ext,
        kind="video" if msg.video else "photo",
    )


async def connect_client() -> TelegramClient:
    await asyncio.to_thread(TELEGRAM_CLIENT_LOCK.acquire)
    lock_held = True
    client = TelegramClient(
        downloader_session_source(),
        API_ID,
        API_HASH,
        connection_retries=1,
        request_retries=1,
        timeout=10,
        retry_delay=1,
    )
    original_disconnect = client.disconnect

    async def disconnect_and_release(*args, **kwargs):
        nonlocal lock_held
        try:
            return await original_disconnect(*args, **kwargs)
        finally:
            if lock_held:
                lock_held = False
                TELEGRAM_CLIENT_LOCK.release()

    client.disconnect = disconnect_and_release
    try:
        await asyncio.wait_for(client.connect(), timeout=20)
        if not await client.is_user_authorized():
            raise RuntimeError("下载器尚未导入已授权的 telegram_downloader.session。")
    except AuthKeyDuplicatedError as exc:
        await client.disconnect()
        raise RuntimeError(
            "下载器 Telegram 授权已失效：同一份 session 被不同网络同时使用。"
            "请导入新的独立会话。"
        ) from exc
    except Exception:
        await client.disconnect()
        raise
    return client


async def downloader_auth_status() -> Dict[str, bool]:
    if not downloader_session_exists():
        return {"authorized": False, "session_exists": False}
    if TELEGRAM_CLIENT_LOCK.locked():
        return {"authorized": True, "session_exists": True, "busy": True}
    try:
        client = await connect_client()
    except Exception as exc:
        return {"authorized": False, "session_exists": True, "error": str(exc)}
    try:
        authorized = await client.is_user_authorized()
        return {"authorized": bool(authorized), "session_exists": True}
    finally:
        await client.disconnect()


def downloader_session_exists() -> bool:
    return DOWNLOADER_SESSION_PATH.exists() or DOWNLOADER_STRING_SESSION_PATH.exists()


def downloader_session_source():
    if DOWNLOADER_STRING_SESSION_PATH.exists():
        return StringSession(DOWNLOADER_STRING_SESSION_PATH.read_text(encoding="utf-8").strip())
    return str(DOWNLOADER_SESSION_PATH)


def session_file_fingerprint(path: Path) -> Optional[str]:
    try:
        data = path.read_bytes()
    except OSError:
        return None
    return hashlib.sha256(data).hexdigest()


def read_revoked_session_fingerprints() -> set:
    try:
        payload = json.loads(REVOKED_SESSION_FINGERPRINTS_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return set()
    if not isinstance(payload, list):
        return set()
    return {str(item) for item in payload if item}


def write_revoked_session_fingerprints(fingerprints: set):
    REVOKED_SESSION_FINGERPRINTS_PATH.write_text(
        json.dumps(sorted(fingerprints), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def revoke_session_file(path: Path):
    fingerprint = session_file_fingerprint(path)
    if not fingerprint:
        return
    fingerprints = read_revoked_session_fingerprints()
    fingerprints.add(fingerprint)
    write_revoked_session_fingerprints(fingerprints)


def read_session_sign_out_ns() -> int:
    try:
        payload = json.loads(SESSION_SIGN_OUT_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return 0
    try:
        return int(payload.get("signed_out_at_ns") or 0)
    except (AttributeError, TypeError, ValueError):
        return 0


def write_session_sign_out_ns(value: int):
    SESSION_SIGN_OUT_PATH.write_text(
        json.dumps({"signed_out_at_ns": int(value)}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def mark_session_signed_out():
    write_session_sign_out_ns(time.time_ns())


def session_file_is_older_than_last_sign_out(path: Path) -> bool:
    cutoff = read_session_sign_out_ns()
    if not cutoff:
        return False
    try:
        return path.stat().st_mtime_ns <= cutoff
    except OSError:
        return False


def session_file_is_revoked(path: Path) -> bool:
    fingerprint = session_file_fingerprint(path)
    if fingerprint and fingerprint in read_revoked_session_fingerprints():
        return True
    return session_file_is_older_than_last_sign_out(path)


def revoke_current_downloader_session():
    revoke_session_file(DOWNLOADER_SESSION_PATH)
    revoke_session_file(DOWNLOADER_STRING_SESSION_PATH)


def import_session_file(session_path: str) -> Dict:
    source = Path(session_path.strip()).expanduser()
    if not source.exists() or not source.is_file():
        return {"ok": False, "error": f"找不到会话文件：{source}"}
    if session_file_is_revoked(source):
        return {"ok": False, "error": "这份 Session 已退出登录，且未找到退出后更新的 .session 文件。Telegram 官方网页版登录不会生成这里可用的 .session；请使用二维码登录，或先生成新的 .session 后再自动查找。"}
    if source.resolve() != DOWNLOADER_SESSION_PATH.resolve():
        DOWNLOADER_SESSION_PATH.write_bytes(source.read_bytes())
    remove_downloader_string_session()
    return {"ok": True, "mode": "session file", "source": str(source)}


def import_string_session(session_string: str) -> Dict:
    session_string = session_string.strip()
    if not session_string:
        return {"ok": False, "error": "请粘贴 StringSession。"}
    DOWNLOADER_STRING_SESSION_PATH.write_text(session_string, encoding="utf-8")
    remove_downloader_session_file()
    return {"ok": True, "mode": "StringSession"}


def auto_import_session() -> Dict:
    candidates = find_session_candidates()
    if not candidates:
        revoked_candidates = find_session_candidates(include_revoked=True)
        if revoked_candidates:
            return {"ok": False, "error": "找到的 Session 已退出登录，且未找到退出后更新的 .session 文件。Telegram 官方网页版登录不会生成这里可用的 .session；请使用二维码登录，或先生成新的 .session 后再自动查找。"}
        return {"ok": False, "error": "没有找到可导入的 .session 文件。"}
    return import_session_file(str(candidates[0]))


def find_session_candidates(include_revoked: bool = False) -> List[Path]:
    found: Dict[Path, Path] = {}
    excluded = {DOWNLOADER_SESSION_PATH.resolve()}
    for root in AUTO_SESSION_ROOTS:
        root = Path(root).expanduser()
        if not root.exists():
            continue
        try:
            iterator = root.rglob("*.session") if root.is_dir() else [root]
            for path in iterator:
                try:
                    if not path.is_file() or path.name.endswith("-journal"):
                        continue
                    resolved = path.resolve()
                    if resolved in excluded:
                        continue
                    if not include_revoked and session_file_is_revoked(path):
                        continue
                    found[resolved] = path
                    if len(found) >= 200:
                        break
                except OSError:
                    continue
        except OSError:
            continue
    return sorted(found.values(), key=lambda path: path.stat().st_mtime, reverse=True)


def clear_downloader_session() -> Dict:
    revoke_current_downloader_session()
    mark_session_signed_out()
    remove_downloader_session_file()
    remove_downloader_string_session()
    return {"ok": True}


class QRLoginManager:
    def __init__(self):
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._state: Dict = {"state": "idle"}
        self._password_event = threading.Event()
        self._password = ""
        self._cancel_requested = False

    def start(self) -> Dict:
        with self._lock:
            if self._thread and self._thread.is_alive():
                return dict(self._state)
            self._state = {"state": "starting"}
            self._cancel_requested = False
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()

        deadline = time.time() + 10
        while time.time() < deadline:
            status = self.status()
            if status["state"] in ("waiting", "authorized", "error"):
                return status
            time.sleep(0.1)
        return self.status()

    def submit_password(self, password: str) -> Dict:
        password = password.strip()
        if not password:
            return {"ok": False, "error": "请输入两步验证密码。"}
        with self._lock:
            if self._state.get("state") != "password_required":
                return {"ok": False, "error": "当前不需要两步验证密码。"}
            self._password = password
            self._state = {"state": "password_submitted"}
            self._password_event.set()
            return {"ok": True, "state": "password_submitted"}

    def status(self) -> Dict:
        with self._lock:
            return dict(self._state)

    def reset(self) -> Dict:
        with self._lock:
            self._cancel_requested = True
            self._password = ""
            self._password_event.set()
            self._state = {"state": "idle"}
        revoke_current_downloader_session()
        mark_session_signed_out()
        remove_downloader_session_file()
        remove_downloader_string_session()
        return {"ok": True}

    def _set_state(self, **state):
        with self._lock:
            if self._cancel_requested:
                return
            self._state = state

    def _is_cancelled(self) -> bool:
        with self._lock:
            return self._cancel_requested

    def _run(self):
        try:
            asyncio.run(self._run_async())
        except Exception as exc:
            if not self._is_cancelled():
                self._set_state(state="error", error=f"{type(exc).__name__}: {exc}")

    async def _run_async(self):
        remove_downloader_session_file()
        remove_downloader_string_session()
        self._password = ""
        self._password_event.clear()
        client = TelegramClient(
            str(DOWNLOADER_SESSION_PATH),
            API_ID,
            API_HASH,
            connection_retries=1,
            request_retries=1,
            timeout=10,
            retry_delay=1,
        )
        await client.connect()
        try:
            if self._is_cancelled():
                return
            qr_login = await client.qr_login()
            self._set_state(
                state="waiting",
                url=qr_login.url,
                image=qr_code_data_url(qr_login.url),
                expires=qr_login.expires.astimezone().isoformat(),
            )
            try:
                await qr_login.wait(timeout=QR_LOGIN_TIMEOUT)
            except SessionPasswordNeededError:
                if self._is_cancelled():
                    return
                self._set_state(state="password_required")
                password_ready = await asyncio.to_thread(self._password_event.wait, QR_LOGIN_TIMEOUT)
                if self._is_cancelled():
                    return
                if not password_ready:
                    self._set_state(state="expired", error="两步验证等待超时，请重新生成二维码。")
                    return
                await client.sign_in(password=self._password)
            if self._is_cancelled():
                return
            self._set_state(state="authorized")
        except asyncio.TimeoutError:
            self._set_state(state="expired", error="二维码已超时，请重新生成。")
        finally:
            await client.disconnect()
            if self._is_cancelled():
                revoke_current_downloader_session()
                mark_session_signed_out()
                remove_downloader_session_file()
                remove_downloader_string_session()


def qr_code_data_url(value: str) -> str:
    import qrcode

    image = qrcode.make(value)
    buffer = io.BytesIO()
    image.save(buffer, format="PNG")
    encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
    return f"data:image/png;base64,{encoded}"


def remove_downloader_session_file():
    for path in DOWNLOADER_SESSION_PATH.parent.glob(DOWNLOADER_SESSION_PATH.name + "*"):
        try:
            path.unlink()
        except OSError:
            pass


def remove_downloader_string_session():
    try:
        DOWNLOADER_STRING_SESSION_PATH.unlink()
    except OSError:
        pass
