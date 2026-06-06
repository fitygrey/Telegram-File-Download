import csv
import re
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List


@dataclass(frozen=True)
class MediaItem:
    message_id: int
    date: datetime
    size_bytes: int
    file_name: str
    extension: str
    kind: str


@dataclass
class ProgressState:
    total: int = 0
    downloaded: int = 0
    skipped: int = 0
    failed: int = 0
    source_missing: int = 0

    @property
    def handled(self) -> int:
        return self.downloaded + self.skipped + self.failed + self.source_missing

    @property
    def pending(self) -> int:
        return max(self.total - self.handled, 0)

    @property
    def restorable(self) -> int:
        return self.skipped


@dataclass
class DownloadStats:
    checked: int = 0
    downloaded: int = 0
    skipped_existing: int = 0
    skipped_large: int = 0
    failed: int = 0


def safe_name(value: str, fallback: str) -> str:
    value = unicodedata.normalize("NFC", value or "").strip()
    value = re.sub(r'[\\/:*?"<>|\x00-\x1f]+', "_", value)
    value = re.sub(r"\s+", " ", value).strip(" .")
    return value[:120] or fallback


def media_target_path(item: MediaItem, out_root: Path) -> Path:
    local_dt = item.date.astimezone()
    day_dir = out_root / local_dt.strftime("%Y-%m-%d")
    ext = item.extension or ".jpg"
    original = safe_name(item.file_name, "")
    stem = local_dt.strftime("%Y%m%d_%H%M%S") + f"_{item.message_id}"
    if original:
        original_path = Path(original)
        original_stem = safe_name(original_path.stem, "media")
        original_ext = original_path.suffix or ext
        return day_dir / f"{stem}_{original_stem}{original_ext}"
    return day_dir / f"{stem}{ext}"


def month_key(date: datetime) -> str:
    return date.astimezone().strftime("%Y-%m")


def summarize_months(items: List[MediaItem], out_root: Path, skip_log: Path) -> Dict[str, Dict[str, int]]:
    skipped_ids = {record.get("message_id") for record in read_skip_records(skip_log)}
    summary: Dict[str, Dict[str, int]] = {}
    for item in items:
        key = month_key(item.date)
        row = summary.setdefault(
            key,
            {"total": 0, "downloaded": 0, "skipped": 0, "pending": 0},
        )
        row["total"] += 1
        target = media_target_path(item, out_root)
        if target.exists() and (
            not item.size_bytes or target.stat().st_size >= item.size_bytes
        ):
            row["downloaded"] += 1
        elif str(item.message_id) in skipped_ids:
            row["skipped"] += 1
        else:
            row["pending"] += 1
    return dict(sorted(summary.items(), reverse=True))


def read_skip_records(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def read_ignore_records(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def read_source_missing_records(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def append_source_missing_record(path: Path, record: Dict[str, str], reason: str = ""):
    path.parent.mkdir(parents=True, exist_ok=True)
    records = read_source_missing_records(path)
    message_id = str(record.get("message_id") or "")
    records = [row for row in records if row.get("message_id") != message_id]
    records.append(
        {
            "message_id": message_id,
            "date": record.get("date", ""),
            "size_bytes": record.get("size_bytes", ""),
            "size_mb": record.get("size_mb", ""),
            "target_path": record.get("target_path", ""),
            "reason": reason,
        }
    )
    with path.open("w", newline="", encoding="utf-8") as f:
        fieldnames = ["message_id", "date", "size_bytes", "size_mb", "target_path", "reason"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)


def append_ignore_record(path: Path, item: MediaItem, target: Path, reason: str = ""):
    path.parent.mkdir(parents=True, exist_ok=True)
    records = read_ignore_records(path)
    message_id = str(item.message_id)
    records = [record for record in records if record.get("message_id") != message_id]
    records.append(
        {
            "message_id": message_id,
            "date": item.date.astimezone().isoformat(),
            "size_bytes": str(item.size_bytes),
            "size_mb": f"{item.size_bytes / 1024 / 1024:.2f}",
            "target_path": str(target),
            "reason": reason,
        }
    )
    with path.open("w", newline="", encoding="utf-8") as f:
        fieldnames = ["message_id", "date", "size_bytes", "size_mb", "target_path", "reason"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)


def append_skip_record(path: Path, item: MediaItem, target: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    records = read_skip_records(path)
    message_id = str(item.message_id)
    records = [record for record in records if record.get("message_id") != message_id]
    records.append(
        {
            "message_id": message_id,
            "date": item.date.astimezone().isoformat(),
            "size_bytes": str(item.size_bytes),
            "size_mb": f"{item.size_bytes / 1024 / 1024:.2f}",
            "target_path": str(target),
        }
    )
    with path.open("w", newline="", encoding="utf-8") as f:
        fieldnames = ["message_id", "date", "size_bytes", "size_mb", "target_path"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
