#!/usr/bin/env python3
import argparse
import re
from datetime import datetime
from pathlib import Path

from telegram_media_core import MediaItem, append_ignore_record


OUT_ROOT = Path("/Volumes/ZHITAI/telegram")


def item_from_path(path: Path) -> MediaItem:
    date_match = re.match(r"(\d{8})_(\d{6})_(\d+)(?:_|\.|$)", path.name)
    if date_match:
        message_id = int(date_match.group(3))
        date = datetime.strptime(date_match.group(1) + date_match.group(2), "%Y%m%d%H%M%S").astimezone()
    else:
        match = re.search(r"_(\d+)(?:_|$)", path.name)
        if not match:
            raise ValueError(f"Could not parse message id from filename: {path.name}")
        message_id = int(match.group(1))
        date = datetime.fromtimestamp(path.stat().st_mtime).astimezone()
    if not path.exists():
        raise ValueError(f"Could not parse message id from filename: {path.name}")
    return MediaItem(
        message_id=message_id,
        date=date,
        size_bytes=path.stat().st_size if path.exists() else 0,
        file_name=path.name,
        extension=path.suffix,
        kind="video" if path.suffix.lower() in {".mp4", ".m4v", ".mov", ".webm"} else "photo",
    )


def main():
    parser = argparse.ArgumentParser(
        description="Mark downloaded Telegram media as ignored so future runs do not download it again."
    )
    parser.add_argument("paths", nargs="+", help="Downloaded media files to ignore")
    parser.add_argument("--delete", action="store_true", help="Delete files after marking them ignored")
    parser.add_argument("--root", default=str(OUT_ROOT), help="Telegram output root")
    args = parser.parse_args()

    root = Path(args.root)
    ignore_log = root / "ignored_downloads.csv"
    for raw_path in args.paths:
        path = Path(raw_path).expanduser()
        item = item_from_path(path)
        append_ignore_record(ignore_log, item, path, "deleted by user")
        print(f"ignored {item.message_id}: {path}")
        if args.delete:
            path.unlink(missing_ok=True)
            print(f"deleted {path}")


if __name__ == "__main__":
    main()
