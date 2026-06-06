#!/usr/bin/env python3
import asyncio
import csv
import json
import os
import re
import unicodedata
from datetime import datetime, timedelta, timezone
from pathlib import Path

from telethon import TelegramClient
from telethon.crypto.authkey import AuthKey
from telethon.sessions import SQLiteSession


API_ID = 2496
API_HASH = "8da85b0d5bfe62527e5b244c209159c3"
CODEX_TG_STORAGE = Path.home() / (
    "Library/Application Support/Codex/Partitions/codex-browser-app/Local Storage/leveldb"
)
OUT_ROOT = Path("/Volumes/ZHITAI/telegram")
SESSION_PATH = Path(__file__).with_name("telegram_web_imported.session")
MAX_BYTES = 100 * 1024 * 1024
SKIP_LOG = OUT_ROOT / "skipped_over_100mb.csv"

DC_ADDR = {
    1: "149.154.175.53",
    2: "149.154.167.51",
    3: "149.154.175.100",
    4: "149.154.167.91",
    5: "91.108.56.130",
}


def load_web_session_data() -> dict:
    newest = None
    for path in CODEX_TG_STORAGE.glob("*"):
        if not path.is_file():
            continue
        text = path.read_bytes().decode("utf-8", errors="ignore")
        for match in re.finditer(r'\{"dcId":\d+,[^{}]*"userId":"\d+"[^{}]*\}', text):
            try:
                data = json.loads(match.group(0))
            except json.JSONDecodeError:
                continue
            if any(k.endswith("_auth_key") for k in data):
                newest = data
    if not newest:
        raise RuntimeError("Could not find Telegram Web auth data in Codex local storage.")
    return newest


def build_session_from_web() -> SQLiteSession:
    data = load_web_session_data()
    dc_id = int(data.get("dcId") or data.get("dcID"))
    key_hex = data[f"dc{dc_id}_auth_key"]
    session = SQLiteSession(str(SESSION_PATH))
    session.set_dc(dc_id, DC_ADDR[dc_id], 443)
    session.auth_key = AuthKey(bytes.fromhex(key_hex))
    session.save()
    return session


def safe_name(value: str, fallback: str) -> str:
    value = unicodedata.normalize("NFC", value or "").strip()
    value = re.sub(r'[\\/:*?"<>|\x00-\x1f]+', "_", value)
    value = re.sub(r"\s+", " ", value).strip(" .")
    return value[:120] or fallback


def target_for_message(msg) -> Path:
    local_dt = msg.date.astimezone()
    day_dir = OUT_ROOT / local_dt.strftime("%Y-%m-%d")
    day_dir.mkdir(parents=True, exist_ok=True)

    ext = ".jpg"
    if msg.file and msg.file.ext:
        ext = msg.file.ext

    original = safe_name(getattr(msg.file, "name", "") if msg.file else "", "")
    stem = local_dt.strftime("%Y%m%d_%H%M%S") + f"_{msg.id}"
    if original:
        original_stem = Path(original).stem
        original_ext = Path(original).suffix or ext
        return day_dir / f"{stem}_{safe_name(original_stem, 'media')}{original_ext}"
    return day_dir / f"{stem}{ext}"


def media_size(msg) -> int:
    size = getattr(getattr(msg, "file", None), "size", None)
    return int(size or 0)


def log_large_skip(msg, target: Path, size: int):
    first_write = not SKIP_LOG.exists()
    with SKIP_LOG.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if first_write:
            writer.writerow(["message_id", "date", "size_bytes", "size_mb", "target_path"])
        writer.writerow([
            msg.id,
            msg.date.astimezone().isoformat(),
            size,
            f"{size / 1024 / 1024:.2f}",
            str(target),
        ])


async def main():
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    cutoff = datetime.now(timezone.utc) - timedelta(days=31)
    session = build_session_from_web()

    client = TelegramClient(
        session,
        API_ID,
        API_HASH,
        connection_retries=1,
        request_retries=1,
        timeout=10,
        retry_delay=1,
    )
    print(f"Using Telegram DC {session.dc_id} at {session.server_address}:443", flush=True)
    await asyncio.wait_for(client.connect(), timeout=20)
    if not await client.is_user_authorized():
        raise RuntimeError("Imported Telegram Web session is not authorized.")

    me = await client.get_me()
    print(f"Connected as Telegram user {me.id}. Downloading media since {cutoff.date()}...")

    checked = downloaded = skipped = large_skipped = 0
    async for msg in client.iter_messages("me"):
        if msg.date < cutoff:
            break
        checked += 1
        if not (msg.photo or msg.video):
            continue

        target = target_for_message(msg)
        size = media_size(msg)
        if size > MAX_BYTES:
            log_large_skip(msg, target, size)
            large_skipped += 1
            print(
                f"Skipping >100MB {msg.id} ({size / 1024 / 1024:.2f} MB) -> {target}",
                flush=True,
            )
            continue

        if target.exists() and target.stat().st_size > 0:
            skipped += 1
            continue

        print(f"Downloading {msg.id} -> {target}")
        result = await client.download_media(msg, file=str(target))
        if result:
            downloaded += 1

    await client.disconnect()
    print(
        f"Done. Checked {checked} messages, downloaded {downloaded}, "
        f"skipped existing {skipped}, skipped >100MB {large_skipped}."
    )


if __name__ == "__main__":
    asyncio.run(main())
