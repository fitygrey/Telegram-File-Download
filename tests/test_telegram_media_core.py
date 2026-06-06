import csv
import tempfile
import unittest
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from telegram_media_core import (
    DownloadStats,
    MediaItem,
    ProgressState,
    append_skip_record,
    media_target_path,
    month_key,
    read_skip_records,
    safe_name,
    summarize_months,
)


class CoreTests(unittest.TestCase):
    def test_safe_name_removes_path_separators_and_control_chars(self):
        self.assertEqual(safe_name(' bad/name:\x00 "x".mp4 ', "fallback"), "bad_name_ _x_.mp4")
        self.assertEqual(safe_name("", "fallback"), "fallback")

    def test_media_target_path_groups_by_local_date_and_message_id(self):
        item = MediaItem(
            message_id=42,
            date=datetime(2026, 4, 25, 8, 5, 6, tzinfo=timezone.utc),
            size_bytes=12,
            file_name="hello/world.mp4",
            extension=".mp4",
            kind="video",
        )

        target = media_target_path(item, Path("/tmp/out"))

        self.assertEqual(target.parent, Path("/tmp/out/2026-04-25"))
        self.assertEqual(target.name, "20260425_160506_42_hello_world.mp4")

    def test_progress_state_counts_pending_and_restorable(self):
        state = ProgressState(total=10, downloaded=3, skipped=2, failed=1)

        self.assertEqual(state.pending, 4)
        self.assertEqual(state.handled, 6)
        self.assertEqual(state.restorable, 2)

    def test_skip_records_are_deduplicated_by_message_id(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "skipped.csv"
            item = MediaItem(
                message_id=99,
                date=datetime(2026, 4, 20, 1, 2, 3, tzinfo=timezone.utc),
                size_bytes=150 * 1024 * 1024,
                file_name="large.mp4",
                extension=".mp4",
                kind="video",
            )

            append_skip_record(path, item, Path("/tmp/out/large.mp4"))
            append_skip_record(path, item, Path("/tmp/out/large.mp4"))

            records = read_skip_records(path)
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0]["message_id"], "99")

    def test_summarize_months_counts_existing_skipped_and_pending(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            existing = MediaItem(
                message_id=1,
                date=datetime(2026, 4, 20, 1, 2, 3, tzinfo=timezone.utc),
                size_bytes=10,
                file_name="one.jpg",
                extension=".jpg",
                kind="photo",
            )
            skipped = MediaItem(
                message_id=2,
                date=datetime(2026, 4, 18, 1, 2, 3, tzinfo=timezone.utc),
                size_bytes=200,
                file_name="two.mp4",
                extension=".mp4",
                kind="video",
            )
            pending = MediaItem(
                message_id=3,
                date=datetime(2026, 3, 30, 1, 2, 3, tzinfo=timezone.utc),
                size_bytes=20,
                file_name="three.jpg",
                extension=".jpg",
                kind="photo",
            )
            media_target_path(existing, root).parent.mkdir(parents=True)
            media_target_path(existing, root).write_bytes(b"x" * existing.size_bytes)
            append_skip_record(root / "skipped_over_100mb.csv", skipped, media_target_path(skipped, root))

            summary = summarize_months([existing, skipped, pending], root, root / "skipped_over_100mb.csv")

            self.assertEqual(month_key(existing.date), "2026-04")
            self.assertEqual(summary["2026-04"]["total"], 2)
            self.assertEqual(summary["2026-04"]["downloaded"], 1)
            self.assertEqual(summary["2026-04"]["skipped"], 1)
            self.assertEqual(summary["2026-04"]["pending"], 0)
            self.assertEqual(summary["2026-03"]["pending"], 1)

    def test_summarize_months_counts_partial_file_as_pending(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            partial = MediaItem(
                message_id=4,
                date=datetime(2026, 4, 20, 1, 2, 3, tzinfo=timezone.utc),
                size_bytes=10,
                file_name="partial.mp4",
                extension=".mp4",
                kind="video",
            )
            target = media_target_path(partial, root)
            target.parent.mkdir(parents=True)
            target.write_bytes(b"x")

            summary = summarize_months([partial], root, root / "skipped_over_100mb.csv")

            self.assertEqual(summary["2026-04"]["downloaded"], 0)
            self.assertEqual(summary["2026-04"]["pending"], 1)


if __name__ == "__main__":
    unittest.main()
