# -*- coding: utf-8 -*-
"""采集器与 Web 端到端测试（仅标准库，运行：PYTHONPATH=. python3 -I -m unittest -v）。"""

import json
import os
import site
import sqlite3
import sys
import tempfile
import threading
import unittest
from unittest import mock

# 兼容 python3 -I（隔离模式会忽略 PYTHONPATH）：允许通过 TEST_SITE 注入依赖路径
for _p in os.environ.get("TEST_SITE", "").split(os.pathsep):
    if _p:
        site.addsitedir(_p)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import collector  # noqa: E402

ACTIVE = collector.ACTIVE_NAME
ROTATED = collector.ROTATED_NAME


def rec(seq: int, temp: float = 25.0, ts: str = "2026-11-15T08:00:00Z") -> str:
    return json.dumps({"seq": seq, "ts": ts, "temp": temp})


class CollectorTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.data = os.path.join(self.tmp.name, "data")
        self.db = os.path.join(self.tmp.name, "db", "test.db")
        os.makedirs(self.data, exist_ok=True)
        self.active = os.path.join(self.data, ACTIVE)
        self.rotated = os.path.join(self.data, ROTATED)

    def tearDown(self):
        self.tmp.cleanup()

    def write(self, path, text, mode="a"):
        with open(path, mode, encoding="utf-8") as fh:
            fh.write(text)

    def seqs(self):
        rows, total = collector.list_records(self.db)
        return sorted(r["seq"] for r in rows), total

    def test_basic_incremental_and_idempotent_click(self):
        self.write(self.active, rec(1) + "\n" + rec(2, 26.5) + "\n")
        r1 = collector.collect_once(self.data, self.db)
        self.assertEqual(r1["status"], "ok")
        self.assertEqual(r1["records_inserted"], 2)
        r2 = collector.collect_once(self.data, self.db)  # 重复点击
        self.assertEqual(r2["status"], "ok")
        self.assertEqual((r2["records_inserted"], r2["lines_seen"]), (0, 0))
        self.assertEqual(self.seqs(), ([1, 2], 2))
        # 再追加，只采新增
        self.write(self.active, rec(3, 27.1) + "\n")
        r3 = collector.collect_once(self.data, self.db)
        self.assertEqual(r3["records_inserted"], 1)
        self.assertEqual(self.seqs(), ([1, 2, 3], 3))

    def test_half_line_waits_for_next_round(self):
        self.write(self.active, rec(1) + "\n")
        collector.collect_once(self.data, self.db)
        half = rec(2, 30.2)
        self.write(self.active, half[: len(half) // 2])  # 无换行
        r = collector.collect_once(self.data, self.db)
        self.assertEqual((r["status"], r["records_inserted"]), ("ok", 0))
        self.assertGreater(r["half_line_bytes"], 0)
        # 进度不前移
        state = {s["inode"]: s for s in collector.list_file_states(self.db)}
        ino = os.stat(self.active).st_ino
        self.assertEqual(state[ino]["offset"], len(rec(1) + "\n"))
        # 补全换行后下一轮入库
        with open(self.active, "a", encoding="utf-8") as fh:
            fh.write(half[len(half) // 2:] + "\n")
        r2 = collector.collect_once(self.data, self.db)
        self.assertEqual(r2["records_inserted"], 1)
        self.assertEqual(self.seqs(), ([1, 2], 2))

    def test_rotation_old_file_continuation_and_new_file(self):
        self.write(self.active, rec(1) + "\n" + rec(2) + "\n")
        collector.collect_once(self.data, self.db)
        ino_active = os.stat(self.active).st_ino

        # 设备轮转
        os.rename(self.active, self.rotated)
        open(self.active, "a").close()
        ino_new = os.stat(self.active).st_ino
        self.assertNotEqual(ino_active, ino_new)

        # 旧文件续写 1 条；新文件写 2 条（轮转与采集交错：本轮前不采集）
        self.write(self.rotated, rec(3) + "\n")
        self.write(self.active, rec(4) + "\n" + rec(5) + "\n")
        r = collector.collect_once(self.data, self.db)
        self.assertEqual(r["status"], "ok", r["error"])
        self.assertEqual(r["records_inserted"], 3)
        self.assertEqual(self.seqs(), ([1, 2, 3, 4, 5], 5))

        states = {s["inode"]: s for s in collector.list_file_states(self.db)}
        # 旧 inode 按新路径记录且偏移推进
        self.assertEqual(states[ino_active]["path"], self.rotated)
        self.assertEqual(states[ino_active]["offset"], os.path.getsize(self.rotated))
        self.assertEqual(states[ino_new]["path"], self.active)

        # 再点一次：空操作，无重记
        r2 = collector.collect_once(self.data, self.db)
        self.assertEqual((r2["records_inserted"], r2["lines_seen"]), (0, 0))
        self.assertEqual(self.seqs(), ([1, 2, 3, 4, 5], 5))

    def test_double_rotation_prunes_stale_inode(self):
        self.write(self.active, rec(1) + "\n")
        collector.collect_once(self.data, self.db)
        ino1 = os.stat(self.active).st_ino

        os.rename(self.active, self.rotated)
        open(self.active, "a").close()
        self.write(self.active, rec(2) + "\n")
        collector.collect_once(self.data, self.db)
        ino2 = os.stat(self.active).st_ino

        # 第二次轮转：ino1 对应的文件消失（被设备替换），ino2 变成 .1
        os.rename(self.active, self.rotated)
        open(self.active, "a").close()
        self.write(self.active, rec(3) + "\n")
        collector.collect_once(self.data, self.db)
        ino3 = os.stat(self.active).st_ino

        states = {s["inode"]: s for s in collector.list_file_states(self.db)}
        self.assertNotIn(ino1, states) if ino1 not in (ino2, ino3) else None
        # 若文件系统复用了 inode，该 inode 的路径必须已更新为现存文件，且不漏记
        self.assertEqual(states[ino2]["path"], self.rotated)
        self.assertEqual(states[ino3]["path"], self.active)
        self.assertEqual(self.seqs(), ([1, 2, 3], 3))

    def test_invalid_line_rejects_whole_round_keeps_progress(self):
        bad_cases = [
            ("nan", "有限数值"),                                    # NaN 温度
            ('{"seq": 2, "ts": "2026-11-15 08:00:00", "temp": 26}', "UTC"),  # 非 UTC 表达
            ('{"seq": "x", "ts": "2026-11-15T08:00:00Z", "temp": 26}', "整数"),  # seq 非整数
            ('{"seq": 2, "ts": "2026-11-15T08:00:00Z", "temp": null}', "有限数值"),  # temp 非数值
            ('{"seq": 2, "ts": "2026-11-15T08:00:00Z", "temp": 26,}', "非法 JSON"),  # 坏 JSON
            ("", "非法 JSON"),                                     # 空行
        ]
        base = rec(1) + "\n"
        for idx, (bad, expect_msg) in enumerate(bad_cases):
            # 每个用例独立数据目录与库：先入库 1 条合法记录，再追加“合法 + 非法”
            case_dir = os.path.join(self.tmp.name, f"case_{idx}")
            os.makedirs(case_dir, exist_ok=True)
            case_db = os.path.join(self.tmp.name, f"case_{idx}.db")
            case_active = os.path.join(case_dir, ACTIVE)
            with open(case_active, "w", encoding="utf-8") as fh:
                fh.write(base)
            collector.collect_once(case_dir, case_db)

            with open(case_active, "a", encoding="utf-8") as fh:
                fh.write(rec(2, 26.0) + "\n")
                if bad == "nan":
                    fh.write('{"seq": 3, "ts": "2026-11-15T08:01:00Z", "temp": NaN}\n')
                else:
                    fh.write(bad + "\n")
            r = collector.collect_once(case_dir, case_db)
            self.assertEqual(r["status"], "rejected", f"case {idx}: {bad}")
            self.assertIn(expect_msg, r["error"])
            # 整轮拒绝：合法新增也不入库，进度停在首条记录之后
            rows, total = collector.list_records(case_db)
            self.assertEqual(sorted(x["seq"] for x in rows), [1], f"case {idx}")
            self.assertEqual(total, 1, f"case {idx}")
            state = collector.list_file_states(case_db)[0]
            self.assertEqual(state["offset"], len(base.encode("utf-8")), f"case {idx}")

        # 非法行移除后，同一偏移可正常继续采集
        with open(self.active, "w", encoding="utf-8") as fh:
            fh.write(rec(1) + "\n" + rec(2, 26.0) + "\n")
        r = collector.collect_once(self.data, self.db)
        self.assertEqual(r["status"], "ok")
        self.assertEqual(self.seqs(), ([1, 2], 2))

    def test_seq_gap_rejects(self):
        self.write(self.active, rec(1) + "\n")
        collector.collect_once(self.data, self.db)
        # 序号跳跃 2 -> 4
        self.write(self.active, rec(3) + "\n")
        r = collector.collect_once(self.data, self.db)
        self.assertEqual(r["status"], "rejected")
        self.assertIn("连续递增", r["error"])
        self.assertEqual(self.seqs(), ([1], 1))

    def test_duplicate_seq_different_content_rejects(self):
        ts1 = "2026-11-15T08:00:00Z"
        self.write(self.active, rec(1, 25.0, ts1) + "\n")
        collector.collect_once(self.data, self.db)
        # 同序号异内容以“新增字节”到达 -> 整轮拒绝，进度不变
        self.write(self.active, rec(1, 99.9, ts1) + "\n")
        r2 = collector.collect_once(self.data, self.db)
        self.assertEqual(r2["status"], "rejected")
        self.assertIn("内容不同", r2["error"])
        self.assertEqual(self.seqs(), ([1], 1))
        # 再点一次仍然拒绝（字节未消费，不会悄悄跳过）
        r_again = collector.collect_once(self.data, self.db)
        self.assertEqual(r_again["status"], "rejected")
        self.assertEqual(self.seqs(), ([1], 1))

    def test_duplicate_seq_same_content_is_skipped(self):
        ts1 = "2026-11-15T08:00:00Z"
        self.write(self.active, rec(1, 25.0, ts1) + "\n")
        collector.collect_once(self.data, self.db)
        # 同序号同内容（重复传输）：跳过计数，不重记
        self.write(self.active, rec(1, 25.0, ts1) + "\n")
        r3 = collector.collect_once(self.data, self.db)
        self.assertEqual(r3["status"], "ok")
        self.assertEqual((r3["duplicates"], r3["records_inserted"]), (1, 0))
        self.assertEqual(self.seqs(), ([1], 1))

    def test_in_batch_same_seq_different_content(self):
        self.write(self.active, rec(1) + "\n" + rec(1, 31.5) + "\n")
        r = collector.collect_once(self.data, self.db)
        self.assertEqual(r["status"], "rejected")
        self.assertIn("同一序号", r["error"])
        self.assertEqual(self.seqs(), ([], 0))

    def test_non_utf8_bytes_reject_whole_round(self):
        self.write(self.active, rec(1) + "\n")
        collector.collect_once(self.data, self.db)
        offset = collector.list_file_states(self.db)[0]["offset"]
        # 追加一条完整“行”，但含非法 UTF-8 字节
        with open(self.active, "ab") as fh:
            fh.write(b'{"seq": 2, "ts": "2026-11-15T08:00:00Z", "temp": 2}\xff\n')
        r = collector.collect_once(self.data, self.db)
        self.assertEqual(r["status"], "rejected")
        self.assertIn("UTF-8", r["error"])
        self.assertEqual(self.seqs(), ([1], 1))
        self.assertEqual(collector.list_file_states(self.db)[0]["offset"], offset)

    def test_stale_inode_state_is_pruned(self):
        self.write(self.active, rec(1) + "\n")
        collector.collect_once(self.data, self.db)
        # 手工插入一条指向不可能存在的 inode 的陈旧进度
        with collector.get_db(self.db) as conn:
            conn.execute(
                "INSERT INTO file_state(inode, path, offset, file_size, updated_at)"
                " VALUES (99999999, '/gone/active.ndjson.1', 0, 0, '2026-01-01T00:00:00Z')"
            )
        self.write(self.active, rec(2) + "\n")
        collector.collect_once(self.data, self.db)
        inodes = {s["inode"] for s in collector.list_file_states(self.db)}
        self.assertNotIn(99999999, inodes)
        self.assertEqual(self.seqs(), ([1, 2], 2))

    def test_restart_resumes_from_persisted_progress(self):
        self.write(self.active, rec(1) + "\n")
        collector.collect_once(self.data, self.db)
        # 模拟服务重启：重新走一遍完整流程，进度从 SQLite 恢复
        collector.init_db(self.db)
        self.write(self.active, rec(2) + "\n")
        r = collector.collect_once(self.data, self.db)
        self.assertEqual(r["records_inserted"], 1)
        self.assertEqual(self.seqs(), ([1, 2], 2))

    def test_interleaved_rotation_between_collects(self):
        # 采集 -> 轮转 -> 采集（旧文件无新增）-> 旧文件追加 + 新文件追加 -> 采集
        self.write(self.active, rec(1) + "\n")
        collector.collect_once(self.data, self.db)
        os.rename(self.active, self.rotated)
        open(self.active, "a").close()
        collector.collect_once(self.data, self.db)  # 空轮次，但已记录两个 inode

        self.write(self.rotated, rec(2) + "\n")
        self.write(self.active, rec(3) + "\n")
        r = collector.collect_once(self.data, self.db)
        self.assertEqual(r["status"], "ok", r["error"])
        self.assertEqual(r["records_inserted"], 2)
        self.assertEqual(self.seqs(), ([1, 2, 3], 3))

    def test_validate_line_rules(self):
        ok = rec(1)
        self.assertEqual(collector.validate_line(ok), (1, "2026-11-15T08:00:00Z", 25.0))
        with self.assertRaises(collector.RejectRound):
            collector.validate_line('{"seq": true, "ts": "2026-11-15T08:00:00Z", "temp": 1}')
        with self.assertRaises(collector.RejectRound):
            collector.validate_line('{"seq": 1, "ts": "2026-11-15T08:00:00+08:00", "temp": 1}')
        with self.assertRaises(collector.RejectRound):
            collector.validate_line("[1,2,3]")


class ResetAtomicityTest(unittest.TestCase):
    """演示重置与采集交错、文件/写库失败、中途重启的原子性用例。"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.data = os.path.join(self.tmp.name, "data")
        self.db = os.path.join(self.tmp.name, "db", "reset.db")
        os.makedirs(self.data, exist_ok=True)
        self.active = os.path.join(self.data, ACTIVE)
        self.rotated = os.path.join(self.data, ROTATED)

    def tearDown(self):
        self.tmp.cleanup()

    def write(self, path, text, mode="a"):
        with open(path, mode, encoding="utf-8") as fh:
            fh.write(text)

    def assert_all_empty(self):
        self.assertEqual(collector.list_records(self.db)[1], 0)
        self.assertEqual(collector.list_file_states(self.db), [])
        self.assertEqual(collector.list_runs(self.db), [])
        self.assertFalse(os.path.exists(self.active))
        self.assertFalse(os.path.exists(self.rotated))
        leftovers = [n for n in os.listdir(self.data) if n != ".gitkeep"]
        self.assertEqual(leftovers, [])

    def test_reset_clears_records_progress_runs_and_both_logs(self):
        self.write(self.rotated, rec(1) + "\n" + rec(2) + "\n")
        self.write(self.active, rec(3) + "\n")
        self.assertEqual(
            collector.collect_once(self.data, self.db)["status"], "ok"
        )
        self.assertEqual(collector.list_records(self.db)[1], 3)
        self.assertEqual(len(collector.list_file_states(self.db)), 2)
        self.assertEqual(len(collector.list_runs(self.db)), 1)

        collector.reset_all(self.db, self.data)
        self.assert_all_empty()
        # 重置后采集空目录：正常空操作，轮次从空开始重新记
        r = collector.collect_once(self.data, self.db)
        self.assertEqual(r["status"], "ok")
        self.assertEqual((r["records_inserted"], r["lines_seen"]), (0, 0))

    def test_reset_interleaved_with_inflight_collect_discards_stale_writeback(self):
        """采集读完日志 -> 重置成功 -> 旧采集不得回写记录/进度/轮次。"""
        self.write(self.active, rec(1) + "\n" + rec(2) + "\n")
        real_read = collector._read_file_chunk
        published = {}

        def read_then_reset(data_dir, info, states):
            chunk = real_read(data_dir, info, states)
            if info["name"] == ACTIVE and "done" not in published:
                published["done"] = True
                # 模拟网页重置在“刚读完日志、尚未写库”时成功返回
                collector.reset_all(self.db, self.data)
                published["reset_epoch"] = collector.current_epoch(self.db)
            return chunk

        with mock.patch.object(collector, "_read_file_chunk", side_effect=read_then_reset):
            result = collector.collect_once(self.data, self.db)

        self.assertEqual(published["reset_epoch"], 1)
        self.assertEqual(result["status"], "superseded")
        self.assertEqual(result["records_inserted"], 0)
        self.assert_all_empty()

    def test_reset_interleaved_then_old_collect_still_no_writeback(self):
        """后台采集线程在重置后完成写库阶段：epoch 复核必须拦住。"""
        self.write(self.active, rec(1) + "\n")
        started = threading.Event()
        release = threading.Event()
        real_read = collector._read_file_chunk

        def blocking_read(data_dir, info, states):
            chunk = real_read(data_dir, info, states)
            if "waited" not in vars(blocking_read):
                blocking_read.waited = True
                started.set()
                release.wait(10)
            return chunk

        out = {}
        with mock.patch.object(collector, "_read_file_chunk", side_effect=blocking_read):
            t = threading.Thread(
                target=lambda: out.setdefault("r", collector.collect_once(self.data, self.db))
            )
            t.start()
            self.assertTrue(started.wait(10))
            collector.reset_all(self.db, self.data)
            release.set()
            t.join(10)
        self.assertFalse(t.is_alive())
        self.assertEqual(out["r"]["status"], "superseded")
        self.assert_all_empty()

    def test_reset_supersedes_inflight_rejected_round_without_run_row(self):
        """采集中途重置后，原本会被拒绝/留痕的轮次也不再留下任何轮次。"""
        self.write(self.active, rec(1) + "\n" + rec(1, 99.0) + "\n")  # 批内冲突
        started = threading.Event()
        release = threading.Event()
        real_read = collector._read_file_chunk

        def blocking_read(data_dir, info, states):
            chunk = real_read(data_dir, info, states)
            if "waited" not in vars(blocking_read):
                blocking_read.waited = True
                started.set()
                release.wait(10)
            return chunk

        out = {}
        with mock.patch.object(collector, "_read_file_chunk", side_effect=blocking_read):
            t = threading.Thread(
                target=lambda: out.setdefault("r", collector.collect_once(self.data, self.db))
            )
            t.start()
            self.assertTrue(started.wait(10))
            collector.reset_all(self.db, self.data)
            release.set()
            t.join(10)
        self.assertFalse(t.is_alive())
        self.assertEqual(out["r"]["status"], "superseded")
        self.assert_all_empty()

    def test_failed_reset_window_also_blocks_inflight_writeback(self):
        """重置尝试过（文件被移走又回滚）后，读取期间的旧采集也不得写回。"""
        self.write(self.active, rec(1) + "\n")
        started = threading.Event()
        release = threading.Event()
        real_read = collector._read_file_chunk

        def blocking_read(data_dir, info, states):
            chunk = real_read(data_dir, info, states)
            if "waited" not in vars(blocking_read):
                blocking_read.waited = True
                started.set()
                release.wait(10)
            return chunk

        out = {}
        with mock.patch.object(collector, "_read_file_chunk", side_effect=blocking_read):
            t = threading.Thread(
                target=lambda: out.setdefault("r", collector.collect_once(self.data, self.db))
            )
            t.start()
            self.assertTrue(started.wait(10))
            with mock.patch.object(
                collector.os, "replace", side_effect=OSError("disk full (simulated)")
            ):
                with self.assertRaises(collector.ResetError):
                    collector.reset_all(self.db, self.data)
            release.set()
            t.join(10)
        self.assertFalse(t.is_alive())
        self.assertEqual(out["r"]["status"], "superseded")
        # 重置失败：日志仍在、记录未被半清空；随后再采集可正常入库
        self.assertTrue(os.path.exists(self.active))
        self.assertEqual(collector.list_records(self.db)[1], 0)
        self.assertEqual(collector.list_runs(self.db), [])
        r = collector.collect_once(self.data, self.db)
        self.assertEqual((r["status"], r["records_inserted"]), ("ok", 1))

    def test_reset_file_failure_keeps_records_and_logs(self):
        """第二份日志移走失败：记录、进度、轮次与两份日志都保持原样。"""
        self.write(self.rotated, rec(1) + "\n")
        self.write(self.active, rec(2) + "\n")
        collector.collect_once(self.data, self.db)
        before_records = collector.list_records(self.db)[1]
        before_states = len(collector.list_file_states(self.db))
        before_runs = len(collector.list_runs(self.db))

        real_replace = os.replace

        def failing_replace(src, dst):
            if src.endswith(ACTIVE):  # 第一份已移走，第二份失败
                raise OSError("disk full (simulated)")
            return real_replace(src, dst)

        with mock.patch.object(collector.os, "replace", side_effect=failing_replace):
            with self.assertRaises(collector.ResetError):
                collector.reset_all(self.db, self.data)

        self.assertEqual(collector.list_records(self.db)[1], before_records)
        self.assertEqual(len(collector.list_file_states(self.db)), before_states)
        self.assertEqual(len(collector.list_runs(self.db)), before_runs)
        self.assertTrue(os.path.exists(self.active))
        self.assertTrue(os.path.exists(self.rotated))
        self.assertFalse(
            [n for n in os.listdir(self.data) if n.startswith(".reset-")]
        )
        # 原有日志仍可继续正常采集（半行/轮转规则不受影响）
        self.write(self.active, rec(3) + "\n")
        r = collector.collect_once(self.data, self.db)
        self.assertEqual(r["status"], "ok", r["error"])
        self.assertEqual(collector.list_records(self.db)[1], before_records + 1)

    def test_reset_db_failure_restores_logs_and_records(self):
        """清库事务失败：两份日志被移回，数据库原有记录仍可用。"""
        self.write(self.rotated, rec(1) + "\n")
        self.write(self.active, rec(2) + "\n")
        collector.collect_once(self.data, self.db)
        before_records = collector.list_records(self.db)[1]
        before_offset = {
            s["inode"]: s["offset"] for s in collector.list_file_states(self.db)
        }

        def failing_clear(conn):
            raise sqlite3.OperationalError("database is locked (simulated)")

        with mock.patch.object(collector, "_clear_all_tables", failing_clear):
            with self.assertRaises(collector.ResetError):
                collector.reset_all(self.db, self.data)

        self.assertEqual(collector.list_records(self.db)[1], before_records)
        self.assertEqual(
            {s["inode"]: s["offset"] for s in collector.list_file_states(self.db)},
            before_offset,
        )
        self.assertTrue(os.path.exists(self.active))
        self.assertTrue(os.path.exists(self.rotated))
        # 日志内容原样（未被截断/丢行），可继续增量采集
        with open(self.rotated, encoding="utf-8") as fh:
            self.assertEqual(fh.read(), rec(1) + "\n")
        with open(self.active, encoding="utf-8") as fh:
            self.assertEqual(fh.read(), rec(2) + "\n")
        self.write(self.active, rec(3) + "\n")
        self.assertEqual(collector.collect_once(self.data, self.db)["records_inserted"], 1)

    def test_recover_preparing_crash_rolls_back_to_full_before_state(self):
        """崩溃在“日志已移走、库未清空”：启动恢复把日志移回，记录完整。"""
        self.write(self.rotated, rec(1) + "\n")
        self.write(self.active, rec(2) + "\n")
        collector.collect_once(self.data, self.db)
        before_runs = len(collector.list_runs(self.db))

        staging = os.path.join(self.data, ".reset-1-crash")
        os.makedirs(staging)
        os.replace(self.rotated, os.path.join(staging, ROTATED))
        os.replace(self.active, os.path.join(staging, ACTIVE))
        with collector.get_db(self.db) as conn:
            collector._write_reset_state(
                conn,
                epoch=0,
                phase="preparing",
                data_dir=self.data,
                staging=staging,
            )

        collector.recover_incomplete_reset(self.db)
        self.assertTrue(os.path.exists(self.active))
        self.assertTrue(os.path.exists(self.rotated))
        self.assertFalse(os.path.isdir(staging))
        self.assertEqual(collector.list_records(self.db)[1], 2)
        self.assertEqual(len(collector.list_runs(self.db)), before_runs)
        # 恢复后采集仍按原进度续接（不重记）
        r = collector.collect_once(self.data, self.db)
        self.assertEqual((r["status"], r["records_inserted"]), ("ok", 0))

    def test_recover_committed_crash_keeps_empty_state(self):
        """崩溃在“库已清空、暂存未删”：启动恢复清理暂存，保持空状态。"""
        self.write(self.active, rec(1) + "\n")
        collector.collect_once(self.data, self.db)
        staging = os.path.join(self.data, ".reset-1-crash")
        os.makedirs(staging)
        os.replace(self.active, os.path.join(staging, ACTIVE))
        conn = collector.get_db(self.db)
        try:
            with collector._immediate_tx(conn):
                collector._clear_all_tables(conn)
                collector._write_reset_state(
                    conn, epoch=1, phase="committed", staging=staging
                )
        finally:
            conn.close()
        collector.recover_incomplete_reset(self.db)
        self.assertFalse(os.path.isdir(staging))
        self.assert_all_empty()

    def test_repeated_reset_is_stable(self):
        self.write(self.active, rec(1) + "\n")
        collector.collect_once(self.data, self.db)
        collector.reset_all(self.db, self.data)
        for _ in range(3):
            collector.reset_all(self.db, self.data)
            self.assert_all_empty()
        self.assertGreaterEqual(collector.current_epoch(self.db), 4)

    def test_collect_after_interrupted_reset_does_not_create_half_state(self):
        """重置 prepare 崩溃后直接采集：先恢复日志再采集，结果等同完整旧状态。"""
        self.write(self.active, rec(1) + "\n" + rec(2) + "\n")
        collector.collect_once(self.data, self.db)
        staging = os.path.join(self.data, ".reset-1-crash")
        os.makedirs(staging)
        os.replace(self.active, os.path.join(staging, ACTIVE))
        with collector.get_db(self.db) as conn:
            collector._write_reset_state(conn, phase="preparing", staging=staging)
        self.write(os.path.join(staging, ACTIVE), rec(3) + "\n")
        r = collector.collect_once(self.data, self.db)
        self.assertEqual(r["status"], "ok", r["error"])
        self.assertEqual(r["records_inserted"], 1)
        self.assertEqual(collector.list_records(self.db)[1], 3)
        self.assertTrue(os.path.exists(self.active))


class WebTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.data = os.path.join(self.tmp.name, "data")
        self.db = os.path.join(self.tmp.name, "db", "web.db")
        os.makedirs(self.data, exist_ok=True)
        os.environ["DATA_DIR"] = self.data
        os.environ["DB_PATH"] = self.db
        os.environ["ENABLE_DEMO_RESET"] = "1"
        # 强制重新加载 app 模块以读取环境变量
        for mod in [m for m in list(sys.modules) if m == "app" or m.startswith("app.")]:
            del sys.modules[mod]
        import app as flask_app  # noqa: WPS433
        self.app_module = flask_app
        self.client = flask_app.create_app().test_client()

    def tearDown(self):
        self.tmp.cleanup()

    def test_full_web_flow_and_csv(self):
        active = os.path.join(self.data, ACTIVE)
        with open(active, "w", encoding="utf-8") as fh:
            fh.write(rec(1, 25.123) + "\n" + rec(2, 26.456) + "\n")

        resp = self.client.post("/collect", follow_redirects=True)
        self.assertEqual(resp.status_code, 200)
        self.assertIn("采集成功", resp.get_data(as_text=True))

        # 重复点击
        self.client.post("/collect")
        page = self.client.get("/").get_data(as_text=True)
        self.assertIn("入库记录总数：<b>2</b>", page)

        csv_resp = self.client.get("/records.csv")
        self.assertEqual(csv_resp.status_code, 200)
        self.assertIn("attachment", csv_resp.headers["Content-Disposition"])
        text = csv_resp.get_data(as_text=True)
        self.assertTrue(text.startswith("﻿seq,ts_utc,temp"))
        lines = [ln for ln in text.splitlines() if ln]
        self.assertEqual(len(lines), 3)  # 表头 + 2 行，且数据按 seq 正序
        self.assertTrue(lines[1].startswith("1,"))
        self.assertTrue(lines[2].startswith("2,"))

        # 非法行 -> 页面出现 rejected 提示且记录数不变
        with open(active, "a", encoding="utf-8") as fh:
            fh.write(json.dumps({"seq": 3, "ts": "2026-11-15T08:02:00Z",
                                 "temp": float("inf")}) + "\n")
        resp = self.client.post("/collect", follow_redirects=True)
        self.assertIn("整轮拒绝", resp.get_data(as_text=True))
        self.assertIn("入库记录总数：<b>2</b>", self.client.get("/").get_data(as_text=True))

        # 演示重置
        self.client.post("/demo/reset", follow_redirects=True)
        self.assertFalse(os.path.exists(active))
        self.assertEqual(collector.list_records(self.db)[1], 0)
        self.assertEqual(collector.list_runs(self.db), [])

    def test_demo_reset_button_reports_success_and_empties_page(self):
        active = os.path.join(self.data, ACTIVE)
        rotated = os.path.join(self.data, ROTATED)
        with open(rotated, "w", encoding="utf-8") as fh:
            fh.write(rec(1) + "\n")
        with open(active, "w", encoding="utf-8") as fh:
            fh.write(rec(2) + "\n")
        self.client.post("/collect")
        self.assertIn("入库记录总数：<b>2</b>", self.client.get("/").get_data(as_text=True))

        resp = self.client.post("/demo/reset", follow_redirects=True)
        html = resp.get_data(as_text=True)
        self.assertIn("已同时清空", html)
        self.assertIn("入库记录总数：<b>0</b>", html)
        self.assertIn("目录中暂无 active.ndjson", html)
        self.assertIn("尚无采集进度", html)
        self.assertIn("还没有执行过采集", html)
        self.assertFalse(os.path.exists(active))
        self.assertFalse(os.path.exists(rotated))
        # 重复重置也保持空状态、可继续采集
        resp = self.client.post("/demo/reset", follow_redirects=True)
        self.assertEqual(resp.status_code, 200)
        with open(active, "w", encoding="utf-8") as fh:
            fh.write(rec(1) + "\n")
        self.client.post("/collect")
        self.assertIn("入库记录总数：<b>1</b>", self.client.get("/").get_data(as_text=True))

    def test_demo_reset_failure_reports_and_keeps_state(self):
        active = os.path.join(self.data, ACTIVE)
        with open(active, "w", encoding="utf-8") as fh:
            fh.write(rec(1) + "\n")
        self.client.post("/collect")

        def failing_clear(conn):
            raise sqlite3.OperationalError("database is locked (simulated)")

        with mock.patch.object(
            self.app_module.collector, "_clear_all_tables", failing_clear
        ):
            resp = self.client.post("/demo/reset", follow_redirects=True)
        html = resp.get_data(as_text=True)
        self.assertIn("演示重置失败", html)
        self.assertIn("入库记录总数：<b>1</b>", html)
        self.assertTrue(os.path.exists(active))
        self.assertEqual(collector.list_records(self.db)[1], 1)

    def test_restart_page_sees_only_full_or_empty_state(self):
        """模拟“进程重启”：新建 app，页面只会看到完整旧状态或空状态。"""
        active = os.path.join(self.data, ACTIVE)
        rotated = os.path.join(self.data, ROTATED)
        with open(rotated, "w", encoding="utf-8") as fh:
            fh.write(rec(1) + "\n")
        with open(active, "w", encoding="utf-8") as fh:
            fh.write(rec(2) + "\n")
        self.client.post("/collect")

        # 崩溃在 prepare 阶段（日志已移走、库未清空）
        staging = os.path.join(self.data, ".reset-1-crash")
        os.makedirs(staging)
        os.replace(rotated, os.path.join(staging, ROTATED))
        os.replace(active, os.path.join(staging, ACTIVE))
        with collector.get_db(self.db) as conn:
            collector._write_reset_state(
                conn, phase="preparing", data_dir=self.data, staging=staging
            )

        # 新建 app 模拟重启（create_app 会执行启动恢复）
        client2 = self.app_module.create_app().test_client()
        html = client2.get("/").get_data(as_text=True)
        self.assertIn("入库记录总数：<b>2</b>", html)
        self.assertIn("当前大小", html)
        self.assertTrue(os.path.exists(active))
        self.assertTrue(os.path.exists(rotated))

    def test_healthz(self):
        self.assertEqual(self.client.get("/healthz").get_json(), {"status": "ok"})

    def test_bad_page_param_does_not_500(self):
        resp = self.client.get("/?page=abc")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("发酵罐", resp.get_data(as_text=True))

    def test_empty_csv_has_header_only(self):
        resp = self.client.get("/records.csv")
        self.assertEqual(resp.status_code, 200)
        text = resp.get_data(as_text=True)
        self.assertTrue(text.startswith("﻿seq,ts_utc,temp"))
        self.assertEqual(len([ln for ln in text.splitlines() if ln]), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
