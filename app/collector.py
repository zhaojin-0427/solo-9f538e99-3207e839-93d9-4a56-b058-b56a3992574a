# -*- coding: utf-8 -*-
"""发酵罐 NDJSON 温度日志采集核心。

设计要点
========
1. 设备把 ``active.ndjson`` 重命名为 ``active.ndjson.1`` 后新建同名文件，
   目录最多保留这两份，归档不删除、不覆盖。
2. 采集进度按 **inode** 记录（而不是路径）：轮转后旧文件换了路径但 inode 不变，
   仍可从上次的字节偏移继续读；新文件 inode 未见过，从头读。
3. 每次采集只处理“上次成功后新增的字节”，且只消费到最后一个换行符；
   换行符之后的半行保留在文件里，偏移不前移，下一轮再读。
4. 一轮采集 = 一个事务：任何一条完整记录非法（JSON 非法 / 字段不合法 /
   序号不连续递增 / 同序号内容冲突），整轮拒绝，已入库记录与采集进度都不变。
5. 重复点击 / 服务重启：进度已持久化在 SQLite，没有新字节时即为空操作，
   不重记、不漏记。
6. 演示重置与采集互斥：重置先把两份日志原子移入暂存目录，再在一个事务里
   清空记录/进度/轮次并递增 epoch；采集在写事务内复核 reset token
   （epoch + updated_at），重置（含失败回滚）后旧采集不会回写。重置中途
   崩溃由 ``reset_state`` journal 在启动/采集前自动回滚或收尾，页面只会看到
   “重置前完整状态”或“重置后空状态”。
"""

from __future__ import annotations

import json
import math
import os
import shutil
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterator, List, Optional, Tuple

ACTIVE_NAME = "active.ndjson"
ROTATED_NAME = "active.ndjson.1"
# 固定的采集顺序：先旧文件后当前文件，便于排查；最终仍按序号归位校验。
LOG_FILES = (ROTATED_NAME, ACTIVE_NAME)

RESET_STAGING_PREFIX = ".reset-"
RESET_JOURNAL_ID = 1
# 同一进程内的采集 / 重置 / 页面快照互斥。RLock：Web 层持有后再进入内部函数不会死锁。
_OP_LOCK = threading.RLock()


class RejectRound(Exception):
    """整轮采集被拒绝的业务异常，message 为可直接展示的中文原因。"""


class ResetError(Exception):
    """演示重置失败；调用方应提示用户，原有记录与日志应保持可用。"""


class ResetSuperseded(Exception):
    """采集读取日志期间发生了已提交的重置，本轮结果必须整体丢弃。"""


@contextmanager
def operation_lock() -> Iterator[None]:
    """供 Web 页面读取快照时使用：不会看到重置进行到一半的中间状态。"""
    with _OP_LOCK:
        yield


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


# --------------------------------------------------------------------------- #
# 数据库
# --------------------------------------------------------------------------- #
def get_db(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    # 自动提交模式：事务由 _immediate_tx 显式控制，避免隐式 BEGIN 让
    # “复核 reset token”与写库落在不同事务里。
    conn = sqlite3.connect(db_path, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@contextmanager
def _immediate_tx(conn: sqlite3.Connection) -> Iterator[None]:
    """显式写事务：进入即取写锁，异常回滚，正常提交。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
    except BaseException:
        conn.rollback()
        raise
    else:
        conn.commit()


def init_db(db_path: str) -> None:
    conn = get_db(db_path)
    try:
        with _immediate_tx(conn):
            for statement in _SCHEMA_STATEMENTS:
                conn.execute(statement)
            conn.execute(
                "INSERT OR IGNORE INTO reset_state(id, epoch, phase, updated_at)"
                " VALUES (?, 0, 'idle', ?)",
                (RESET_JOURNAL_ID, utc_now_iso()),
            )
    finally:
        conn.close()


_SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS records (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        seq          INTEGER NOT NULL UNIQUE,
        ts           TEXT    NOT NULL,
        temp         REAL    NOT NULL,
        raw          TEXT    NOT NULL,
        source_path  TEXT    NOT NULL,
        source_inode INTEGER NOT NULL,
        collected_at TEXT    NOT NULL
    )
    """,
    """
    -- 采集进度按 inode 记录，path 仅作展示（轮转后路径会变）。
    CREATE TABLE IF NOT EXISTS file_state (
        inode      INTEGER PRIMARY KEY,
        path       TEXT    NOT NULL,
        offset     INTEGER NOT NULL,
        file_size  INTEGER NOT NULL,
        updated_at TEXT    NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS collect_runs (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        started_at       TEXT    NOT NULL,
        finished_at      TEXT    NOT NULL,
        status           TEXT    NOT NULL,           -- ok / rejected / error
        files_scanned    INTEGER NOT NULL DEFAULT 0,
        lines_seen       INTEGER NOT NULL DEFAULT 0, -- 本轮读到的完整行数
        records_inserted INTEGER NOT NULL DEFAULT 0,
        duplicates       INTEGER NOT NULL DEFAULT 0, -- 已存在且内容一致
        half_line_bytes  INTEGER NOT NULL DEFAULT 0, -- 留在文件里的半行字节
        detail           TEXT    NOT NULL DEFAULT '',
        error            TEXT    NOT NULL DEFAULT ''
    )
    """,
    """
    -- 演示重置日志。epoch 是重置代数：采集开始时记下 (epoch, updated_at)，
    -- 写库前在同一事务里复核；若期间发生重置，则本轮整体丢弃，绝不回写。
    -- phase: idle / preparing / committed。
    CREATE TABLE IF NOT EXISTS reset_state (
        id         INTEGER PRIMARY KEY CHECK (id = 1),
        epoch      INTEGER NOT NULL DEFAULT 0,
        phase      TEXT    NOT NULL DEFAULT 'idle',
        data_dir   TEXT    NOT NULL DEFAULT '',
        staging    TEXT    NOT NULL DEFAULT '',
        updated_at TEXT    NOT NULL DEFAULT ''
    )
    """,
)


def _read_reset_state(conn: sqlite3.Connection) -> sqlite3.Row:
    return conn.execute(
        "SELECT * FROM reset_state WHERE id = ?", (RESET_JOURNAL_ID,)
    ).fetchone()


def current_reset_token(db_path: str) -> Tuple[int, str]:
    """(epoch, updated_at)：epoch 标识“已提交的重置”，updated_at 还覆盖失败的
    重置尝试（文件可能被移动过又回滚），采集遇到任一变化都放弃写回。"""
    conn = get_db(db_path)
    try:
        row = _read_reset_state(conn)
        if row is None:
            return 0, ""
        return int(row["epoch"]), row["updated_at"]
    finally:
        conn.close()


def current_epoch(db_path: str) -> int:
    return current_reset_token(db_path)[0]


def _write_reset_state(
    conn: sqlite3.Connection,
    *,
    epoch: Optional[int] = None,
    phase: Optional[str] = None,
    data_dir: Optional[str] = None,
    staging: Optional[str] = None,
) -> None:
    row = _read_reset_state(conn)
    conn.execute(
        "UPDATE reset_state SET epoch = ?, phase = ?, data_dir = ?, staging = ?,"
        " updated_at = ? WHERE id = ?",
        (
            int(row["epoch"]) if epoch is None else int(epoch),
            row["phase"] if phase is None else phase,
            row["data_dir"] if data_dir is None else data_dir,
            row["staging"] if staging is None else staging,
            utc_now_iso(),
            RESET_JOURNAL_ID,
        ),
    )


def _clear_all_tables(conn: sqlite3.Connection) -> None:
    conn.execute("DELETE FROM records")
    conn.execute("DELETE FROM file_state")
    conn.execute("DELETE FROM collect_runs")


def _remove_tree(path: str) -> None:
    if os.path.isdir(path):
        shutil.rmtree(path)
    elif os.path.exists(path):
        os.remove(path)


def _restore_staged_files(staging: str, data_dir: str) -> None:
    """把暂存目录里的日志移回原位置（失败重置 / 崩溃回滚）。

    若设备在重置窗口内已新建同名文件，绝不覆盖设备新数据：把重置前的旧文件
    改名保留在数据目录中（不参与采集），保证记录与设备日志都不丢。
    """
    if not os.path.isdir(staging):
        return
    for name in LOG_FILES:
        staged = os.path.join(staging, name)
        if not os.path.exists(staged):
            continue
        dest = os.path.join(data_dir, name)
        if os.path.exists(dest):
            keep = os.path.join(
                data_dir, f"{name}{RESET_STAGING_PREFIX}kept-{uuid.uuid4().hex[:8]}"
            )
            os.replace(staged, keep)
        else:
            os.replace(staged, dest)


def recover_incomplete_reset(db_path: str) -> None:
    """完成或回滚上次中途中断的演示重置。

    - preparing：数据库尚未清空，把暂存日志移回原位置 -> 回到重置前的完整状态；
    - committed：数据库已清空，清掉暂存目录 -> 保持重置后的空状态。
    两种结果都只会是“完整旧状态”或“空状态”，不会留下半清空状态。
    """
    init_db(db_path)
    with _OP_LOCK:
        conn = get_db(db_path)
        try:
            row = _read_reset_state(conn)
            if row is None or row["phase"] == "idle":
                return
            phase, staging = row["phase"], row["staging"]
            data_dir = row["data_dir"] or os.path.dirname(os.path.abspath(staging))
            try:
                if phase == "preparing":
                    # 日志已被移走而库未清空：必须移回，否则页面会看到半清空状态。
                    _restore_staged_files(staging, data_dir)
                    _remove_tree(staging)
                else:
                    # committed：库已是空状态，暂存目录只是残留，清理失败不影响一致性。
                    try:
                        _remove_tree(staging)
                    except OSError:
                        pass
            except OSError:
                # 保持 journal 原样，下次启动/下一次操作继续尝试，期间不做半清空写入。
                raise
            with _immediate_tx(conn):
                _write_reset_state(conn, phase="idle", staging="")
        finally:
            conn.close()


def reset_all(db_path: str, data_dir: str) -> dict:
    """演示用：原子地清空数据库中的记录/进度/轮次与挂载目录中的两份日志。

    顺序：先把两份日志移入同文件系统的暂存目录（任一步失败则全部移回、数据库
    原样保留）；两份都移走后，才在一个数据库事务里清表并递增 epoch。因此：
    - 采集在重置提交前读到的旧日志，因 epoch 变化无法再写回；
    - 中途崩溃时，recover_incomplete_reset 会按 journal 回滚或收尾；
    - 成功返回后，记录、进度、轮次与两份日志同时为空。
    """
    data_dir = os.path.abspath(data_dir)
    with _OP_LOCK:
        init_db(db_path)
        recover_incomplete_reset(db_path)
        conn = get_db(db_path)
        try:
            row = _read_reset_state(conn)
            old_epoch = int(row["epoch"])
            staging = os.path.join(
                data_dir, f"{RESET_STAGING_PREFIX}{old_epoch + 1}-{uuid.uuid4().hex}"
            )
            committed = False
            try:
                os.makedirs(staging, exist_ok=True)
                # 1) 先记日志：prepare 阶段只表示“可能正在移动”，数据库仍未清空。
                with _immediate_tx(conn):
                    _write_reset_state(
                        conn,
                        epoch=old_epoch,
                        phase="preparing",
                        data_dir=data_dir,
                        staging=staging,
                    )
                # 2) 两份日志都成功移入暂存目录后，才允许清库。
                for name in LOG_FILES:
                    src = os.path.join(data_dir, name)
                    if os.path.exists(src):
                        os.replace(src, os.path.join(staging, name))
                # 3) 清表与 epoch 递增在同一事务提交。committed 与空库同时生效。
                with _immediate_tx(conn):
                    _clear_all_tables(conn)
                    _write_reset_state(
                        conn, epoch=old_epoch + 1, phase="committed", staging=staging
                    )
                committed = True
            except Exception as exc:  # noqa: BLE001 - 需要按文件/写库失败分类回滚
                # 极端情况：清库事务实际已提交、但提交后才报错。此时不能把日志
                # 移回去（否则又成了半清空），按已提交收尾。
                try:
                    row = _read_reset_state(conn)
                except Exception:  # noqa: BLE001
                    row = None
                already_committed = (
                    row is not None
                    and row["phase"] == "committed"
                    and int(row["epoch"]) == old_epoch + 1
                )
                if not already_committed:
                    try:
                        _restore_staged_files(staging, data_dir)
                        _remove_tree(staging)
                        with _immediate_tx(conn):
                            _write_reset_state(
                                conn, epoch=old_epoch, phase="idle", staging=""
                            )
                    except Exception:  # noqa: BLE001 - 回滚也失败，留给启动恢复
                        pass
                    raise ResetError(
                        f"演示重置失败，原有记录与日志保持不变：{exc}"
                    ) from exc
        finally:
            conn.close()
        # 4) 数据库已进入空状态；清理暂存文件失败只会留下不可见的暂存目录。
        try:
            _remove_tree(staging)
            final_conn = get_db(db_path)
            try:
                with _immediate_tx(final_conn):
                    _write_reset_state(final_conn, phase="idle", staging="")
            finally:
                final_conn.close()
        except OSError:
            pass
        assert committed  # 未提交的失败路径已在上面抛出 ResetError
        return {"epoch": old_epoch + 1}


# --------------------------------------------------------------------------- #
# 单行校验
# --------------------------------------------------------------------------- #
def _parse_utc_ts(value) -> Optional[datetime]:
    """要求带时区且 UTC 偏移为 0 的 ISO-8601 字符串（如 2026-11-15T08:30:00Z）。"""
    if not isinstance(value, str) or not value:
        return None
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None or dt.utcoffset() != timedelta(0):
        return None
    return dt


def validate_line(raw_line: str) -> Tuple[int, str, float]:
    """校验一条完整记录，返回 (seq, ts 原文, temp)；非法则抛 RejectRound。"""
    try:
        obj = json.loads(raw_line)
    except json.JSONDecodeError as exc:
        raise RejectRound(f"非法 JSON 行：{exc.msg}；内容：{raw_line[:120]!r}")
    if not isinstance(obj, dict):
        raise RejectRound(f"记录必须是 JSON 对象：{raw_line[:120]!r}")

    seq = obj.get("seq")
    if isinstance(seq, bool) or not isinstance(seq, int):
        raise RejectRound(f"序号 seq 必须是整数：{raw_line[:120]!r}")

    ts = obj.get("ts")
    if _parse_utc_ts(ts) is None:
        raise RejectRound(f"时刻 ts 必须是 UTC 的 ISO-8601 字符串（结尾 Z）：{raw_line[:120]!r}")

    temp = obj.get("temp")
    if isinstance(temp, bool) or not isinstance(temp, (int, float)) or not math.isfinite(temp):
        raise RejectRound(f"温度 temp 必须是有限数值：{raw_line[:120]!r}")

    return seq, ts, float(temp)


# --------------------------------------------------------------------------- #
# 文件扫描与增量读取
# --------------------------------------------------------------------------- #
@dataclass
class FileChunk:
    path: str
    name: str
    inode: int
    size: int
    start_offset: int          # 本轮读取前的偏移
    complete_text: str         # 本轮新增且以换行结束的文本
    new_offset: int            # 本轮成功后应持久化的偏移
    half_line_bytes: int       # 未以换行结束、留给下轮的字节数
    complete_lines: List[str] = field(default_factory=list)


def _scan_files(data_dir: str) -> List[dict]:
    found = []
    for name in LOG_FILES:
        path = os.path.join(data_dir, name)
        try:
            st = os.stat(path)
        except FileNotFoundError:
            continue
        if not os.path.isfile(path):
            continue
        found.append({"path": path, "name": name, "inode": st.st_ino, "size": st.st_size})
    return found


def _read_file_chunk(data_dir: str, info: dict, states: Dict[int, sqlite3.Row]) -> FileChunk:
    """按 inode 进度读取某文件的新增字节，只切出完整行。"""
    path, inode, size = info["path"], info["inode"], info["size"]
    state = states.get(inode)
    start = state["offset"] if state else 0
    if state is not None and state["path"] != path:
        # inode 被文件系统复用：同 inode 出现在新路径上，旧进度不再适用，从头读。
        # （设备只 rename 不删除，正常不会发生；这是防御性处理，靠 seq 唯一约束兜底。）
        start = 0
    if size < start:
        # 防御：文件被截断过，从头重读（序号唯一约束 + 内容比对保证不重记）。
        start = 0

    with open(path, "rb") as fh:
        fh.seek(start)
        raw = fh.read()  # 读到读取时刻的 EOF，读取间若又追加也不影响本轮切分

    last_nl = raw.rfind(b"\n")
    if last_nl < 0:
        complete_bytes, tail = b"", raw
    else:
        complete_bytes, tail = raw[: last_nl + 1], raw[last_nl + 1 :]

    new_offset = start + len(complete_bytes)
    try:
        text = complete_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RejectRound(f"日志包含非 UTF-8 字节，整轮拒绝：{exc}") from exc
    # text 保证以 \n 结尾；空行（含 \n\n）会保留为空串，
    # 后续 validate_line 解析空串时按非法 JSON 处理 -> 整轮拒绝。
    lines = text.split("\n")[:-1] if text else []
    return FileChunk(
        path=path,
        name=info["name"],
        inode=inode,
        size=size,
        start_offset=start,
        complete_text=text,
        new_offset=new_offset,
        half_line_bytes=len(tail),
        complete_lines=lines,
    )


# --------------------------------------------------------------------------- #
# 一轮采集
# --------------------------------------------------------------------------- #
def collect_once(data_dir: str, db_path: str) -> dict:
    started = utc_now_iso()
    result = {
        "status": "error",
        "files": [],
        "lines_seen": 0,
        "records_inserted": 0,
        "duplicates": 0,
        "half_line_bytes": 0,
        "error": "",
        "started_at": started,
        "finished_at": None,
    }
    # 上一次重置若中途崩溃，先按 journal 收尾，避免在“半清空”状态上采集。
    # 恢复失败时本轮不采集、也不留痕，直接提示失败（不污染现有状态）。
    try:
        with _OP_LOCK:
            recover_incomplete_reset(db_path)
            reset_token = current_reset_token(db_path)
    except Exception as exc:  # noqa: BLE001
        result["error"] = f"{type(exc).__name__}: {exc}"
        result["finished_at"] = utc_now_iso()
        return result

    try:
        conn = get_db(db_path)
        try:
            # 读“文件清单 + 进度 + 已有记录”这一段与重置互斥：重置提交后
            # 不可能再拿到旧日志清单，避免旧快照在本轮事务里被误判为可写。
            # 读文件字节的阶段不持锁，允许设备/重置并发，靠 reset token 复核兜底。
            with _OP_LOCK:
                states = {
                    row["inode"]: row for row in conn.execute("SELECT * FROM file_state")
                }
                files = _scan_files(data_dir)
                existing_rows = conn.execute("SELECT seq, raw FROM records").fetchall()
                existing = {row["seq"]: row["raw"] for row in existing_rows}
            chunks: List[FileChunk] = [_read_file_chunk(data_dir, info, states) for info in files]
            result["files"] = [
                {
                    "name": c.name,
                    "path": c.path,
                    "inode": c.inode,
                    "size": c.size,
                    "start_offset": c.start_offset,
                    "new_offset": c.new_offset,
                    "complete_lines": len(c.complete_lines),
                    "half_line_bytes": c.half_line_bytes,
                }
                for c in chunks
            ]
            result["half_line_bytes"] = sum(c.half_line_bytes for c in chunks)

            # 1) 逐行解析校验（任何一行非法 -> 整轮拒绝）
            parsed: List[dict] = []
            for chunk in chunks:
                for raw_line in chunk.complete_lines:
                    seq, ts, temp = validate_line(raw_line)
                    parsed.append(
                        {
                            "seq": seq,
                            "ts": ts,
                            "temp": temp,
                            "raw": raw_line,
                            "source_path": chunk.name,
                            "source_inode": chunk.inode,
                        }
                    )
            result["lines_seen"] = len(parsed)

            # 2) 批内同序号冲突检测
            by_seq: Dict[int, dict] = {}
            for rec in parsed:
                old = by_seq.get(rec["seq"])
                if old is not None and old["raw"] != rec["raw"]:
                    raise RejectRound(
                        f"同一序号 {rec['seq']} 出现不同内容，整轮拒绝："
                        f"{old['raw'][:80]!r} != {rec['raw'][:80]!r}"
                    )
                by_seq.setdefault(rec["seq"], rec)

            # 3) 与已入库记录比对 + 序号连续性校验
            for seq, rec in by_seq.items():
                if seq in existing and existing[seq] != rec["raw"]:
                    raise RejectRound(
                        f"序号 {seq} 已入库但内容不同，整轮拒绝："
                        f"库内={existing[seq][:80]!r} 新行={rec['raw'][:80]!r}"
                    )

            last_row = conn.execute("SELECT MAX(seq) AS m FROM records").fetchone()
            last_seq = last_row["m"] if last_row is not None else None
            new_seqs = sorted(s for s in by_seq if s not in existing)
            if new_seqs:
                expected_start = (last_seq + 1) if last_seq is not None else new_seqs[0]
                expected = list(range(expected_start, expected_start + len(new_seqs)))
                if new_seqs != expected:
                    raise RejectRound(
                        "序号未按 +1 连续递增，整轮拒绝："
                        f"库内最大序号={last_seq}，本轮新序号={new_seqs}，期望={expected}"
                    )

            # 4) 全部通过 -> 在同一事务内复核 reset token 后写记录 + 推进进度。
            #    若重置在此期间已提交，本轮整体丢弃，绝不回写旧记录。
            duplicates = len(by_seq) - len(new_seqs)
            result.update(
                status="ok",
                records_inserted=len(new_seqs),
                duplicates=duplicates,
                finished_at=utc_now_iso(),
            )
            with _OP_LOCK:
                with _immediate_tx(conn):
                    _guard_reset_token(conn, reset_token)
                    for seq in new_seqs:
                        rec = by_seq[seq]
                        conn.execute(
                            "INSERT INTO records(seq, ts, temp, raw, source_path, source_inode, collected_at)"
                            " VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (
                                seq,
                                rec["ts"],
                                rec["temp"],
                                rec["raw"],
                                rec["source_path"],
                                rec["source_inode"],
                                started,
                            ),
                        )
                    live_inodes = [c.inode for c in chunks]
                    for chunk in chunks:
                        conn.execute(
                            "INSERT INTO file_state(inode, path, offset, file_size, updated_at)"
                            " VALUES (?, ?, ?, ?, ?)"
                            " ON CONFLICT(inode) DO UPDATE SET"
                            " path=excluded.path, offset=excluded.offset,"
                            " file_size=excluded.file_size, updated_at=excluded.updated_at",
                            (chunk.inode, chunk.path, chunk.new_offset, chunk.size, started),
                        )
                    # 清理已不在两份日志中的旧 inode 进度（多次轮转后的历史文件）
                    if live_inodes:
                        placeholders = ",".join("?" for _ in live_inodes)
                        conn.execute(
                            f"DELETE FROM file_state WHERE inode NOT IN ({placeholders})",
                            live_inodes,
                        )
                    _log_run(conn, result)
            return result
        finally:
            conn.close()
    except ResetSuperseded:
        # 重置先提交：不写记录、不写进度，也不留下本轮轮次。
        result["status"] = "superseded"
        result["files"] = []
        result["lines_seen"] = 0
        result["records_inserted"] = 0
        result["duplicates"] = 0
        result["half_line_bytes"] = 0
        result["error"] = "读取日志期间演示重置已生效，本轮采集结果已整体丢弃。"
        result["finished_at"] = utc_now_iso()
        return result
    except RejectRound as exc:
        result["status"] = "rejected"
        result["error"] = str(exc)
    except Exception as exc:  # noqa: BLE001 - 任何意外都不能破坏进度
        result["status"] = "error"
        result["error"] = f"{type(exc).__name__}: {exc}"

    result["finished_at"] = utc_now_iso()
    # 拒绝/出错的轮次也留痕，但同样必须在 reset token 未变时写入；
    # 若期间重置已提交，则整条轮次也不生成，保证重置成功后轮次为空。
    try:
        with _OP_LOCK:
            log_conn = get_db(db_path)
            try:
                with _immediate_tx(log_conn):
                    _guard_reset_token(log_conn, reset_token)
                    _log_run(log_conn, result)
            finally:
                log_conn.close()
    except ResetSuperseded:
        result["status"] = "superseded"
        result["error"] = "读取日志期间演示重置已生效，本轮采集结果已整体丢弃。"
    except Exception as exc:  # noqa: BLE001 - 留痕失败不改变本轮采集结论
        result["status"] = "error"
        result["error"] = f"{result['error']}; 轮次留痕失败：{type(exc).__name__}: {exc}"
    return result


def _guard_reset_token(conn: sqlite3.Connection, token: Tuple[int, str]) -> None:
    """在持锁的写事务中确认没有发生过重置；否则抛 ResetSuperseded。"""
    row = _read_reset_state(conn)
    if row is None:
        raise ResetSuperseded()
    if (int(row["epoch"]), row["updated_at"]) != (int(token[0]), token[1]):
        raise ResetSuperseded()


def _log_run(conn: sqlite3.Connection, result: dict) -> None:
    conn.execute(
        "INSERT INTO collect_runs(started_at, finished_at, status, files_scanned,"
        " lines_seen, records_inserted, duplicates, half_line_bytes, detail, error)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            result["started_at"],
            result["finished_at"] or utc_now_iso(),
            result["status"],
            len(result["files"]),
            result["lines_seen"],
            result["records_inserted"],
            result["duplicates"],
            result["half_line_bytes"],
            json.dumps(result["files"], ensure_ascii=False),
            result["error"],
        ),
    )


def list_records(db_path: str, limit: int = 200, offset: int = 0) -> Tuple[List[sqlite3.Row], int]:
    conn = get_db(db_path)
    try:
        total = conn.execute("SELECT COUNT(*) AS c FROM records").fetchone()["c"]
        rows = conn.execute(
            "SELECT * FROM records ORDER BY seq DESC LIMIT ? OFFSET ?", (limit, offset)
        ).fetchall()
        return rows, total
    finally:
        conn.close()


def list_runs(db_path: str, limit: int = 50) -> List[sqlite3.Row]:
    conn = get_db(db_path)
    try:
        return conn.execute(
            "SELECT * FROM collect_runs ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
    finally:
        conn.close()


def list_file_states(db_path: str) -> List[sqlite3.Row]:
    conn = get_db(db_path)
    try:
        return conn.execute("SELECT * FROM file_state ORDER BY path").fetchall()
    finally:
        conn.close()
