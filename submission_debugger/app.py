#!/usr/bin/env python3
from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import mimetypes
import os
import re
import secrets
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse

from fastapi import FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

ROOT_DIR = Path(__file__).resolve().parent.parent
APP_DIR = Path(__file__).resolve().parent


def resolve_dataset_dir() -> Path:
    raw = os.environ.get("SD_DATASET_DIR", "").strip()
    if not raw:
        return (ROOT_DIR / "dataset").resolve()
    p = Path(raw).expanduser()
    if not p.is_absolute():
        p = ROOT_DIR / p
    return p.resolve()


DATASET_DIR = resolve_dataset_dir()
VIDEOS_DIR = DATASET_DIR / "videos"
TRAIN_VIDEOS_DIR = DATASET_DIR / "sim_dataset" / "videos"
TEST_META_PATH = DATASET_DIR / "test_metadata.csv"
TRAIN_META_PATH = DATASET_DIR / "sim_dataset" / "labels.csv"
DB_PATH = APP_DIR / "data" / "debugger.db"
USER_SUBMISSIONS_DIR = APP_DIR / "data" / "user_submissions"

AUTH_COOKIE_NAME = "sd_session"
SESSION_TTL_HOURS = 24 * 14
DEFAULT_ADMIN_USER = os.environ.get("SD_ADMIN_USER", "admin")
DEFAULT_ADMIN_PASS = os.environ.get("SD_ADMIN_PASS", "change-me")
DATA_SOURCES = ["test", "train"]
FORCE_SECURE_COOKIE = os.environ.get("SD_COOKIE_SECURE", "0").strip().lower() in {"1", "true", "yes", "on"}

GT_TYPE_OPTIONS = [
    {"value": "head-on", "label": "head-on", "description": "두 차량이 정면 방향으로 직접 충돌한 경우"},
    {"value": "rear-end", "label": "rear-end", "description": "뒤따르던 차량이 앞차를 추돌한 경우"},
    {"value": "sideswipe", "label": "sideswipe", "description": "차량 측면끼리 스치듯 접촉하거나 밀고 지나간 경우"},
    {"value": "single", "label": "single", "description": "다른 차량과 직접 충돌 없이 단독으로 사고가 난 경우"},
    {"value": "t-bone", "label": "t-bone", "description": "한 차량의 전면이 다른 차량 측면을 수직에 가깝게 충돌한 경우"},
]
VALID_TYPES = [item["value"] for item in GT_TYPE_OPTIONS]
SUBMISSION_REQUIRED_COLUMNS = {"path", "accident_time", "center_x", "center_y", "type"}
PERSONAL_SUBMISSION_PREFIX = "@"
DEFAULT_METADATA_TAG_FIELDS = ("scene_layout",)
TEST_DATASET_TAG_OPTIONS = [
    {"value": "label_ambiguous", "label": "label 애매함"},
    {"value": "time_ambiguous", "label": "시간 애매함"},
]
DATASET_TAG_LABELS = {item["value"]: item["label"] for item in TEST_DATASET_TAG_OPTIONS}

app = FastAPI(title="Submission Debugger", version="0.1.0")
app.mount("/static", StaticFiles(directory=str(APP_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(APP_DIR / "templates"))


@app.middleware("http")
async def normalize_redundant_slashes(request: Request, call_next):
    # Some clients/extensions may request paths like //video?...; normalize to /video?... .
    path = request.url.path
    if "//" in path:
        normalized = re.sub(r"/{2,}", "/", path)
        if not normalized.startswith("/"):
            normalized = f"/{normalized}"
        query = request.url.query
        target = f"{normalized}?{query}" if query else normalized
        return RedirectResponse(url=target, status_code=307)
    return await call_next(request)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with get_db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS expected_gt (
                video_path TEXT PRIMARY KEY,
                accident_time REAL,
                center_x REAL,
                center_y REAL,
                type TEXT,
                note TEXT,
                updated_by TEXT,
                updated_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS gt_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                video_path TEXT NOT NULL,
                edited_by TEXT NOT NULL,
                edited_at TEXT NOT NULL,
                before_json TEXT,
                after_json TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                username TEXT PRIMARY KEY,
                salt TEXT NOT NULL,
                password_hash TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_preferences (
                username TEXT PRIMARY KEY,
                default_submission TEXT,
                default_source TEXT NOT NULL DEFAULT 'test',
                FOREIGN KEY(username) REFERENCES users(username)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sessions (
                token TEXT PRIMARY KEY,
                username TEXT NOT NULL,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                FOREIGN KEY(username) REFERENCES users(username)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_submission_permissions (
                username TEXT NOT NULL,
                submission_name TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY(username, submission_name),
                FOREIGN KEY(username) REFERENCES users(username)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_submission_meta (
                submission_ref TEXT PRIMARY KEY,
                username TEXT NOT NULL,
                kind TEXT NOT NULL DEFAULT 'test',
                original_filename TEXT,
                note TEXT,
                kaggle_score REAL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(username) REFERENCES users(username)
            )
            """
        )
        try:
            conn.execute("ALTER TABLE user_submission_meta ADD COLUMN note TEXT")
        except sqlite3.OperationalError:
            # Column already exists on upgraded DBs.
            pass
        try:
            conn.execute("ALTER TABLE user_submission_meta ADD COLUMN kaggle_score REAL")
        except sqlite3.OperationalError:
            # Column already exists on upgraded DBs.
            pass
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS submission_comments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                submission_ref TEXT NOT NULL,
                comment TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(username, submission_ref),
                FOREIGN KEY(username) REFERENCES users(username)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS submission_video_comments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                submission_ref TEXT NOT NULL,
                video_path TEXT NOT NULL,
                comment TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(username, submission_ref, video_path),
                FOREIGN KEY(username) REFERENCES users(username)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dataset_video_notes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                video_path TEXT NOT NULL,
                username TEXT NOT NULL,
                comment TEXT NOT NULL,
                tags TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(username) REFERENCES users(username)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dataset_video_tags (
                source TEXT NOT NULL,
                video_path TEXT NOT NULL,
                tag TEXT NOT NULL,
                added_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY(source, video_path, tag),
                FOREIGN KEY(added_by) REFERENCES users(username)
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sessions_username ON sessions(username)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_perm_username ON user_submission_permissions(username)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_usm_username ON user_submission_meta(username)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sc_submission ON submission_comments(submission_ref)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_svc_submission ON submission_video_comments(submission_ref)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_dvn_source_video ON dataset_video_notes(source, video_path)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_dvt_source_video ON dataset_video_tags(source, video_path)")
        conn.commit()


def hash_password(password: str, salt: str) -> str:
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), 120_000)
    return digest.hex()


def ensure_default_admin_user() -> None:
    with get_db() as conn:
        cur = conn.execute("SELECT username FROM users WHERE username = ?", (DEFAULT_ADMIN_USER,))
        row = cur.fetchone()
        if row is not None:
            return
        salt = secrets.token_hex(16)
        pwd_hash = hash_password(DEFAULT_ADMIN_PASS, salt)
        now = utc_now_iso()
        conn.execute(
            "INSERT INTO users(username, salt, password_hash, created_at) VALUES (?, ?, ?, ?)",
            (DEFAULT_ADMIN_USER, salt, pwd_hash, now),
        )
        conn.execute(
            "INSERT OR IGNORE INTO user_preferences(username, default_submission, default_source) VALUES (?, NULL, 'test')",
            (DEFAULT_ADMIN_USER,),
        )
        conn.commit()


def is_admin(username: str) -> bool:
    return username == DEFAULT_ADMIN_USER


def is_valid_username(username: str) -> bool:
    if not username:
        return False
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-")
    return all(ch in allowed for ch in username)


def make_personal_submission_ref(owner: str, filename: str) -> str:
    return f"{PERSONAL_SUBMISSION_PREFIX}{owner}/{filename}"


def parse_submission_ref(ref: str) -> tuple[str, str | None, str]:
    s = str(ref).strip()
    if s.startswith(PERSONAL_SUBMISSION_PREFIX):
        body = s[1:]
        owner, sep, filename = body.partition("/")
        if not sep or not owner or not filename:
            raise HTTPException(status_code=400, detail=f"Invalid personal submission ref: {ref}")
        return "personal", owner, filename
    return "shared", None, s


def sanitize_submission_filename(filename: str | None) -> str:
    raw = (filename or "").strip()
    base = Path(raw).name
    base = re.sub(r"[^A-Za-z0-9_.-]", "_", base)
    if not base:
        base = f"submission_upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    if not base.lower().endswith(".csv"):
        base = f"{base}.csv"
    if not base.startswith("submission_"):
        base = f"submission_{base}"
    return base


def list_submission_files() -> list[Path]:
    return sorted(ROOT_DIR.glob("submission_*.csv"))


def list_user_submission_files(username: str) -> list[Path]:
    user_dir = USER_SUBMISSIONS_DIR / username
    if not user_dir.exists():
        return []
    return sorted(user_dir.glob("submission_*.csv"))


def list_all_personal_submission_refs() -> list[str]:
    if not USER_SUBMISSIONS_DIR.exists():
        return []
    refs: list[str] = []
    for user_dir in sorted(USER_SUBMISSIONS_DIR.iterdir()):
        if not user_dir.is_dir():
            continue
        owner = user_dir.name
        if not is_valid_username(owner):
            continue
        for p in sorted(user_dir.glob("submission_*.csv")):
            refs.append(make_personal_submission_ref(owner, p.name))
    return refs


def list_personal_submission_refs_for_user(username: str) -> list[str]:
    return [make_personal_submission_ref(username, p.name) for p in list_user_submission_files(username)]


def ensure_personal_starter_submissions(username: str) -> None:
    # Seed starter files only when the user has no personal submissions yet.
    if list_user_submission_files(username):
        return

    user_dir = USER_SUBMISSIONS_DIR / username
    user_dir.mkdir(parents=True, exist_ok=True)

    # Train starter
    train_target = user_dir / "submission_train_demo.csv"
    if not train_target.exists():
        try:
            train_rows = load_train_metadata()
            sample_path = train_rows[0]["path"] if train_rows else "videos/sample_train.mp4"
            sio = io.StringIO()
            writer = csv.DictWriter(sio, fieldnames=["path", "accident_time", "center_x", "center_y", "type"])
            writer.writeheader()
            writer.writerow(
                {
                    "path": sample_path,
                    "accident_time": 1.23,
                    "center_x": 0.50,
                    "center_y": 0.50,
                    "type": "rear-end",
                }
            )
            train_target.write_text(sio.getvalue(), encoding="utf-8")
        except Exception:
            pass

    # Test starter
    test_target = user_dir / "submission_test_demo.csv"
    if not test_target.exists():
        try:
            shared_subs = list_submission_files()
            if shared_subs:
                test_target.write_text(shared_subs[0].read_text(encoding="utf-8"), encoding="utf-8")
            else:
                test_rows = load_test_metadata()
                sample_path = test_rows[0]["path"] if test_rows else "videos/sample_test.mp4"
                sio = io.StringIO()
                writer = csv.DictWriter(sio, fieldnames=["path", "accident_time", "center_x", "center_y", "type"])
                writer.writeheader()
                writer.writerow(
                    {
                        "path": sample_path,
                        "accident_time": 1.23,
                        "center_x": 0.50,
                        "center_y": 0.50,
                        "type": "rear-end",
                    }
                )
                test_target.write_text(sio.getvalue(), encoding="utf-8")
        except Exception:
            pass

    if train_target.exists():
        upsert_user_submission_meta(
            username,
            make_personal_submission_ref(username, train_target.name),
            "train",
            original_filename=train_target.name,
        )
    if test_target.exists():
        upsert_user_submission_meta(
            username,
            make_personal_submission_ref(username, test_target.name),
            "test",
            original_filename=test_target.name,
        )


def normalize_submission_kind(kind: str | None) -> str:
    v = (kind or "").strip().lower()
    if v in {"train", "sim", "sim_dataset"}:
        return "train"
    return "test"


def get_submission_kind_for_user(username: str, submission_name: str) -> str:
    kind, owner, _ = parse_submission_ref(submission_name)
    if kind == "shared":
        return "test"
    if owner is None:
        return "test"

    if is_admin(username):
        meta = get_all_user_submission_meta_map().get(submission_name, {})
        return normalize_submission_kind(meta.get("kind"))

    meta = get_user_submission_meta_map(owner).get(submission_name, {})
    return normalize_submission_kind(meta.get("kind"))


def normalize_tags(raw: str | None) -> list[str]:
    if not raw:
        return []
    items = [x.strip().lower() for x in str(raw).split(",")]
    out: list[str] = []
    for t in items:
        if not t:
            continue
        safe = re.sub(r"[^a-z0-9_.-]", "_", t)
        if safe and safe not in out:
            out.append(safe)
    return out


def parse_optional_float(raw: str | None, field_name: str) -> float | None:
    v = str(raw or "").strip()
    if not v:
        return None
    try:
        return float(v)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"{field_name} must be a number") from exc


def normalize_single_tag(raw: str | None) -> str:
    tags = normalize_tags(raw)
    if not tags:
        return ""
    return tags[0]


def merge_tag_lists(*tag_lists: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for tag_list in tag_lists:
        for raw in tag_list:
            tag = str(raw or "").strip()
            if not tag or tag in seen:
                continue
            seen.add(tag)
            out.append(tag)
    return out


def build_default_metadata_tags(source: str, meta: dict[str, Any] | None) -> list[str]:
    if source != "test" or not meta:
        return []

    tags: list[str] = []
    for field in DEFAULT_METADATA_TAG_FIELDS:
        field_norm = normalize_single_tag(field)
        value_norm = normalize_single_tag(str(meta.get(field, "")))
        if field_norm and value_norm:
            tags.append(f"{field_norm}.{value_norm}")
    return merge_tag_lists(tags)


def get_dataset_tag_option_rows(source: str) -> list[dict[str, str]]:
    if source == "test":
        return [dict(item) for item in TEST_DATASET_TAG_OPTIONS]
    return []


def normalize_tag_filter_mode(raw: str | None) -> str:
    v = str(raw or "").strip().lower()
    if v == "or":
        return "or"
    if v == "not":
        return "not"
    return "and"


def parse_manual_tag_filters(
    legacy_tag: str | None,
    raw_modes: list[str] | None,
    raw_values: list[str] | None,
) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for mode_raw, value_raw in zip(raw_modes or [], raw_values or []):
        tag = normalize_single_tag(value_raw)
        if not tag:
            continue
        out.append({"mode": normalize_tag_filter_mode(mode_raw), "tag": tag})

    legacy_norm = normalize_single_tag(legacy_tag)
    if legacy_norm and legacy_norm != "all" and not out:
        out.append({"mode": "and", "tag": legacy_norm})
    return out


def match_manual_tag_filters(tags: list[str], filters: list[dict[str, str]]) -> bool:
    if not filters:
        return True

    tag_set = set(tags)
    or_tags: list[str] = []

    for item in filters:
        mode = normalize_tag_filter_mode(item.get("mode"))
        tag = normalize_single_tag(item.get("tag"))
        if not tag:
            continue
        if mode == "and":
            if tag not in tag_set:
                return False
        elif mode == "not":
            if tag in tag_set:
                return False
        else:
            or_tags.append(tag)

    if or_tags and not any(tag in tag_set for tag in or_tags):
        return False
    return True


def set_submission_comment(username: str, submission_ref: str, comment: str) -> None:
    now = utc_now_iso()
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO submission_comments(username, submission_ref, comment, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(username, submission_ref) DO UPDATE SET
                comment=excluded.comment,
                updated_at=excluded.updated_at
            """,
            (username, submission_ref, comment, now, now),
        )
        conn.commit()


def set_submission_video_comment(username: str, submission_ref: str, video_path: str, comment: str) -> None:
    now = utc_now_iso()
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO submission_video_comments(username, submission_ref, video_path, comment, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(username, submission_ref, video_path) DO UPDATE SET
                comment=excluded.comment,
                updated_at=excluded.updated_at
            """,
            (username, submission_ref, video_path, comment, now, now),
        )
        conn.commit()


def get_submission_comment(username: str, submission_ref: str) -> str:
    with get_db() as conn:
        cur = conn.execute(
            "SELECT comment FROM submission_comments WHERE username = ? AND submission_ref = ?",
            (username, submission_ref),
        )
        row = cur.fetchone()
    return str(row["comment"]) if row and row["comment"] is not None else ""


def get_submission_video_comment_map(username: str, submission_ref: str) -> dict[str, str]:
    with get_db() as conn:
        cur = conn.execute(
            """
            SELECT video_path, comment
            FROM submission_video_comments
            WHERE username = ? AND submission_ref = ?
            """,
            (username, submission_ref),
        )
        rows = cur.fetchall()
    return {str(r["video_path"]): str(r["comment"] or "") for r in rows}


def list_submission_model_comments(submission_ref: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    with get_db() as conn:
        cur1 = conn.execute(
            """
            SELECT username, comment, updated_at
            FROM submission_comments
            WHERE submission_ref = ?
            ORDER BY updated_at DESC
            """,
            (submission_ref,),
        )
        for r in cur1.fetchall():
            out.append(
                {
                    "scope": "submission",
                    "video_path": None,
                    "username": r["username"],
                    "comment": r["comment"],
                    "updated_at": r["updated_at"],
                }
            )

        cur2 = conn.execute(
            """
            SELECT username, video_path, comment, updated_at
            FROM submission_video_comments
            WHERE submission_ref = ?
            ORDER BY updated_at DESC
            """,
            (submission_ref,),
        )
        for r in cur2.fetchall():
            out.append(
                {
                    "scope": "video",
                    "video_path": r["video_path"],
                    "username": r["username"],
                    "comment": r["comment"],
                    "updated_at": r["updated_at"],
                }
            )
    out.sort(key=lambda x: str(x.get("updated_at") or ""), reverse=True)
    return out


def list_submission_video_comments(submission_ref: str, video_path: str) -> list[dict[str, Any]]:
    with get_db() as conn:
        cur = conn.execute(
            """
            SELECT username, comment, updated_at
            FROM submission_video_comments
            WHERE submission_ref = ? AND video_path = ?
            ORDER BY updated_at DESC
            """,
            (submission_ref, video_path),
        )
        rows = cur.fetchall()
    return [
        {
            "username": str(r["username"] or ""),
            "comment": str(r["comment"] or ""),
            "updated_at": str(r["updated_at"] or ""),
        }
        for r in rows
    ]


def add_dataset_video_note(source: str, video_path: str, username: str, comment: str) -> None:
    now = utc_now_iso()
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO dataset_video_notes(source, video_path, username, comment, tags, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (source, video_path, username, comment, "", now),
        )
        conn.commit()


def add_dataset_video_tag(source: str, video_path: str, username: str, tag: str) -> None:
    now = utc_now_iso()
    with get_db() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO dataset_video_tags(source, video_path, tag, added_by, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (source, video_path, tag, username, now),
        )
        conn.commit()


def delete_dataset_video_tag(source: str, video_path: str, tag: str) -> None:
    with get_db() as conn:
        # Primary tag store
        conn.execute(
            "DELETE FROM dataset_video_tags WHERE source = ? AND video_path = ? AND tag = ?",
            (source, video_path, tag),
        )

        # Backward-compat cleanup for old note rows that still carry comma-separated tags.
        cur = conn.execute(
            "SELECT id, tags FROM dataset_video_notes WHERE source = ? AND video_path = ?",
            (source, video_path),
        )
        rows = cur.fetchall()
        for r in rows:
            existing = normalize_tags(r["tags"])
            if tag not in existing:
                continue
            remain = [t for t in existing if t != tag]
            conn.execute(
                "UPDATE dataset_video_notes SET tags = ? WHERE id = ?",
                (
                    ",".join(remain),
                    int(r["id"]),
                ),
            )
        conn.commit()


def list_dataset_video_tags(source: str, video_path: str) -> list[str]:
    with get_db() as conn:
        cur = conn.execute(
            """
            SELECT tag
            FROM dataset_video_tags
            WHERE source = ? AND video_path = ?
            ORDER BY tag ASC
            """,
            (source, video_path),
        )
        rows = cur.fetchall()
    tags = [str(r["tag"] or "") for r in rows if str(r["tag"] or "")]
    return sorted(list(dict.fromkeys(tags)))


def get_dataset_video_notes_index(source: str) -> tuple[dict[str, dict[str, Any]], list[str]]:
    with get_db() as conn:
        cur = conn.execute(
            """
            SELECT source, video_path, username, comment, tags, created_at
            FROM dataset_video_notes
            WHERE source = ?
            ORDER BY created_at DESC
            """,
            (source,),
        )
        rows = cur.fetchall()

    index: dict[str, dict[str, Any]] = {}
    tags_all: list[str] = []
    tags_by_video: dict[str, set[str]] = {}

    def ensure_item(path: str) -> dict[str, Any]:
        if path not in index:
            index[path] = {
                "count": 0,
                "tags": [],
                "latest_comment": "",
                "latest_user": "",
                "latest_at": "",
            }
        if path not in tags_by_video:
            tags_by_video[path] = set()
        return index[path]

    for r in rows:
        p = str(r["video_path"])
        item = ensure_item(p)
        if int(item["count"]) == 0:
            item["latest_comment"] = str(r["comment"] or "")
            item["latest_user"] = str(r["username"] or "")
            item["latest_at"] = str(r["created_at"] or "")
        item["count"] = int(item["count"]) + 1
        for tag in normalize_tags(r["tags"]):
            tags_by_video[p].add(tag)
            if tag not in tags_all:
                tags_all.append(tag)

    with get_db() as conn:
        cur_tags = conn.execute(
            """
            SELECT source, video_path, tag
            FROM dataset_video_tags
            WHERE source = ?
            ORDER BY video_path ASC, tag ASC
            """,
            (source,),
        )
        tag_rows = cur_tags.fetchall()

    for r in tag_rows:
        p = str(r["video_path"] or "")
        tag = str(r["tag"] or "")
        if not p or not tag:
            continue
        ensure_item(p)
        tags_by_video[p].add(tag)
        if tag not in tags_all:
            tags_all.append(tag)

    tags_all.sort()
    for p in index:
        index[p]["tags"] = sorted(list(tags_by_video.get(p, set())))
    return index, tags_all


def list_dataset_video_notes(source: str, video_path: str) -> tuple[list[dict[str, Any]], list[str]]:
    with get_db() as conn:
        cur = conn.execute(
            """
            SELECT username, comment, tags, created_at
            FROM dataset_video_notes
            WHERE source = ? AND video_path = ?
            ORDER BY created_at DESC
            """,
            (source, video_path),
        )
        rows = cur.fetchall()

    notes: list[dict[str, Any]] = []
    all_tags = set(list_dataset_video_tags(source, video_path))
    for r in rows:
        # Backward-compat: expose legacy tags that were previously stored with notes.
        for t in normalize_tags(r["tags"]):
            all_tags.add(t)
        notes.append(
            {
                "username": str(r["username"] or ""),
                "comment": str(r["comment"] or ""),
                "created_at": str(r["created_at"] or ""),
            }
        )
    return notes, sorted(list(all_tags))


def upsert_user_submission_meta(
    username: str,
    submission_ref: str,
    kind: str,
    original_filename: str | None = None,
    note: str | None = None,
    kaggle_score: float | None = None,
) -> None:
    now = utc_now_iso()
    kind_eff = normalize_submission_kind(kind)
    score_eff = kaggle_score if kind_eff == "test" else None
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO user_submission_meta(submission_ref, username, kind, original_filename, note, kaggle_score, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(submission_ref) DO UPDATE SET
                username=excluded.username,
                kind=excluded.kind,
                original_filename=excluded.original_filename,
                note=COALESCE(excluded.note, user_submission_meta.note),
                kaggle_score=CASE
                    WHEN excluded.kind = 'test' THEN excluded.kaggle_score
                    ELSE NULL
                END
            """,
            (submission_ref, username, kind_eff, original_filename, note, score_eff, now),
        )
        conn.commit()


def set_user_submission_note(submission_ref: str, request_user: str, note: str) -> None:
    kind, owner, _ = parse_submission_ref(submission_ref)
    if kind != "personal" or owner is None:
        raise HTTPException(status_code=400, detail="Only personal submissions support notes")
    if owner != request_user:
        raise HTTPException(status_code=403, detail="Submission access denied")

    text = str(note or "").strip()
    if len(text) > 2000:
        raise HTTPException(status_code=400, detail="note is too long")

    with get_db() as conn:
        cur = conn.execute("SELECT submission_ref FROM user_submission_meta WHERE submission_ref = ?", (submission_ref,))
        if cur.fetchone() is None:
            raise HTTPException(status_code=404, detail="Submission metadata not found")
        conn.execute("UPDATE user_submission_meta SET note = ? WHERE submission_ref = ?", (text, submission_ref))
        conn.commit()


def set_user_submission_kaggle_score(submission_ref: str, request_user: str, kaggle_score: float | None) -> None:
    kind, owner, _ = parse_submission_ref(submission_ref)
    if kind != "personal" or owner is None:
        raise HTTPException(status_code=400, detail="Only personal submissions support kaggle score")
    if owner != request_user:
        raise HTTPException(status_code=403, detail="Submission access denied")

    with get_db() as conn:
        cur = conn.execute(
            "SELECT submission_ref, kind FROM user_submission_meta WHERE submission_ref = ?",
            (submission_ref,),
        )
        row = cur.fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Submission metadata not found")
        if normalize_submission_kind(row["kind"]) != "test":
            raise HTTPException(status_code=400, detail="kaggle score is only available for test submissions")

        conn.execute(
            "UPDATE user_submission_meta SET kaggle_score = ? WHERE submission_ref = ?",
            (kaggle_score, submission_ref),
        )
        conn.commit()


def get_user_submission_meta_map(username: str) -> dict[str, dict[str, Any]]:
    with get_db() as conn:
        cur = conn.execute(
            """
            SELECT submission_ref, kind, original_filename, note, kaggle_score, created_at
            FROM user_submission_meta
            WHERE username = ?
            ORDER BY created_at DESC
            """,
            (username,),
        )
        rows = cur.fetchall()
    out: dict[str, dict[str, Any]] = {}
    for r in rows:
        ref = str(r["submission_ref"])
        out[ref] = {
            "kind": normalize_submission_kind(r["kind"]),
            "original_filename": r["original_filename"],
            "note": str(r["note"] or ""),
            "kaggle_score": parse_float(r["kaggle_score"]),
            "created_at": r["created_at"],
        }
    return out


def get_all_user_submission_meta_map() -> dict[str, dict[str, Any]]:
    with get_db() as conn:
        cur = conn.execute(
            """
            SELECT submission_ref, username, kind, original_filename, note, kaggle_score, created_at
            FROM user_submission_meta
            ORDER BY created_at DESC
            """
        )
        rows = cur.fetchall()
    out: dict[str, dict[str, Any]] = {}
    for r in rows:
        ref = str(r["submission_ref"])
        out[ref] = {
            "username": str(r["username"] or ""),
            "kind": normalize_submission_kind(r["kind"]),
            "original_filename": r["original_filename"],
            "note": str(r["note"] or ""),
            "kaggle_score": parse_float(r["kaggle_score"]),
            "created_at": str(r["created_at"] or ""),
        }
    return out


def list_personal_submission_entries(username: str) -> list[dict[str, Any]]:
    refs = list_personal_submission_refs_for_user(username)
    meta_map = get_user_submission_meta_map(username)
    items: list[dict[str, Any]] = []
    for ref in refs:
        _, _, filename = parse_submission_ref(ref)
        created_at = None
        try:
            path = resolve_submission_path(ref, request_user=username)
            created_at = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat(timespec="seconds")
        except Exception:
            created_at = None
        meta = meta_map.get(ref, {})
        item = {
            "submission": ref,
            "filename": filename,
            "kind": normalize_submission_kind(meta.get("kind")),
            "created_at": meta.get("created_at") or created_at,
            "original_filename": meta.get("original_filename"),
            "note": str(meta.get("note") or ""),
            "kaggle_score": meta.get("kaggle_score"),
        }
        items.append(item)
    items.sort(key=lambda x: x.get("created_at") or "", reverse=True)
    return items


def delete_personal_submission(submission_ref: str, request_user: str) -> bool:
    kind, owner, _ = parse_submission_ref(submission_ref)
    if kind != "personal" or owner is None:
        raise HTTPException(status_code=400, detail="Only personal submissions can be deleted")
    if (not is_admin(request_user)) and owner != request_user:
        raise HTTPException(status_code=403, detail="Submission access denied")

    path = USER_SUBMISSIONS_DIR / owner / parse_submission_ref(submission_ref)[2]
    deleted = False
    if path.exists() and path.is_file():
        path.unlink()
        deleted = True

    with get_db() as conn:
        conn.execute("DELETE FROM user_submission_meta WHERE submission_ref = ?", (submission_ref,))
        conn.execute(
            """
            UPDATE user_preferences
            SET default_submission = NULL
            WHERE username = ? AND default_submission = ?
            """,
            (owner, submission_ref),
        )
        conn.commit()

    return deleted


def get_top_contributors(limit: int = 3) -> list[dict[str, Any]]:
    with get_db() as conn:
        cur = conn.execute(
            """
            SELECT
                edited_by,
                COUNT(*) AS edits,
                COUNT(DISTINCT video_path) AS videos,
                MAX(edited_at) AS last_at
            FROM gt_history
            WHERE edited_by IS NOT NULL AND TRIM(edited_by) != ''
            GROUP BY edited_by
            ORDER BY edits DESC, videos DESC, last_at DESC
            LIMIT ?
            """,
            (max(1, int(limit)),),
        )
        rows = cur.fetchall()
    return [
        {
            "rank": i + 1,
            "name": r["edited_by"],
            "edits": int(r["edits"] or 0),
            "videos": int(r["videos"] or 0),
            "last_at": r["last_at"],
        }
        for i, r in enumerate(rows)
    ]


def list_all_submissions() -> list[str]:
    return [p.name for p in list_submission_files()]


def list_all_uploaded_csv_entries() -> list[dict[str, Any]]:
    meta_map = get_all_user_submission_meta_map()
    items: list[dict[str, Any]] = []

    # Shared submissions at repo root.
    for p in list_submission_files():
        mtime = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc).isoformat(timespec="seconds")
        items.append(
            {
                "submission": p.name,
                "filename": p.name,
                "owner": "shared",
                "kind": "test",
                "created_at": mtime,
                "note": "",
                "kaggle_score": None,
                "can_edit_note": False,
            }
        )

    # Personal submissions across users.
    for ref in list_all_personal_submission_refs():
        _, owner, filename = parse_submission_ref(ref)
        path = USER_SUBMISSIONS_DIR / str(owner or "") / filename
        created_at = None
        if path.exists():
            created_at = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat(timespec="seconds")
        meta = meta_map.get(ref, {})
        items.append(
            {
                "submission": ref,
                "filename": filename,
                "owner": str(owner or ""),
                "kind": normalize_submission_kind(meta.get("kind")),
                "created_at": str(meta.get("created_at") or created_at or ""),
                "note": str(meta.get("note") or ""),
                "kaggle_score": meta.get("kaggle_score"),
                "can_edit_note": True,
            }
        )

    items.sort(key=lambda x: str(x.get("created_at") or ""), reverse=True)
    return items


def get_allowed_submissions(username: str) -> list[str]:
    shared_subs = list_all_submissions()
    if is_admin(username):
        return sorted(shared_subs + list_all_personal_submission_refs())
    with get_db() as conn:
        cur = conn.execute(
            "SELECT submission_name FROM user_submission_permissions WHERE username = ? ORDER BY submission_name ASC",
            (username,),
        )
        rows = [r["submission_name"] for r in cur.fetchall()]
    allowed_shared = [s for s in rows if s in shared_subs]
    personal = list_personal_submission_refs_for_user(username)
    return sorted(allowed_shared + personal)


def can_access_submission(username: str, submission_name: str | None) -> bool:
    if submission_name is None:
        return False
    return submission_name in get_allowed_submissions(username)


def resolve_submission_path(submission_name: str, request_user: str) -> Path:
    kind, owner, filename = parse_submission_ref(submission_name)
    if kind == "shared":
        target = ROOT_DIR / filename
        if not target.exists() or target.suffix.lower() != ".csv":
            raise HTTPException(status_code=404, detail=f"Submission not found: {submission_name}")
        return target

    if owner is None:
        raise HTTPException(status_code=400, detail=f"Invalid personal submission ref: {submission_name}")
    if (not is_admin(request_user)) and request_user != owner:
        raise HTTPException(status_code=403, detail="Submission access denied")
    target = USER_SUBMISSIONS_DIR / owner / filename
    if not target.exists() or target.suffix.lower() != ".csv":
        raise HTTPException(status_code=404, detail=f"Submission not found: {submission_name}")
    return target


def set_user_submission_permissions(username: str, submission_names: list[str]) -> None:
    if is_admin(username):
        return
    all_subs = set(list_all_submissions())
    clean = sorted(set(s for s in submission_names if s in all_subs))
    now = utc_now_iso()
    with get_db() as conn:
        conn.execute("DELETE FROM user_submission_permissions WHERE username = ?", (username,))
        for sub in clean:
            conn.execute(
                "INSERT INTO user_submission_permissions(username, submission_name, created_at) VALUES (?, ?, ?)",
                (username, sub, now),
            )
        conn.commit()


def list_users_with_permissions() -> list[dict[str, Any]]:
    subs = list_all_submissions()
    with get_db() as conn:
        users_cur = conn.execute("SELECT username, created_at FROM users ORDER BY username ASC")
        users = users_cur.fetchall()
        out: list[dict[str, Any]] = []
        for u in users:
            username = u["username"]
            prefs = get_user_preferences(username)
            allowed = get_allowed_submissions(username)
            out.append(
                {
                    "username": username,
                    "created_at": u["created_at"],
                    "is_admin": is_admin(username),
                    "default_submission": prefs["default_submission"],
                    "default_source": prefs["default_source"],
                    "allowed_submissions": allowed,
                    "all_submissions": subs,
                }
            )
    return out


def create_user(username: str, password: str) -> None:
    if not is_valid_username(username):
        raise HTTPException(status_code=400, detail="Invalid username format")
    if len(password) < 4:
        raise HTTPException(status_code=400, detail="Password must be at least 4 characters")
    salt = secrets.token_hex(16)
    pwd_hash = hash_password(password, salt)
    now = utc_now_iso()
    with get_db() as conn:
        cur = conn.execute("SELECT username FROM users WHERE username = ?", (username,))
        if cur.fetchone() is not None:
            raise HTTPException(status_code=400, detail="Username already exists")
        conn.execute(
            "INSERT INTO users(username, salt, password_hash, created_at) VALUES (?, ?, ?, ?)",
            (username, salt, pwd_hash, now),
        )
        conn.execute(
            "INSERT OR IGNORE INTO user_preferences(username, default_submission, default_source) VALUES (?, NULL, 'test')",
            (username,),
        )
        conn.commit()


def change_password(username: str, new_password: str) -> None:
    if len(new_password) < 4:
        raise HTTPException(status_code=400, detail="Password must be at least 4 characters")
    salt = secrets.token_hex(16)
    pwd_hash = hash_password(new_password, salt)
    with get_db() as conn:
        cur = conn.execute("SELECT username FROM users WHERE username = ?", (username,))
        if cur.fetchone() is None:
            raise HTTPException(status_code=404, detail="User not found")
        conn.execute("UPDATE users SET salt = ?, password_hash = ? WHERE username = ?", (salt, pwd_hash, username))
        conn.commit()


def verify_login(username: str, password: str) -> bool:
    with get_db() as conn:
        cur = conn.execute("SELECT username, salt, password_hash FROM users WHERE username = ?", (username,))
        row = cur.fetchone()
    if row is None:
        return False
    calc = hash_password(password, row["salt"])
    return secrets.compare_digest(calc, row["password_hash"])


def create_session(username: str) -> tuple[str, str]:
    token = secrets.token_urlsafe(32)
    created_at = datetime.now(timezone.utc)
    expires_at = created_at + timedelta(hours=SESSION_TTL_HOURS)
    with get_db() as conn:
        conn.execute(
            "INSERT INTO sessions(token, username, created_at, expires_at) VALUES (?, ?, ?, ?)",
            (token, username, created_at.isoformat(timespec="seconds"), expires_at.isoformat(timespec="seconds")),
        )
        conn.commit()
    return token, expires_at.isoformat(timespec="seconds")


def delete_session(token: str) -> None:
    with get_db() as conn:
        conn.execute("DELETE FROM sessions WHERE token = ?", (token,))
        conn.commit()


def get_current_user(request: Request) -> str | None:
    token = request.cookies.get(AUTH_COOKIE_NAME)
    if not token:
        return None
    with get_db() as conn:
        cur = conn.execute("SELECT token, username, expires_at FROM sessions WHERE token = ?", (token,))
        row = cur.fetchone()
        if row is None:
            return None
        try:
            exp = datetime.fromisoformat(row["expires_at"])
        except ValueError:
            conn.execute("DELETE FROM sessions WHERE token = ?", (token,))
            conn.commit()
            return None
        if exp < datetime.now(timezone.utc):
            conn.execute("DELETE FROM sessions WHERE token = ?", (token,))
            conn.commit()
            return None
        return row["username"]


def require_user(request: Request) -> str:
    user = get_current_user(request)
    if user is None:
        raise HTTPException(status_code=401, detail="Authentication required")
    return user


def get_user_preferences(username: str) -> dict[str, Any]:
    with get_db() as conn:
        cur = conn.execute(
            "SELECT username, default_submission, default_source FROM user_preferences WHERE username = ?",
            (username,),
        )
        row = cur.fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO user_preferences(username, default_submission, default_source) VALUES (?, NULL, 'test')",
                (username,),
            )
            conn.commit()
            return {"default_submission": None, "default_source": "test"}
        return {
            "default_submission": row["default_submission"],
            "default_source": row["default_source"] if row["default_source"] in DATA_SOURCES else "test",
        }


def set_user_preferences(username: str, default_submission: str | None, default_source: str | None) -> None:
    src = default_source if default_source in DATA_SOURCES else "test"
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO user_preferences(username, default_submission, default_source)
            VALUES (?, ?, ?)
            ON CONFLICT(username) DO UPDATE SET
                default_submission=excluded.default_submission,
                default_source=excluded.default_source
            """,
            (username, default_submission, src),
        )
        conn.commit()


def parse_float(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, str) and not v.strip():
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def sanitize_next_url(next_url: str | None) -> str:
    if not next_url:
        return "/"
    target = next_url.strip()
    if not target:
        return "/"
    # Only allow local absolute paths to prevent open redirects.
    if not target.startswith("/") or target.startswith("//"):
        return "/"
    parsed = urlparse(target)
    if parsed.scheme or parsed.netloc:
        return "/"
    return target


def read_submission_map(submission_name: str, request_user: str) -> dict[str, dict[str, Any]]:
    target = resolve_submission_path(submission_name, request_user=request_user)
    rows: dict[str, dict[str, Any]] = {}
    with target.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None or not SUBMISSION_REQUIRED_COLUMNS.issubset(set(reader.fieldnames)):
            raise HTTPException(status_code=400, detail="Invalid submission header")

        for row in reader:
            video_path = str(row["path"]).strip()
            rows[video_path] = {
                "path": video_path,
                "accident_time": parse_float(row.get("accident_time")),
                "center_x": parse_float(row.get("center_x")),
                "center_y": parse_float(row.get("center_y")),
                "type": str(row.get("type", "")).strip(),
            }
    return rows


def validate_submission_text(csv_text: str) -> None:
    reader = csv.DictReader(io.StringIO(csv_text))
    if reader.fieldnames is None:
        raise HTTPException(status_code=400, detail="Invalid submission header")
    normalized = [str(c).strip() for c in reader.fieldnames if str(c).strip()]
    fields = set(normalized)
    if fields != SUBMISSION_REQUIRED_COLUMNS:
        required = ", ".join(sorted(SUBMISSION_REQUIRED_COLUMNS))
        raise HTTPException(
            status_code=400,
            detail=f"Invalid submission header. Only these columns are allowed: {required}",
        )


def decode_uploaded_csv(content: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8"):
        try:
            return content.decode(enc)
        except UnicodeDecodeError:
            continue
    raise HTTPException(status_code=400, detail="CSV must be UTF-8 encoded")


def load_test_metadata() -> list[dict[str, Any]]:
    if not TEST_META_PATH.exists():
        raise HTTPException(status_code=500, detail=f"Missing metadata: {TEST_META_PATH}")

    rows: list[dict[str, Any]] = []
    with TEST_META_PATH.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            path = str(row.get("path", "")).strip()
            if not path:
                continue
            rows.append(
                {
                    "path": path,
                    "duration": parse_float(row.get("duration")),
                    "height": int(float(row.get("height", 0) or 0)),
                    "width": int(float(row.get("width", 0) or 0)),
                    "region": str(row.get("region", "")),
                    "scene_layout": str(row.get("scene_layout", "")),
                    "weather": str(row.get("weather", "")),
                    "day_time": str(row.get("day_time", "")),
                    "quality": str(row.get("quality", "")),
                }
            )
    return rows


def load_train_metadata() -> list[dict[str, Any]]:
    if not TRAIN_META_PATH.exists():
        raise HTTPException(status_code=500, detail=f"Missing train metadata: {TRAIN_META_PATH}")

    rows: list[dict[str, Any]] = []
    with TRAIN_META_PATH.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            path = str(row.get("rgb_path", "")).strip()
            if not path:
                continue
            rows.append(
                {
                    "path": path,
                    "duration": parse_float(row.get("duration")),
                    "height": int(float(row.get("height", 0) or 0)),
                    "width": int(float(row.get("width", 0) or 0)),
                    "region": str(row.get("map", "")),
                    "scene_layout": "sim",
                    "weather": str(row.get("weather", "")),
                    "day_time": "sim",
                    "quality": "sim",
                    "gt_time": parse_float(row.get("accident_time")),
                    "gt_cx": parse_float(row.get("center_x")),
                    "gt_cy": parse_float(row.get("center_y")),
                    "gt_type": str(row.get("type", "")).strip() or None,
                }
            )
    return rows


def load_metadata(source: str) -> list[dict[str, Any]]:
    if source == "train":
        return load_train_metadata()
    return load_test_metadata()


def resolve_video_path(source: str, video_path: str) -> Path:
    if source == "train":
        base = TRAIN_VIDEOS_DIR.resolve()
        rel = video_path[7:] if video_path.startswith("videos/") else video_path
        target = (TRAIN_VIDEOS_DIR / rel).resolve()
    else:
        base = VIDEOS_DIR.resolve()
        target = (DATASET_DIR / video_path).resolve()
    if not str(target).startswith(str(base)):
        raise HTTPException(status_code=400, detail="Invalid path")
    if not target.exists():
        raise HTTPException(status_code=404, detail="Video not found")
    return target


def get_gt_map() -> dict[str, dict[str, Any]]:
    with get_db() as conn:
        cur = conn.execute("SELECT * FROM expected_gt")
        rows = cur.fetchall()
    out: dict[str, dict[str, Any]] = {}
    for r in rows:
        out[r["video_path"]] = dict(r)
    return out


def get_history(video_path: str) -> list[dict[str, Any]]:
    with get_db() as conn:
        cur = conn.execute(
            """
            SELECT id, video_path, edited_by, edited_at, before_json, after_json
            FROM gt_history
            WHERE video_path = ?
            ORDER BY id DESC
            LIMIT 200
            """,
            (video_path,),
        )
        rows = cur.fetchall()
    return [dict(r) for r in rows]


def is_complete_gt(gt: dict[str, Any] | None) -> bool:
    if gt is None:
        return False
    return (
        gt.get("accident_time") is not None
        and gt.get("center_x") is not None
        and gt.get("center_y") is not None
        and bool(gt.get("type"))
    )


def score_components(pred: dict[str, Any], gt: dict[str, Any], sigma_t: float, sigma_s: float) -> dict[str, float]:
    if gt.get("accident_time") is None or gt.get("center_x") is None or gt.get("center_y") is None or not gt.get("type"):
        raise ValueError("GT is incomplete")

    pt = pred.get("accident_time")
    pcx = pred.get("center_x")
    pcy = pred.get("center_y")
    ptype = pred.get("type")
    if pt is None or pcx is None or pcy is None or not ptype:
        raise ValueError("Prediction is incomplete")

    dt = pt - float(gt["accident_time"])
    ds = math.sqrt((pcx - float(gt["center_x"])) ** 2 + (pcy - float(gt["center_y"])) ** 2)

    temporal = math.exp(-0.5 * ((dt / sigma_t) ** 2)) if sigma_t > 0 else 0.0
    spatial = math.exp(-0.5 * ((ds / sigma_s) ** 2)) if sigma_s > 0 else 0.0
    classification = 1.0 if str(ptype) == str(gt["type"]) else 0.0
    return {"T": temporal, "S": spatial, "C": classification}


def harmonic_mean(a: float, b: float, c: float) -> float:
    eps = 1e-9
    return 3.0 / ((1.0 / (a + eps)) + (1.0 / (b + eps)) + (1.0 / (c + eps)))


def aggregate_submission_score(
    sub_map: dict[str, dict[str, Any]],
    gt_map: dict[str, dict[str, Any]],
    sigma_t: float,
    sigma_s: float,
) -> dict[str, Any]:
    t_vals: list[float] = []
    s_vals: list[float] = []
    c_vals: list[float] = []

    for video_path, gt in gt_map.items():
        pred = sub_map.get(video_path)
        if pred is None:
            continue
        try:
            comp = score_components(pred, gt, sigma_t=sigma_t, sigma_s=sigma_s)
        except ValueError:
            continue
        t_vals.append(comp["T"])
        s_vals.append(comp["S"])
        c_vals.append(comp["C"])

    used = len(t_vals)
    if used == 0:
        return {
            "used": 0,
            "T": None,
            "S": None,
            "C": None,
            "H": None,
        }

    t_mean = sum(t_vals) / used
    s_mean = sum(s_vals) / used
    c_mean = sum(c_vals) / used
    h = harmonic_mean(t_mean, s_mean, c_mean)
    return {
        "used": used,
        "T": t_mean,
        "S": s_mean,
        "C": c_mean,
        "H": h,
    }


@app.on_event("startup")
def startup() -> None:
    init_db()
    ensure_default_admin_user()


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request, next_url: str = "/", error: str = "") -> HTMLResponse:
    user = get_current_user(request)
    safe_next = sanitize_next_url(next_url)
    if user is not None:
        return RedirectResponse(url=safe_next, status_code=303)
    return templates.TemplateResponse(
        "login.html",
        {
            "request": request,
            "next_url": safe_next,
            "error": error,
        },
    )


@app.post("/login")
def login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    next_url: str = Form("/"),
) -> HTMLResponse:
    u = username.strip()
    safe_next = sanitize_next_url(next_url)
    if not verify_login(u, password):
        return templates.TemplateResponse(
            "login.html",
            {
                "request": request,
                "next_url": safe_next,
                "error": "아이디 또는 비밀번호가 올바르지 않습니다.",
            },
            status_code=401,
        )

    token, _ = create_session(u)
    response = RedirectResponse(url=safe_next, status_code=303)
    response.set_cookie(
        AUTH_COOKIE_NAME,
        token,
        max_age=SESSION_TTL_HOURS * 3600,
        httponly=True,
        samesite="lax",
        secure=(request.url.scheme == "https" or FORCE_SECURE_COOKIE),
    )
    return response


@app.post("/logout")
def logout(request: Request) -> RedirectResponse:
    token = request.cookies.get(AUTH_COOKIE_NAME)
    if token:
        delete_session(token)
    response = RedirectResponse(url="/login", status_code=303)
    response.delete_cookie(AUTH_COOKIE_NAME)
    return response


def require_admin_user(request: Request) -> str:
    user = require_user(request)
    if not is_admin(user):
        raise HTTPException(status_code=403, detail="Admin only")
    return user


@app.get("/admin/users", response_class=HTMLResponse)
def admin_users_page(request: Request, msg: str = "", error: str = "") -> HTMLResponse:
    admin_user = require_admin_user(request)
    return templates.TemplateResponse(
        "admin_users.html",
        {
            "request": request,
            "user": admin_user,
            "users": list_users_with_permissions(),
            "submissions": list_all_submissions(),
            "msg": msg,
            "error": error,
        },
    )


@app.post("/admin/users/create")
def admin_create_user(request: Request, username: str = Form(...), password: str = Form(...)) -> RedirectResponse:
    _ = require_admin_user(request)
    try:
        create_user(username.strip(), password)
        return RedirectResponse(url="/admin/users?msg=created", status_code=303)
    except HTTPException as e:
        detail = e.detail if isinstance(e.detail, str) else "create_failed"
        return RedirectResponse(url=f"/admin/users?error={detail}", status_code=303)


@app.post("/admin/users/password")
def admin_change_password(request: Request, username: str = Form(...), new_password: str = Form(...)) -> RedirectResponse:
    _ = require_admin_user(request)
    try:
        change_password(username.strip(), new_password)
        return RedirectResponse(url="/admin/users?msg=password_changed", status_code=303)
    except HTTPException as e:
        detail = e.detail if isinstance(e.detail, str) else "password_change_failed"
        return RedirectResponse(url=f"/admin/users?error={detail}", status_code=303)


@app.post("/admin/users/permissions")
async def admin_set_permissions(request: Request) -> RedirectResponse:
    _ = require_admin_user(request)
    form = await request.form()
    username = str(form.get("username", "")).strip()
    selected = [v for k, v in form.multi_items() if k == "submission_names"]
    try:
        if not username:
            raise HTTPException(status_code=400, detail="username is required")
        set_user_submission_permissions(username, [str(s) for s in selected])
        return RedirectResponse(url="/admin/users?msg=permissions_updated", status_code=303)
    except HTTPException as e:
        detail = e.detail if isinstance(e.detail, str) else "permission_update_failed"
        return RedirectResponse(url=f"/admin/users?error={detail}", status_code=303)


@app.get("/", response_class=HTMLResponse)
def index(
    request: Request,
    submission: str | None = None,
    source: str | None = None,
) -> HTMLResponse:
    user = get_current_user(request)
    if user is None:
        return RedirectResponse(url="/login?next_url=/", status_code=303)

    ensure_personal_starter_submissions(user)

    gt_map = get_gt_map()
    test_paths = {m["path"] for m in load_test_metadata()}
    labeled_count = sum(1 for p, g in gt_map.items() if p in test_paths and is_complete_gt(g))
    total_count = len(test_paths)
    completion_ratio = (labeled_count / total_count) if total_count else 0.0

    personal_entries = list_personal_submission_entries(user)
    all_csv_entries = list_all_uploaded_csv_entries()
    for e in all_csv_entries:
        owner = str(e.get("owner") or "")
        e["can_edit_note"] = bool(e.get("can_edit_note")) and (owner == user)
    personal_submissions = [x["submission"] for x in personal_entries]
    contributors_top = get_top_contributors(limit=3)

    if not personal_submissions:
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "user": user,
                "is_admin": is_admin(user),
                "personal_entries": [],
                "contributors_top": contributors_top,
                "empty_personal_submissions": True,
                "all_csv_entries": all_csv_entries,
                "submissions": [],
                "current_submission": None,
                "current_source": "test",
                "sources": DATA_SOURCES,
                "gt_stats": {"total": total_count, "labeled": labeled_count, "completion_ratio": completion_ratio},
                "score": None,
                "error": None,
            },
        )

    prefs = get_user_preferences(user)
    pref_sub = prefs["default_submission"]
    if submission in personal_submissions:
        current = submission
    elif pref_sub in personal_submissions:
        current = pref_sub
    else:
        current = personal_submissions[0]

    entry_map = {x["submission"]: x for x in personal_entries}
    current_kind = normalize_submission_kind(entry_map.get(current, {}).get("kind"))
    source_eff = source if source in DATA_SOURCES else current_kind

    set_user_preferences(user, current, source_eff)

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "user": user,
            "is_admin": is_admin(user),
            "contributors_top": contributors_top,
            "empty_personal_submissions": False,
            "personal_entries": personal_entries,
            "all_csv_entries": all_csv_entries,
            "submissions": personal_submissions,
            "current_submission": current,
            "current_source": source_eff,
            "sources": DATA_SOURCES,
            "gt_stats": {
                "total": total_count,
                "labeled": labeled_count,
                "completion_ratio": completion_ratio,
            },
            "score": None,
            "error": None,
        },
    )


@app.get("/submission", response_class=HTMLResponse)
def submission_page(
    request: Request,
    submission: str,
    source: str = "test",
    q: str = "",
    quality: str = "all",
    weather: str = "all",
    day_time: str = "all",
    scene_layout: str = "all",
    only_labeled: bool = False,
    tag: str = "all",
    manual_tag_mode: list[str] = Query(default=[]),
    manual_tag_value: list[str] = Query(default=[]),
) -> HTMLResponse:
    user = get_current_user(request)
    if user is None:
        return RedirectResponse(url="/login?next_url=/", status_code=303)

    raw_submissions = request.query_params.getlist("submission")
    requested_video_path = str(request.query_params.get("video_path", "")).strip()
    if requested_video_path:
        candidate_submission: str | None = None
        for s in reversed(raw_submissions):
            s_norm = str(s).strip()
            if not s_norm:
                continue
            try:
                kind, owner, _ = parse_submission_ref(s_norm)
            except HTTPException:
                continue
            if kind == "personal" and owner == user:
                candidate_submission = s_norm
                break

        if candidate_submission is None:
            raise HTTPException(status_code=400, detail="Invalid submission query")

        source_q = str(request.query_params.get("source", source)).strip()
        source_eff_q = source_q if source_q in DATA_SOURCES else "test"
        target = (
            f"/video?source={quote(source_eff_q, safe='')}&"
            f"submission={quote(candidate_submission, safe='')}&"
            f"video_path={quote(requested_video_path, safe='')}"
        )
        return RedirectResponse(url=target, status_code=307)

    if "/video?" in submission or submission.startswith("//"):
        raise HTTPException(status_code=400, detail="Malformed submission parameter")

    personal_entries = list_personal_submission_entries(user)
    personal_submissions = [x["submission"] for x in personal_entries]
    if submission not in personal_submissions:
        raise HTTPException(status_code=403, detail="Submission access denied")

    entry_map = {x["submission"]: x for x in personal_entries}
    source_eff = normalize_submission_kind(entry_map.get(submission, {}).get("kind"))
    set_user_preferences(user, submission, source_eff)
    submission_meta = entry_map.get(submission, {})

    sub_map = read_submission_map(submission, request_user=user)
    gt_map = get_gt_map()
    metadata = load_metadata(source_eff)

    kaggle_score = submission_meta.get("kaggle_score")
    estimated_score = None
    estimated_used = 0
    expected_complete_count = 0
    if source_eff == "test":
        source_paths = {m["path"] for m in metadata}
        source_gt = {p: g for p, g in gt_map.items() if p in source_paths}
        expected_complete_count = sum(1 for g in source_gt.values() if is_complete_gt(g))
        agg = aggregate_submission_score(sub_map, source_gt, sigma_t=2.0, sigma_s=0.15)
        estimated_score = agg.get("H")
        estimated_used = int(agg.get("used") or 0)

    quality_values = sorted({m["quality"] for m in metadata if m.get("quality")})
    weather_values = sorted({m["weather"] for m in metadata if m.get("weather")})
    day_time_values = sorted({m["day_time"] for m in metadata if m.get("day_time")})
    scene_layout_values = sorted({m["scene_layout"] for m in metadata if m.get("scene_layout")})
    q_norm = q.strip().lower()
    manual_tag_filters = parse_manual_tag_filters(tag, manual_tag_mode, manual_tag_value)

    dataset_note_map, dataset_tags_all = get_dataset_video_notes_index(source_eff)
    default_tags_all = sorted({tag for m in metadata for tag in build_default_metadata_tags(source_eff, m)})
    combined_tags_all = sorted(set(dataset_tags_all) | set(default_tags_all))
    my_submission_comment = get_submission_comment(user, submission)
    my_video_comment_map = get_submission_video_comment_map(user, submission)
    submission_comment_rows = list_submission_model_comments(submission)

    rows: list[dict[str, Any]] = []
    for m in metadata:
        p = m["path"]
        gt = gt_map.get(p) if source_eff == "test" else {
            "accident_time": m.get("gt_time"),
            "center_x": m.get("gt_cx"),
            "center_y": m.get("gt_cy"),
            "type": m.get("gt_type"),
            "updated_by": "sim_dataset",
            "updated_at": None,
        }
        pred = sub_map.get(p)

        if q_norm and q_norm not in p.lower():
            continue
        if quality != "all" and m["quality"] != quality:
            continue
        if weather != "all" and m["weather"] != weather:
            continue
        if day_time != "all" and m["day_time"] != day_time:
            continue
        if scene_layout != "all" and m["scene_layout"] != scene_layout:
            continue
        note_info = dataset_note_map.get(p, {"count": 0, "tags": [], "latest_comment": "", "latest_user": "", "latest_at": ""})
        note_tags = list(note_info.get("tags") or [])
        default_tags = build_default_metadata_tags(source_eff, m)
        combined_tags = merge_tag_lists(default_tags, note_tags)
        if not match_manual_tag_filters(note_tags, manual_tag_filters):
            continue
        if only_labeled and not is_complete_gt(gt):
            continue

        score_a = None
        if is_complete_gt(gt) and pred is not None:
            comp_a = score_components(pred, gt, sigma_t=2.0, sigma_s=0.15)
            score_a = harmonic_mean(comp_a["T"], comp_a["S"], comp_a["C"])

        rows.append(
            {
                "path": p,
                "duration": m["duration"],
                "quality": m["quality"],
                "weather": m["weather"],
                "day_time": m["day_time"],
                "scene_layout": m["scene_layout"],
                "pred": pred,
                "gt": gt,
                "score_a": score_a,
                "my_model_comment": my_video_comment_map.get(p, ""),
                "dataset_note_count": int(note_info.get("count") or 0),
                "dataset_tags": combined_tags,
                "dataset_latest_comment": str(note_info.get("latest_comment") or ""),
                "dataset_latest_user": str(note_info.get("latest_user") or ""),
                "dataset_latest_at": str(note_info.get("latest_at") or ""),
            }
        )

    return templates.TemplateResponse(
        "submission.html",
        {
            "request": request,
            "user": user,
            "submission": submission,
            "source": source_eff,
            "rows": rows,
            "quality_values": quality_values,
            "weather_values": weather_values,
            "day_time_values": day_time_values,
            "scene_layout_values": scene_layout_values,
            "manual_tag_values": dataset_tags_all,
            "manual_tag_filters": manual_tag_filters,
            "dataset_tag_labels": DATASET_TAG_LABELS,
            "q": q,
            "quality": quality,
            "weather": weather,
            "day_time": day_time,
            "scene_layout": scene_layout,
            "only_labeled": only_labeled,
            "tag": tag,
            "all_tags": combined_tags_all,
            "my_submission_comment": my_submission_comment,
            "submission_comment_rows": submission_comment_rows,
            "kaggle_score": kaggle_score,
            "estimated_score": estimated_score,
            "estimated_used": estimated_used,
            "expected_complete_count": expected_complete_count,
        },
    )


@app.get("/video", response_class=HTMLResponse)
def video_page(
    request: Request,
    submission: str,
    video_path: str,
    source: str = "test",
    compare_submission: str | None = None,
    compare_submission2: str | None = None,
) -> HTMLResponse:
    user = get_current_user(request)
    if user is None:
        return RedirectResponse(url="/login?next_url=/", status_code=303)

    submissions = get_allowed_submissions(user)
    if submission not in submissions:
        raise HTTPException(status_code=403, detail="Submission access denied")

    # Source is derived from the selected submission kind to avoid query-param drift.
    source_eff = get_submission_kind_for_user(user, submission)

    # Train videos must still show model predictions from uploaded CSV.
    sub_map = read_submission_map(submission, request_user=user)
    metadata_rows = {r["path"]: r for r in load_metadata(source_eff)}
    if video_path not in metadata_rows:
        raise HTTPException(status_code=404, detail="Video not found in metadata")

    _ = resolve_video_path(source_eff, video_path)

    gt_map = get_gt_map()
    gt = gt_map.get(video_path) if source_eff == "test" else {
        "video_path": video_path,
        "accident_time": metadata_rows[video_path].get("gt_time"),
        "center_x": metadata_rows[video_path].get("gt_cx"),
        "center_y": metadata_rows[video_path].get("gt_cy"),
        "type": metadata_rows[video_path].get("gt_type"),
        "note": "sim_dataset label",
        "updated_by": "sim_dataset",
        "updated_at": None,
    }
    pred = sub_map.get(video_path)
    compare = compare_submission if compare_submission in submissions and compare_submission != submission else None
    if compare and get_submission_kind_for_user(user, compare) != source_eff:
        compare = None
    compare2 = (
        compare_submission2
        if compare_submission2 in submissions and compare_submission2 not in {submission, compare}
        else None
    )
    if compare2 and get_submission_kind_for_user(user, compare2) != source_eff:
        compare2 = None
    pred_b = read_submission_map(compare, request_user=user).get(video_path) if compare else None
    pred_c = read_submission_map(compare2, request_user=user).get(video_path) if compare2 else None
    history = get_history(video_path) if source_eff == "test" else []
    my_submission_comment = get_submission_comment(user, submission)
    my_video_comment = get_submission_video_comment_map(user, submission).get(video_path, "")
    model_video_comments = list_submission_video_comments(submission, video_path)
    dataset_notes, dataset_user_tags = list_dataset_video_notes(source_eff, video_path)
    default_dataset_tags = build_default_metadata_tags(source_eff, metadata_rows[video_path])
    dataset_tags = merge_tag_lists(default_dataset_tags, dataset_user_tags)
    default_dataset_tag_set = set(default_dataset_tags)
    dataset_user_tags_visible = [t for t in dataset_user_tags if t not in default_dataset_tag_set]
    dataset_tag_option_rows = get_dataset_tag_option_rows(source_eff)

    initial_payload = {
        "user": user,
        "video_path": video_path,
        "source": source_eff,
        "submission": submission,
        "compare_submission": compare,
        "compare_submission2": compare2,
        "pred": pred,
        "pred_b": pred_b,
        "pred_c": pred_c,
        "gt": gt,
        "meta": metadata_rows[video_path],
    }

    return templates.TemplateResponse(
        "video.html",
        {
            "request": request,
            "asset_version": datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S"),
            "payload": initial_payload,
            "history": history,
            "submission": submission,
            "compare_submission": compare,
            "compare_submission2": compare2,
            "source": source_eff,
            "can_edit_gt": source_eff == "test",
            "video_path": video_path,
            "gt_type_options": GT_TYPE_OPTIONS,
            "my_submission_comment": my_submission_comment,
            "my_video_comment": my_video_comment,
            "model_video_comments": model_video_comments,
            "dataset_notes": dataset_notes,
            "dataset_tags": dataset_tags,
            "dataset_user_tags": dataset_user_tags_visible,
            "default_dataset_tags": default_dataset_tags,
            "dataset_tag_labels": DATASET_TAG_LABELS,
            "dataset_tag_option_rows": dataset_tag_option_rows,
        },
    )


@app.get("/media")
def media(request: Request, video_path: str = Query(..., description="e.g. videos/abc.mp4"), source: str = "test"):
    _ = require_user(request)
    source_eff = source if source in DATA_SOURCES else "test"
    target = resolve_video_path(source_eff, video_path)
    file_size = target.stat().st_size
    if file_size <= 0:
        raise HTTPException(status_code=404, detail="Video file is empty")
    media_type = mimetypes.guess_type(str(target))[0] or "application/octet-stream"
    range_header = request.headers.get("range")

    if not range_header:
        return FileResponse(
            path=target,
            media_type=media_type,
            filename=target.name,
            headers={"Accept-Ranges": "bytes"},
        )

    m = re.match(r"bytes=(\d*)-(\d*)", range_header.strip())
    if not m:
        raise HTTPException(status_code=416, detail="Invalid Range header")

    start_s, end_s = m.groups()
    try:
        if not start_s:
        # suffix byte range: bytes=-N
            suffix = int(end_s or "0")
            if suffix <= 0:
                raise HTTPException(status_code=416, detail="Range Not Satisfiable")
            start = max(0, file_size - suffix)
            end = file_size - 1
        else:
            start = int(start_s)
            end = int(end_s) if end_s else file_size - 1
    except ValueError as exc:
        raise HTTPException(status_code=416, detail="Invalid Range header") from exc

    if end < start:
        raise HTTPException(status_code=416, detail="Range Not Satisfiable")
    if start >= file_size:
        return JSONResponse(status_code=416, content={"detail": "Range Not Satisfiable"})
    end = min(end, file_size - 1)
    chunk_size = end - start + 1

    def file_iterator(path: Path, begin: int, finish: int, block_size: int = 1024 * 1024):
        with path.open("rb") as f:
            f.seek(begin)
            remaining = finish - begin + 1
            while remaining > 0:
                data = f.read(min(block_size, remaining))
                if not data:
                    break
                remaining -= len(data)
                yield data

    headers = {
        "Accept-Ranges": "bytes",
        "Content-Range": f"bytes {start}-{end}/{file_size}",
        "Content-Length": str(chunk_size),
    }
    return StreamingResponse(file_iterator(target, start, end), status_code=206, headers=headers, media_type=media_type)


@app.post("/api/submission/upload")
async def api_submission_upload(
    request: Request,
    file: UploadFile = File(...),
    filename: str = Form(""),
    kind: str = Form("test"),
    note: str = Form(""),
    kaggle_score: str = Form(""),
) -> JSONResponse:
    user = require_user(request)
    content = await file.read()
    text = decode_uploaded_csv(content)
    validate_submission_text(text)

    target_name = sanitize_submission_filename(filename or file.filename)
    user_dir = USER_SUBMISSIONS_DIR / user
    user_dir.mkdir(parents=True, exist_ok=True)
    target = user_dir / target_name

    if target.exists():
        stem = target.stem
        suffix = target.suffix
        target = user_dir / f"{stem}_{datetime.now().strftime('%Y%m%d_%H%M%S')}{suffix}"

    target.write_text(text, encoding="utf-8")
    ref = make_personal_submission_ref(user, target.name)
    kind_eff = normalize_submission_kind(kind)
    note_text = str(note or "").strip()
    if len(note_text) > 2000:
        raise HTTPException(status_code=400, detail="note is too long")
    kaggle_score_val = parse_optional_float(kaggle_score, "kaggle_score")
    if kind_eff != "test":
        kaggle_score_val = None
    upsert_user_submission_meta(
        user,
        ref,
        kind_eff,
        original_filename=file.filename,
        note=note_text,
        kaggle_score=kaggle_score_val,
    )
    return JSONResponse(
        {
            "ok": True,
            "submission": ref,
            "kind": kind_eff,
            "note": note_text,
            "kaggle_score": kaggle_score_val,
            "saved_path": str(target.relative_to(APP_DIR)),
        }
    )


@app.get("/api/submission/global-download")
def api_submission_global_download(request: Request, submission: str) -> FileResponse:
    _ = require_user(request)
    kind, owner, filename = parse_submission_ref(submission)
    if kind == "shared":
        target = ROOT_DIR / filename
        if not target.exists() or target.suffix.lower() != ".csv":
            raise HTTPException(status_code=404, detail=f"Submission not found: {submission}")
    else:
        if owner is None:
            raise HTTPException(status_code=400, detail=f"Invalid personal submission ref: {submission}")
        user_base = (USER_SUBMISSIONS_DIR / owner).resolve()
        target = (USER_SUBMISSIONS_DIR / owner / filename).resolve()
        if not str(target).startswith(str(user_base)):
            raise HTTPException(status_code=400, detail="Invalid submission path")
        if not target.exists() or target.suffix.lower() != ".csv":
            raise HTTPException(status_code=404, detail=f"Submission not found: {submission}")

    out_name = target.name if target.name.lower().endswith(".csv") else f"{target.name}.csv"
    return FileResponse(target, media_type="text/csv", filename=out_name)


@app.get("/api/submission/download")
def api_submission_download(request: Request, submission: str) -> FileResponse:
    user = require_user(request)
    target = resolve_submission_path(submission, request_user=user)
    filename = target.name if target.name.lower().endswith(".csv") else f"{target.name}.csv"
    return FileResponse(target, media_type="text/csv", filename=filename)


@app.post("/api/submission/delete")
async def api_submission_delete(request: Request, submission: str = Form(...)) -> JSONResponse:
    user = require_user(request)
    deleted = delete_personal_submission(submission_ref=submission, request_user=user)
    return JSONResponse({"ok": True, "submission": submission, "deleted": deleted})


@app.post("/api/submission/note")
async def api_submission_note(
    request: Request,
    submission: str = Form(...),
    note: str = Form(""),
) -> JSONResponse:
    user = require_user(request)
    set_user_submission_note(submission_ref=submission, request_user=user, note=note)
    return JSONResponse({"ok": True, "submission": submission, "note": str(note or "").strip()})


@app.post("/api/submission/kaggle-score")
async def api_submission_kaggle_score(
    request: Request,
    submission: str = Form(...),
    kaggle_score: str = Form(""),
) -> JSONResponse:
    user = require_user(request)
    score = parse_optional_float(kaggle_score, "kaggle_score")
    set_user_submission_kaggle_score(submission_ref=submission, request_user=user, kaggle_score=score)
    return JSONResponse({"ok": True, "submission": submission, "kaggle_score": score})


@app.post("/api/submission/comment")
async def api_submission_comment(
    request: Request,
    submission: str = Form(...),
    comment: str = Form(""),
) -> JSONResponse:
    user = require_user(request)
    if not can_access_submission(user, submission):
        raise HTTPException(status_code=403, detail="Submission access denied")
    text = str(comment or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="comment is required")
    if len(text) > 2000:
        raise HTTPException(status_code=400, detail="comment is too long")
    set_submission_comment(user, submission, text)
    return JSONResponse({"ok": True, "submission": submission})


@app.post("/api/submission/video-comment")
async def api_submission_video_comment(
    request: Request,
    submission: str = Form(...),
    video_path: str = Form(...),
    comment: str = Form(""),
) -> JSONResponse:
    user = require_user(request)
    if not can_access_submission(user, submission):
        raise HTTPException(status_code=403, detail="Submission access denied")
    path = str(video_path or "").strip()
    text = str(comment or "").strip()
    if not path:
        raise HTTPException(status_code=400, detail="video_path is required")
    if not text:
        raise HTTPException(status_code=400, detail="comment is required")
    if len(text) > 2000:
        raise HTTPException(status_code=400, detail="comment is too long")
    set_submission_video_comment(user, submission, path, text)
    return JSONResponse({"ok": True, "submission": submission, "video_path": path})


@app.post("/api/dataset/video-note")
async def api_dataset_video_note(
    request: Request,
    source: str = Form("test"),
    video_path: str = Form(...),
    comment: str = Form(""),
) -> JSONResponse:
    user = require_user(request)
    source_eff = source if source in DATA_SOURCES else "test"
    path = str(video_path or "").strip()
    text = str(comment or "").strip()
    if not path:
        raise HTTPException(status_code=400, detail="video_path is required")
    if not text:
        raise HTTPException(status_code=400, detail="comment is required")
    if len(text) > 2000:
        raise HTTPException(status_code=400, detail="comment is too long")

    valid_paths = {r["path"] for r in load_metadata(source_eff)}
    if path not in valid_paths:
        raise HTTPException(status_code=404, detail="Unknown video_path")

    add_dataset_video_note(source_eff, path, user, text)
    _, tags = list_dataset_video_notes(source_eff, path)
    return JSONResponse({"ok": True, "source": source_eff, "video_path": path, "tags": tags})


@app.post("/api/dataset/video-tag/add")
async def api_dataset_video_tag_add(
    request: Request,
    source: str = Form("test"),
    video_path: str = Form(...),
    tag: str = Form(""),
) -> JSONResponse:
    user = require_user(request)
    source_eff = source if source in DATA_SOURCES else "test"
    path = str(video_path or "").strip()
    tag_norm = normalize_single_tag(tag)
    if not path:
        raise HTTPException(status_code=400, detail="video_path is required")
    if not tag_norm:
        raise HTTPException(status_code=400, detail="tag is required")

    valid_paths = {r["path"] for r in load_metadata(source_eff)}
    if path not in valid_paths:
        raise HTTPException(status_code=404, detail="Unknown video_path")

    add_dataset_video_tag(source_eff, path, user, tag_norm)
    _, tags = list_dataset_video_notes(source_eff, path)
    return JSONResponse({"ok": True, "source": source_eff, "video_path": path, "tags": tags})


@app.post("/api/dataset/video-tag/delete")
async def api_dataset_video_tag_delete(
    request: Request,
    source: str = Form("test"),
    video_path: str = Form(...),
    tag: str = Form(""),
) -> JSONResponse:
    _ = require_user(request)
    source_eff = source if source in DATA_SOURCES else "test"
    path = str(video_path or "").strip()
    tag_norm = normalize_single_tag(tag)
    if not path:
        raise HTTPException(status_code=400, detail="video_path is required")
    if not tag_norm:
        raise HTTPException(status_code=400, detail="tag is required")

    valid_paths = {r["path"] for r in load_metadata(source_eff)}
    if path not in valid_paths:
        raise HTTPException(status_code=404, detail="Unknown video_path")

    delete_dataset_video_tag(source_eff, path, tag_norm)
    _, tags = list_dataset_video_notes(source_eff, path)
    return JSONResponse({"ok": True, "source": source_eff, "video_path": path, "tags": tags})


@app.get("/api/score")
def api_score(request: Request, submission: str, sigma_t: float = 2.0, sigma_s: float = 0.15) -> JSONResponse:
    user = require_user(request)
    if not can_access_submission(user, submission):
        raise HTTPException(status_code=403, detail="Submission access denied")
    if sigma_t <= 0 or sigma_s <= 0:
        raise HTTPException(status_code=400, detail="sigma must be positive")

    sub_map = read_submission_map(submission, request_user=user)
    gt_map = get_gt_map()

    agg = aggregate_submission_score(sub_map, gt_map, sigma_t=sigma_t, sigma_s=sigma_s)
    used = agg["used"]

    if used == 0:
        return JSONResponse(
            {
                "submission": submission,
                "used": 0,
                "message": "완성된 예상 GT가 없습니다.",
            }
        )

    return JSONResponse(
        {
            "submission": submission,
            "used": used,
            "T": agg["T"],
            "S": agg["S"],
            "C": agg["C"],
            "H": agg["H"],
            "sigma_t": sigma_t,
            "sigma_s": sigma_s,
            "formula": "exp(-0.5*(error/sigma)^2), C=top1, final=harmonic mean of component means",
        }
    )


@app.get("/api/scoreboard")
def api_scoreboard(request: Request, sigma_t: float = 2.0, sigma_s: float = 0.15) -> JSONResponse:
    user = require_user(request)
    if sigma_t <= 0 or sigma_s <= 0:
        raise HTTPException(status_code=400, detail="sigma must be positive")

    gt_map = get_gt_map()
    rows: list[dict[str, Any]] = []
    for sub_name in get_allowed_submissions(user):
        sub_map = read_submission_map(sub_name, request_user=user)
        agg = aggregate_submission_score(sub_map, gt_map, sigma_t=sigma_t, sigma_s=sigma_s)
        rows.append(
            {
                "submission": sub_name,
                "used": agg["used"],
                "T": agg["T"],
                "S": agg["S"],
                "C": agg["C"],
                "H": agg["H"],
            }
        )

    rows.sort(key=lambda r: (r["H"] is not None, r["H"] if r["H"] is not None else -1.0), reverse=True)
    return JSONResponse({"sigma_t": sigma_t, "sigma_s": sigma_s, "rows": rows})


@app.get("/api/gt/export")
def api_export_gt(request: Request, source: str = "test") -> StreamingResponse:
    _ = require_user(request)
    source_eff = (source or "test").strip().lower()
    if source_eff != "test":
        raise HTTPException(status_code=400, detail="GT CSV export is supported only for test source")

    with get_db() as conn:
        cur = conn.execute(
            """
            SELECT video_path, accident_time, center_x, center_y, type, note, updated_by, updated_at
            FROM expected_gt
            ORDER BY video_path ASC
            """
        )
        rows = cur.fetchall()

    sio = io.StringIO()
    writer = csv.DictWriter(
        sio,
        fieldnames=["video_path", "accident_time", "center_x", "center_y", "type", "note", "updated_by", "updated_at"],
    )
    writer.writeheader()
    for row in rows:
        writer.writerow(dict(row))

    filename = f"expected_gt_test_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    return StreamingResponse(
        iter([sio.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.get("/api/submission/train-example")
def api_submission_train_example(request: Request) -> StreamingResponse:
    _ = require_user(request)
    train_rows = load_train_metadata()
    sample_path = train_rows[0]["path"] if train_rows else "videos/sample_train.mp4"

    sio = io.StringIO()
    writer = csv.DictWriter(
        sio,
        fieldnames=["path", "accident_time", "center_x", "center_y", "type"],
    )
    writer.writeheader()
    writer.writerow(
        {
            "path": sample_path,
            "accident_time": 1.23,
            "center_x": 0.50,
            "center_y": 0.50,
            "type": "rear-end",
        }
    )

    filename = "train_submission_example.csv"
    return StreamingResponse(
        iter([sio.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.post("/api/gt")
async def api_upsert_gt(request: Request) -> JSONResponse:
    _ = require_user(request)
    body = await request.json()

    source = str(body.get("source", "test")).strip() or "test"
    video_path = str(body.get("video_path", "")).strip()
    editor = str(body.get("editor", "anonymous")).strip() or "anonymous"
    gt_type = str(body.get("type", "")).strip()
    note = str(body.get("note", "")).strip()
    raw_base_updated_at = body.get("base_updated_at")
    if raw_base_updated_at is None:
        base_updated_at = None
    else:
        base_updated_at_str = str(raw_base_updated_at).strip()
        base_updated_at = base_updated_at_str if base_updated_at_str and base_updated_at_str.lower() != "null" else None

    if not video_path:
        raise HTTPException(status_code=400, detail="video_path is required")
    if source != "test":
        raise HTTPException(status_code=400, detail="GT save is allowed only for test source")
    if gt_type and gt_type not in VALID_TYPES:
        raise HTTPException(status_code=400, detail=f"Invalid type: {gt_type}")

    all_paths = {r["path"] for r in load_test_metadata()}
    if video_path not in all_paths:
        raise HTTPException(status_code=404, detail="Unknown video_path")

    record = {
        "video_path": video_path,
        "accident_time": parse_float(body.get("accident_time")),
        "center_x": parse_float(body.get("center_x")),
        "center_y": parse_float(body.get("center_y")),
        "type": gt_type or None,
        "note": note or None,
        "updated_by": editor,
        "updated_at": utc_now_iso(),
    }

    if record["center_x"] is not None and not (0.0 <= record["center_x"] <= 1.0):
        raise HTTPException(status_code=400, detail="center_x must be in [0,1]")
    if record["center_y"] is not None and not (0.0 <= record["center_y"] <= 1.0):
        raise HTTPException(status_code=400, detail="center_y must be in [0,1]")

    with get_db() as conn:
        cur = conn.execute("SELECT * FROM expected_gt WHERE video_path = ?", (video_path,))
        old = cur.fetchone()
        old_dict = dict(old) if old else None
        if old is not None and base_updated_at and old_dict.get("updated_at") != base_updated_at:
            raise HTTPException(
                status_code=409,
                detail={
                    "message": "충돌 감지: 다른 팀원이 먼저 수정했습니다. 새로고침 후 다시 저장하세요.",
                    "current": old_dict,
                },
            )

        conn.execute(
            """
            INSERT INTO expected_gt(video_path, accident_time, center_x, center_y, type, note, updated_by, updated_at)
            VALUES (:video_path, :accident_time, :center_x, :center_y, :type, :note, :updated_by, :updated_at)
            ON CONFLICT(video_path) DO UPDATE SET
                accident_time=excluded.accident_time,
                center_x=excluded.center_x,
                center_y=excluded.center_y,
                type=excluded.type,
                note=excluded.note,
                updated_by=excluded.updated_by,
                updated_at=excluded.updated_at
            """,
            record,
        )

        conn.execute(
            """
            INSERT INTO gt_history(video_path, edited_by, edited_at, before_json, after_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                video_path,
                editor,
                record["updated_at"],
                json.dumps(old_dict, ensure_ascii=True),
                json.dumps(record, ensure_ascii=True),
            ),
        )
        conn.commit()

    return JSONResponse({"ok": True, "record": record})


@app.get("/api/gt/history")
def api_gt_history(request: Request, video_path: str, source: str = "test") -> JSONResponse:
    _ = require_user(request)
    if source != "test":
        return JSONResponse({"video_path": video_path, "items": []})
    return JSONResponse({"video_path": video_path, "items": get_history(video_path)})


@app.get("/healthz")
def healthz() -> JSONResponse:
    return JSONResponse({"status": "ok", "time": utc_now_iso()})


if __name__ == "__main__":
    import uvicorn

    host = os.environ.get("SD_HOST", "0.0.0.0").strip() or "0.0.0.0"
    raw_port = os.environ.get("SD_PORT", "18080").strip()
    try:
        port = int(raw_port)
    except ValueError:
        port = 18080

    reload_enabled = os.environ.get("SD_RELOAD", "0").strip().lower() in {"1", "true", "yes", "on"}
    raw_workers = os.environ.get("SD_WORKERS", "1").strip()
    try:
        workers = max(1, int(raw_workers))
    except ValueError:
        workers = 1

    proxy_headers = os.environ.get("SD_PROXY_HEADERS", "1").strip().lower() in {"1", "true", "yes", "on"}
    forwarded_allow_ips = os.environ.get("SD_FORWARDED_ALLOW_IPS", "127.0.0.1").strip() or "127.0.0.1"

    uvicorn.run(
        "app:app",
        host=host,
        port=port,
        reload=reload_enabled,
        workers=(1 if reload_enabled else workers),
        proxy_headers=proxy_headers,
        forwarded_allow_ips=forwarded_allow_ips,
    )
