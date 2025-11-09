#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Drive Folder Watcher (Windows)
------------------------------
Continuously watches a local folder and uploads newly created or added files
to a specific Google Drive folder using Google Drive API v3.

Features:
- Uses watchdog for reliable folder monitoring (create/move events).
- Waits until files are fully written (stabilized) before upload.
- OAuth2 with local token caching (token.json).
- Detailed logging (console + rotating log file).
- Robust retries with exponential backoff.
- Ignores common temp/incomplete files (.tmp, .part, .crdownload, ~prefix).
- Handles files moved into the watched folder.
"""

import io
import os
import sys
import time
import json
import queue
import threading
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from typing import Optional

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileMovedEvent

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# ============================
# ====== CONFIGURATION =======
# ============================

# Path to config file (same folder as script or EXE)
BASE_DIR = os.path.dirname(os.path.abspath(sys.executable if getattr(sys, 'frozen', False) else __file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config.json")

# Default values in case config.json is missing
WATCH_FOLDER_PATH = None
GOOGLE_DRIVE_FOLDER_ID = None

# Load config file
try:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = json.load(f)
        WATCH_FOLDER_PATH = config.get("WATCH_FOLDER_PATH")
        GOOGLE_DRIVE_FOLDER_ID = config.get("GOOGLE_DRIVE_FOLDER_ID")
except FileNotFoundError:
    print(f"[ERROR] Config file not found at: {CONFIG_PATH}")
    sys.exit(1)
except Exception as e:
    print(f"[ERROR] Failed to load config: {e}")
    sys.exit(1)

# Optional: log file path (will be created if missing)
LOG_FILE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "drive_watcher.log")

# How long a file must remain unchanged before upload (seconds)
STABILIZE_SECONDS = 2.0

# Poll interval while checking for stabilization (seconds)
STABILIZE_POLL_INTERVAL = 0.5

# Max upload retries and base backoff (seconds)
MAX_RETRIES = 5
BACKOFF_BASE_SECONDS = 2

# OAuth scopes for Google Drive file creation
SCOPES = ["https://www.googleapis.com/auth/drive.file"]

# Exclude patterns: filenames to ignore (temp/incomplete files)
EXCLUDED_SUFFIXES = (".tmp", ".part", ".crdownload")
EXCLUDED_PREFIXES = ("~",)

# ============================
# ====== LOGGING SETUP =======
# ============================

logger = logging.getLogger("drive_watcher")
logger.setLevel(logging.INFO)

_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

_console = logging.StreamHandler(sys.stdout)
_console.setFormatter(_formatter)
console_level = logging.INFO
_console.setLevel(console_level)

_file = RotatingFileHandler(LOG_FILE_PATH, maxBytes=2 * 1024 * 1024, backupCount=3, encoding="utf-8")
_file.setFormatter(_formatter)
_file.setLevel(logging.INFO)

if not logger.handlers:
    logger.addHandler(_console)
    logger.addHandler(_file)

# ============================
# ====== AUTH FUNCTIONS ======
# ============================

def get_drive_service() -> "googleapiclient.discovery.Resource":
    """
    Returns an authorized Drive API service client.
    Stores/refreshes OAuth token in token.json alongside credentials.json.
    """
    creds = None
    token_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "token.json")
    creds_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "credentials.json")

    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    # Refresh or run local server flow if needed
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                logger.info("Refreshing OAuth token...")
                creds.refresh(Request())
            except Exception as e:
                logger.warning(f"Token refresh failed: {e}. Removing token.json and retrying OAuth flow.")
                try:
                    os.remove(token_path)
                except Exception:
                    pass
                creds = None

        if not creds:
            if not os.path.exists(creds_path):
                raise FileNotFoundError(
                    f"credentials.json not found at {creds_path}. "
                    "Download it from Google Cloud Console and place it next to this script."
                )
            logger.info("Launching browser for Google OAuth consent...")
            flow = InstalledAppFlow.from_client_secrets_file(creds_path, SCOPES)
            creds = flow.run_local_server(port=0)

        # Save the credentials for next run
        with open(token_path, "w", encoding="utf-8") as token:
            token.write(creds.to_json())
        logger.info("OAuth token saved to token.json")

    return build("drive", "v3", credentials=creds, cache_discovery=False)

# ============================
# ====== UPLOAD LOGIC ========
# ============================

def is_excluded(path: str) -> bool:
    name = os.path.basename(path)
    if name.startswith(EXCLUDED_PREFIXES):
        return True
    if name.lower().endswith(EXCLUDED_SUFFIXES):
        return True
    return False

def wait_until_stable(path: str, min_stable_seconds: float = STABILIZE_SECONDS) -> bool:
    """
    Poll the file until its size stops changing for min_stable_seconds AND
    it can be opened for reading. Returns True if stable; False if file disappeared.
    """
    logger.info(f"Waiting for file to finish writing: {path}")
    last_size = -1
    stable_since: Optional[float] = None

    while True:
        if not os.path.exists(path):
            logger.warning(f"File disappeared before upload: {path}")
            return False

        try:
            size = os.path.getsize(path)
        except OSError:
            size = -1

        if size == last_size and size >= 0:
            # attempt to open to ensure lock-free read
            try:
                with open(path, "rb"):
                    pass
                if stable_since is None:
                    stable_since = time.time()
                else:
                    if time.time() - stable_since >= min_stable_seconds:
                        logger.info(f"File stabilized: {path} ({size} bytes)")
                        return True
            except Exception:
                # still locked; reset timer
                stable_since = None
        else:
            # size changed; reset timer
            stable_since = None

        last_size = size
        time.sleep(STABILIZE_POLL_INTERVAL)

def upload_file_with_retries(service, local_path: str, drive_folder_id: str) -> Optional[str]:
    """
    Upload the file to Drive (into drive_folder_id) with retries.
    Returns the created file's Drive ID, or None on failure.
    """
    filename = os.path.basename(local_path)

    media = MediaFileUpload(local_path, resumable=True)
    body = {
        "name": filename,
        "parents": [drive_folder_id],
    }

    attempt = 0
    while attempt <= MAX_RETRIES:
        try:
            logger.info(f"Uploading '{filename}' to Drive (attempt {attempt+1}/{MAX_RETRIES+1})...")
            request = service.files().create(body=body, media_body=media, fields="id, name")
            response = None
            while response is None:
                status, response = request.next_chunk()
                # status is None for non-resumable / quick uploads; ignore
            drive_id = response.get("id")
            logger.info(f"Upload succeeded: {filename} -> Drive ID {drive_id}")
            return drive_id

        except HttpError as e:
            code = getattr(e, "status_code", None) or (e.resp.status if hasattr(e, "resp") else None)
            logger.warning(f"HttpError during upload: {e} (status={code})")
        except Exception as e:
            logger.warning(f"Error during upload: {e}")

        # backoff
        sleep_for = BACKOFF_BASE_SECONDS * (2 ** attempt)
        logger.info(f"Retrying in {sleep_for:.1f} seconds...")
        time.sleep(sleep_for)
        attempt += 1

    logger.error(f"Upload permanently failed after {MAX_RETRIES+1} attempts: {filename}")
    return None

# ============================
# ====== WATCH HANDLER =======
# ============================

class NewFileHandler(FileSystemEventHandler):
    """
    Handles new files created or moved into the watch folder.
    Uses a worker thread to serialize uploads.
    """
    def __init__(self, work_q: queue.Queue, drive_folder_id: str):
        super().__init__()
        self.work_q = work_q
        self.drive_folder_id = drive_folder_id

    def on_created(self, event):
        if isinstance(event, FileCreatedEvent) and not event.is_directory:
            path = event.src_path
            if not is_excluded(path):
                logger.info(f"Detected new file: {path}")
                self.work_q.put(path)

    def on_moved(self, event):
        # If a file is moved into the directory, treat it like a new file
        if isinstance(event, FileMovedEvent) and not event.is_directory:
            dest = event.dest_path
            if not is_excluded(dest):
                logger.info(f"Detected file moved into folder: {dest}")
                self.work_q.put(dest)

# ============================
# ====== MAIN WORKER =========
# ============================

def worker_loop(work_q: queue.Queue, drive_folder_id: str, stop_event: threading.Event):
    service = None
    while not stop_event.is_set():
        try:
            path = work_q.get(timeout=0.5)
        except queue.Empty:
            continue

        if not os.path.isfile(path):
            logger.info(f"Skipping non-file or missing path: {path}")
            work_q.task_done()
            continue

        if is_excluded(path):
            logger.info(f"Ignored (temp or excluded): {path}")
            work_q.task_done()
            continue

        # Ensure Drive service
        if service is None:
            try:
                service = get_drive_service()
            except Exception as e:
                logger.error(f"Failed to create Drive service: {e}")
                work_q.task_done()
                # Wait before trying again for next file
                time.sleep(5)
                continue

        # Wait until file is fully written
        if not wait_until_stable(path):
            work_q.task_done()
            continue

        # Upload
        drive_id = upload_file_with_retries(service, path, drive_folder_id)

        # Log completion
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if drive_id:
            logger.info(f"[Uploaded] {os.path.basename(path)} at {timestamp} (Drive ID: {drive_id})")
        else:
            logger.error(f"[Failed] {os.path.basename(path)} at {timestamp}")

        work_q.task_done()

def main():
    # Basic validation
    if not os.path.isdir(WATCH_FOLDER_PATH):
        logger.error(f"WATCH_FOLDER_PATH does not exist or is not a directory: {WATCH_FOLDER_PATH}")
        sys.exit(1)

    if not GOOGLE_DRIVE_FOLDER_ID or GOOGLE_DRIVE_FOLDER_ID.strip().lower() == "your_drive_folder_id_here":
        logger.error("Please set a valid GOOGLE_DRIVE_FOLDER_ID at the top of the script.")
        sys.exit(1)

    logger.info("Starting Drive Folder Watcher...")
    logger.info(f"Watching: {WATCH_FOLDER_PATH}")
    logger.info(f"Target Drive Folder ID: {GOOGLE_DRIVE_FOLDER_ID}")

    work_q: queue.Queue = queue.Queue()
    stop_event = threading.Event()

    # Start worker thread
    t = threading.Thread(target=worker_loop, args=(work_q, GOOGLE_DRIVE_FOLDER_ID, stop_event), daemon=True)
    t.start()

    # Start watchdog observer
    event_handler = NewFileHandler(work_q, GOOGLE_DRIVE_FOLDER_ID)
    observer = Observer()
    observer.schedule(event_handler, WATCH_FOLDER_PATH, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping watcher...")
        stop_event.set()
        observer.stop()
    observer.join()
    logger.info("Exited cleanly.")

if __name__ == "__main__":
    main()
