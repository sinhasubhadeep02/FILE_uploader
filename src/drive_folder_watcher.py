#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Drive Folder Watcher (Windows)
--------------------------------
Continuously monitors a local folder and uploads newly created files to
a specific Google Drive folder using Google Drive API v3.

Compatible with both Python and PyInstaller-compiled .exe versions.
"""

import os
import sys
import time
import json
import queue
import threading
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileMovedEvent
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# ===============================
#      PATH INITIALIZATION
# ===============================

# Handle both .py and PyInstaller .exe cases
if getattr(sys, 'frozen', False):
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

CONFIG_PATH = os.path.join(BASE_DIR, "config.json")
CREDENTIALS_PATH = os.path.join(BASE_DIR, "credentials.json")
TOKEN_PATH = os.path.join(BASE_DIR, "token.json")
LOG_FILE_PATH = os.path.join(BASE_DIR, "drive_watcher.log")

# ===============================
#       LOGGING SETUP
# ===============================

logger = logging.getLogger("drive_watcher")
logger.setLevel(logging.INFO)

_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

_file = RotatingFileHandler(LOG_FILE_PATH, maxBytes=2 * 1024 * 1024, backupCount=3, encoding="utf-8")
_file.setFormatter(_formatter)
logger.addHandler(_file)

_console = logging.StreamHandler(sys.stdout)
_console.setFormatter(_formatter)
logger.addHandler(_console)

logger.info("Drive Watcher starting...")

# ===============================
#       LOAD CONFIG
# ===============================

if not os.path.exists(CONFIG_PATH):
    logger.error(f"Config file not found at: {CONFIG_PATH}")
    sys.exit(1)

try:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = json.load(f)
        WATCH_FOLDER_PATH = config.get("WATCH_FOLDER_PATH")
        GOOGLE_DRIVE_FOLDER_ID = config.get("GOOGLE_DRIVE_FOLDER_ID")
except Exception as e:
    logger.error(f"Failed to read config file: {e}")
    sys.exit(1)

if not WATCH_FOLDER_PATH or not os.path.isdir(WATCH_FOLDER_PATH):
    logger.error(f"Invalid or missing WATCH_FOLDER_PATH: {WATCH_FOLDER_PATH}")
    sys.exit(1)

if not GOOGLE_DRIVE_FOLDER_ID:
    logger.error("Missing GOOGLE_DRIVE_FOLDER_ID in config.json")
    sys.exit(1)

# ===============================
#      CONSTANTS
# ===============================

SCOPES = ["https://www.googleapis.com/auth/drive.file"]
STABILIZE_SECONDS = 2.0
STABILIZE_POLL_INTERVAL = 0.5
MAX_RETRIES = 5
BACKOFF_BASE_SECONDS = 2
EXCLUDED_SUFFIXES = (".tmp", ".part", ".crdownload")
EXCLUDED_PREFIXES = ("~",)

# ===============================
#      AUTHENTICATION
# ===============================

def get_drive_service():
    """Return an authorized Google Drive API service."""
    creds = None

    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                logger.info("Refreshing OAuth token...")
                creds.refresh(Request())
            except Exception as e:
                logger.warning(f"Token refresh failed: {e}, removing token.json")
                try:
                    os.remove(TOKEN_PATH)
                except Exception:
                    pass
                creds = None

        if not creds:
            if not os.path.exists(CREDENTIALS_PATH):
                logger.error(f"credentials.json not found at {CREDENTIALS_PATH}. "
                             "Download it from Google Cloud Console and place it next to this script.")
                sys.exit(1)
            logger.info("Launching browser for Google OAuth consent...")
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_PATH, SCOPES)
            creds = flow.run_local_server(port=0)

        with open(TOKEN_PATH, "w", encoding="utf-8") as token:
            token.write(creds.to_json())
        logger.info("OAuth token saved to token.json")

    return build("drive", "v3", credentials=creds, cache_discovery=False)

# ===============================
#      FILE STABILITY CHECK
# ===============================

def is_excluded(path):
    name = os.path.basename(path)
    return name.startswith(EXCLUDED_PREFIXES) or name.lower().endswith(EXCLUDED_SUFFIXES)

def wait_until_stable(path):
    """Wait until the file size stops changing for STABILIZE_SECONDS."""
    logger.info(f"Waiting for file to finish writing: {path}")
    last_size = -1
    stable_since = None

    while True:
        if not os.path.exists(path):
            logger.warning(f"File disappeared before upload: {path}")
            return False

        try:
            size = os.path.getsize(path)
        except OSError:
            size = -1

        if size == last_size and size >= 0:
            try:
                with open(path, "rb"):
                    pass
                if stable_since is None:
                    stable_since = time.time()
                elif time.time() - stable_since >= STABILIZE_SECONDS:
                    logger.info(f"File stabilized: {path} ({size} bytes)")
                    return True
            except Exception:
                stable_since = None
        else:
            stable_since = None

        last_size = size
        time.sleep(STABILIZE_POLL_INTERVAL)

# ===============================
#      UPLOAD LOGIC
# ===============================

def upload_file_with_retries(service, local_path, drive_folder_id):
    filename = os.path.basename(local_path)
    media = MediaFileUpload(local_path, resumable=True)
    body = {"name": filename, "parents": [drive_folder_id]}

    for attempt in range(MAX_RETRIES + 1):
        try:
            logger.info(f"Uploading '{filename}' (attempt {attempt + 1})...")
            request = service.files().create(body=body, media_body=media, fields="id, name")
            response = None
            while response is None:
                status, response = request.next_chunk()
            drive_id = response.get("id")
            logger.info(f"Upload succeeded: {filename} -> {drive_id}")
            return drive_id
        except HttpError as e:
            logger.warning(f"HttpError during upload: {e}")
        except Exception as e:
            logger.warning(f"Error during upload: {e}")
        sleep_for = BACKOFF_BASE_SECONDS * (2 ** attempt)
        logger.info(f"Retrying in {sleep_for:.1f} seconds...")
        time.sleep(sleep_for)
    logger.error(f"Upload permanently failed: {filename}")
    return None

# ===============================
#     FILE EVENT HANDLER
# ===============================

class NewFileHandler(FileSystemEventHandler):
    def __init__(self, work_q):
        self.work_q = work_q

    def on_created(self, event):
        if isinstance(event, FileCreatedEvent) and not event.is_directory:
            path = event.src_path
            if not is_excluded(path):
                logger.info(f"Detected new file: {path}")
                self.work_q.put(path)

    def on_moved(self, event):
        if isinstance(event, FileMovedEvent) and not event.is_directory:
            dest = event.dest_path
            if not is_excluded(dest):
                logger.info(f"Detected file moved into folder: {dest}")
                self.work_q.put(dest)

# ===============================
#          WORKER LOOP
# ===============================

def worker_loop(work_q, stop_event):
    service = None
    while not stop_event.is_set():
        try:
            path = work_q.get(timeout=0.5)
        except queue.Empty:
            continue

        if not os.path.isfile(path):
            logger.info(f"Skipping missing file: {path}")
            work_q.task_done()
            continue

        if is_excluded(path):
            logger.info(f"Ignored excluded file: {path}")
            work_q.task_done()
            continue

        if service is None:
            try:
                service = get_drive_service()
            except Exception as e:
                logger.error(f"Failed to create Drive service: {e}")
                work_q.task_done()
                time.sleep(5)
                continue

        if not wait_until_stable(path):
            work_q.task_done()
            continue

        drive_id = upload_file_with_retries(service, path, GOOGLE_DRIVE_FOLDER_ID)
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if drive_id:
            logger.info(f"[Uploaded] {os.path.basename(path)} at {ts}")
        else:
            logger.error(f"[Failed] {os.path.basename(path)} at {ts}")
        work_q.task_done()

# ===============================
#             MAIN
# ===============================

def main():
    logger.info("Starting Drive Folder Watcher...")
    logger.info(f"Watching folder: {WATCH_FOLDER_PATH}")
    logger.info(f"Target Drive Folder ID: {GOOGLE_DRIVE_FOLDER_ID}")

    work_q = queue.Queue()
    stop_event = threading.Event()

    t = threading.Thread(target=worker_loop, args=(work_q, stop_event), daemon=True)
    t.start()

    observer = Observer()
    observer.schedule(NewFileHandler(work_q), WATCH_FOLDER_PATH, recursive=False)
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
