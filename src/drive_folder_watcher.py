#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Drive Folder Watcher (Windows)
-------------------------------
Watches a folder and uploads new files to Google Drive using Drive API v3.
Works with both Python and PyInstaller (.exe) builds.
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
import socket
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileMovedEvent
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# ================================================================
#                PATH INITIALIZATION
# ================================================================

if getattr(sys, 'frozen', False):
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

CONFIG_PATH = os.path.join(BASE_DIR, "config.json")
CREDENTIALS_PATH = os.path.join(BASE_DIR, "credentials.json")
TOKEN_PATH = os.path.join(BASE_DIR, "token.json")
LOG_FILE_PATH = os.path.join(BASE_DIR, "drive_watcher.log")

# ================================================================
#                LOGGING SETUP
# ================================================================

logger = logging.getLogger("drive_watcher")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

file_handler = RotatingFileHandler(LOG_FILE_PATH, maxBytes=2 * 1024 * 1024, backupCount=3, encoding="utf-8")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

logger.info("Drive Watcher starting...")

# ================================================================
#                CONFIGURATION LOADING
# ================================================================

if not os.path.exists(CONFIG_PATH):
    logger.error(f"Missing config.json at {CONFIG_PATH}")
    sys.exit(1)

try:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = json.load(f)
        WATCH_FOLDER_PATH = config.get("WATCH_FOLDER_PATH")
        GOOGLE_DRIVE_FOLDER_ID = config.get("GOOGLE_DRIVE_FOLDER_ID")
except Exception as e:
    logger.error(f"Error reading config.json: {e}")
    sys.exit(1)

if not WATCH_FOLDER_PATH or not os.path.isdir(WATCH_FOLDER_PATH):
    logger.error(f"Invalid WATCH_FOLDER_PATH: {WATCH_FOLDER_PATH}")
    sys.exit(1)
if not GOOGLE_DRIVE_FOLDER_ID:
    logger.error("Missing GOOGLE_DRIVE_FOLDER_ID in config.json")
    sys.exit(1)

# ================================================================
#                CONSTANTS
# ================================================================

SCOPES = ["https://www.googleapis.com/auth/drive.file"]
STABILIZE_SECONDS = 2.0
STABILIZE_POLL_INTERVAL = 0.5
MAX_RETRIES = 5
BACKOFF_BASE_SECONDS = 2
EXCLUDED_SUFFIXES = (".tmp", ".part", ".crdownload")
EXCLUDED_PREFIXES = ("~",)

# ================================================================
#                HELPER FUNCTIONS
# ================================================================

def wait_for_internet(timeout=5):
    """Wait until internet is available."""
    while True:
        try:
            socket.create_connection(("www.google.com", 80), timeout=timeout)
            return True
        except OSError:
            logger.warning("No internet connection. Retrying in 10 seconds...")
            time.sleep(10)

def is_excluded(path):
    name = os.path.basename(path)
    return name.startswith(EXCLUDED_PREFIXES) or name.lower().endswith(EXCLUDED_SUFFIXES)

def wait_until_stable(path):
    """Wait until file stops changing size for STABILIZE_SECONDS."""
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

# ================================================================
#                GOOGLE DRIVE AUTHENTICATION
# ================================================================

def get_drive_service():
    creds = None
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
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
                logger.error(f"credentials.json not found at {CREDENTIALS_PATH}")
                sys.exit(1)
            logger.info("Launching browser for Google OAuth consent...")
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_PATH, SCOPES)
            creds = flow.run_local_server(port=0)

        with open(TOKEN_PATH, "w", encoding="utf-8") as token:
            token.write(creds.to_json())
        logger.info("OAuth token saved to token.json")

    return build("drive", "v3", credentials=creds, cache_discovery=False)

# ================================================================
#                UPLOAD LOGIC (IMPROVED)
# ================================================================

def upload_file_with_retries(service, local_path, drive_folder_id):
    filename = os.path.basename(local_path)
    media = MediaFileUpload(local_path, resumable=True, chunksize=10 * 1024 * 1024)  # 10MB chunks
    body = {"name": filename, "parents": [drive_folder_id]}

    for attempt in range(MAX_RETRIES + 1):
        try:
            wait_for_internet()
            logger.info(f"Uploading '{filename}' (attempt {attempt + 1})...")
            request = service.files().create(body=body, media_body=media, fields="id, name")
            response = None
            retry_chunk = 0

            while response is None:
                try:
                    status, response = request.next_chunk()
                except OSError as e:
                    if e.errno == 10053:
                        logger.warning("Connection aborted (WinError 10053). Retrying this chunk...")
                    else:
                        logger.warning(f"OSError during upload: {e}")
                    retry_chunk += 1
                    if retry_chunk > 5:
                        raise
                    time.sleep(5)
                    continue
                except Exception as e:
                    logger.warning(f"Error during upload chunk: {e}")
                    retry_chunk += 1
                    if retry_chunk > 5:
                        raise
                    time.sleep(5)
                    continue

            drive_id = response.get("id")
            logger.info(f"Upload succeeded: {filename} -> Drive ID: {drive_id}")
            return drive_id

        except HttpError as e:
            logger.warning(f"HttpError during upload: {e}")
        except Exception as e:
            logger.warning(f"Error during upload: {e}")

        sleep_for = BACKOFF_BASE_SECONDS * (2 ** attempt)
        logger.info(f"Retrying in {sleep_for:.1f} seconds...")
        time.sleep(sleep_for)

    logger.error(f"Upload permanently failed after retries: {filename}")
    return None

# ================================================================
#                FILE EVENT HANDLER
# ================================================================

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

# ================================================================
#                WORKER THREAD
# ================================================================

def worker_loop(work_q, stop_event):
    service = None
    while not stop_event.is_set():
        try:
            path = work_q.get(timeout=0.5)
        except queue.Empty:
            continue

        if not os.path.isfile(path):
            work_q.task_done()
            continue

        if is_excluded(path):
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

# ================================================================
#                MAIN PROGRAM
# ================================================================

def main():
    logger.info("Starting Drive Folder Watcher...")
    logger.info(f"Watching: {WATCH_FOLDER_PATH}")
    logger.info(f"Drive Folder ID: {GOOGLE_DRIVE_FOLDER_ID}")

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
