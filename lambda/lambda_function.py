import os
import io
import sys
import time
import json
import logging
import zipfile
import urllib.request
from datetime import datetime
import random
import socket
import urllib.error
import shutil                      # NEW: for file ops
from pathlib import Path           # NEW: for /tmp paths
import hashlib                     # NEW: for SHA-256 checksums  <-- CHANGED (added)

import boto3
from botocore.exceptions import ClientError

# =========================
# Config from environment
# =========================

BUCKET_NAME = os.environ.get("BUCKET_NAME")
DATA_PREFIX = os.environ.get("DATA_PREFIX", "odis/raw/")
LOG_PREFIX  = os.environ.get("LOG_PREFIX", "odis/logs/")
ZIP_URL     = os.environ.get("ZIP_URL")
LOG_LEVEL   = os.environ.get("LOG_LEVEL", "INFO").upper()

HTTP_TIMEOUT_SECONDS    = int(os.environ.get("HTTP_TIMEOUT_SECONDS", "30"))  # per-op socket timeout
MAX_HTTP_ATTEMPTS       = int(os.environ.get("MAX_HTTP_ATTEMPTS", "5"))
BACKOFF_BASE_SECONDS    = float(os.environ.get("BACKOFF_BASE_SECONDS", "0.5"))
BACKOFF_CAP_SECONDS     = float(os.environ.get("BACKOFF_CAP_SECONDS", "8"))
TOTAL_DOWNLOAD_BUDGET_S = float(os.environ.get("TOTAL_DOWNLOAD_BUDGET_S", "300"))  # 5 minutes

if not BUCKET_NAME:
    raise RuntimeError("Missing required environment variable: BUCKET_NAME")
if not ZIP_URL:
    raise RuntimeError("Missing required environment variable: ZIP_URL")

s3 = boto3.client("s3")

# =========================
# Logging setup
# =========================

log_stream = io.StringIO()
logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)

if logger.hasHandlers():
    logger.handlers.clear()

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

memory_handler = logging.StreamHandler(log_stream)
memory_handler.setFormatter(formatter)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)

logger.addHandler(memory_handler)
logger.addHandler(console_handler)

# =========================
# /tmp workspace
# =========================
# NEW: define stable temp locations under /tmp; cleaned each run
TMP_DIR = Path("/tmp/odis")
TMP_ZIP_PATH = TMP_DIR / "archive.zip"
TMP_EXTRACT_DIR = TMP_DIR / "extract"

# =========================
# Helpers
# =========================

def s3_file_exists(key: str) -> bool:
    """Check if file already exists in S3."""
    try:
        s3.head_object(Bucket=BUCKET_NAME, Key=key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NotFound", "NoSuchKey"):
            return False
        logger.exception(f"s3.head_object failed for key={key}")
        raise

def get_s3_obj_metadata(key: str):  # NEW: fetch existing user metadata for idempotency checks
    """
    Return metadata dict for an existing object, or None if not found.
    Keys are lowercased by boto3.
    """
    try:
        resp = s3.head_object(Bucket=BUCKET_NAME, Key=key)
        return resp.get("Metadata", {}) or {}
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NotFound", "NoSuchKey"):
            return None
        logger.exception(f"s3.head_object failed for key={key}")
        raise

def upload_to_s3(key: str, data_bytes: bytes):
    """Upload a file to S3."""
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=data_bytes,
        ContentType="application/json"
    )

def upload_file_to_s3(key: str, local_path: Path, *, metadata: dict | None = None):  # CHANGED: support metadata
    extra = {"ContentType": "application/json"}
    if metadata:
        extra["Metadata"] = metadata  # will be stored as x-amz-meta-*
    s3.upload_file(
        Filename=str(local_path),
        Bucket=BUCKET_NAME,
        Key=key,
        ExtraArgs=extra
    )

def write_log_to_s3():
    """Write captured log output to S3 as a .log file."""
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log_key = f"{LOG_PREFIX.rstrip('/')}/ingestion_run_log_{timestamp}.log"
    log_data = log_stream.getvalue()

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=log_key,
        Body=log_data.encode("utf-8"),
        ContentType="text/plain"
    )
    logger.debug(f"Log file uploaded to: {log_key}")

def _retryable_status(code: int) -> bool:
    # Retry on 5xx, 429 Too Many Requests, 408 Request Timeout
    return (500 <= code < 600) or code in (408, 429)

# CHANGED: stream the download directly to a file under /tmp (no in-memory ZIP bytes)
def download_zip_to_file_with_budget(url: str, dst_path: Path, total_budget_seconds: float) -> None:
    """
    Download URL to a file with a hard total time budget, per-op socket timeout,
    and exponential backoff with jitter.
    """
    start = time.monotonic()
    attempt = 0
    last_exc = None

    # Ensure parent dir exists
    dst_path.parent.mkdir(parents=True, exist_ok=True)

    while attempt < MAX_HTTP_ATTEMPTS:
        attempt += 1

        elapsed = time.monotonic() - start
        remaining = total_budget_seconds - elapsed
        if remaining <= 0:
            raise TimeoutError("No remaining time budget")

        per_op_timeout = max(0.1, min(HTTP_TIMEOUT_SECONDS, remaining))

        try:
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=per_op_timeout) as resp, open(dst_path, "wb") as f:
                status = getattr(resp, "status", 200)
                if 200 <= status < 300:
                    # Read in chunks; after each chunk, ensure we haven't blown the total budget
                    while True:
                        if (time.monotonic() - start) > total_budget_seconds:
                            raise TimeoutError("Download exceeded total budget")
                        chunk = resp.read(64 * 1024)
                        if not chunk:
                            break
                        f.write(chunk)
                    return
                if not _retryable_status(status):
                    raise RuntimeError(f"HTTP {status} (not retryable)")
                last_exc = RuntimeError(f"HTTP {status}")

        except (urllib.error.URLError, socket.timeout, TimeoutError) as e:
            last_exc = e

        if attempt < MAX_HTTP_ATTEMPTS:
            delay = min(BACKOFF_CAP_SECONDS, BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)))
            delay += random.uniform(0, BACKOFF_BASE_SECONDS)  # jitter
            remaining = total_budget_seconds - (time.monotonic() - start)
            time.sleep(max(0.0, min(delay, remaining)))

    raise last_exc if last_exc else TimeoutError("Exhausted attempts within time budget")

# NEW: safe extraction that only writes .json files and prevents Zip Slip
def _safe_extract_jsons(zip_path: Path, dest_dir: Path):
    """
    Extract only .json members, guarding against Zip Slip.
    Returns list of extracted file Paths.
    """
    extracted = []
    dest_dir.mkdir(parents=True, exist_ok=True)
    base = dest_dir.resolve()

    with zipfile.ZipFile(zip_path, "r") as zf:
        for info in zf.infolist():
            if not info.filename.endswith(".json"):
                continue

            target = (base / info.filename).resolve()
            if not str(target).startswith(str(base)):
                logger.warning(f"Skipping suspicious path: {info.filename}")
                continue

            target.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(info, "r") as src, open(target, "wb") as out:
                shutil.copyfileobj(src, out, length=64 * 1024)
            extracted.append(target)

    return extracted

# NEW: streaming checksum for files on disk
def compute_sha256_file(path: Path, chunk_size: int = 64 * 1024) -> str:
    """Stream a file and return its hex SHA-256 digest."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()

# =========================
# Lambda handler
# =========================

def lambda_handler(event, context):
    start_time = time.time()
    new_files = 0
    skipped_files = 0
    
    manifest_rows = []   # NEW: accumulate per-file records for the manifest


    logger.info(
        f"... Lambda started | bucket={BUCKET_NAME}, data_prefix={DATA_PREFIX}, log_prefix={LOG_PREFIX}"
    )

    try:
        logger.info(f"Downloading ZIP to /tmp from: {ZIP_URL}")  # CHANGED: message reflects /tmp approach

        # NEW: clean temp workspace (warm containers may reuse /tmp)
        try:
            if TMP_EXTRACT_DIR.exists():
                shutil.rmtree(TMP_EXTRACT_DIR)
            if TMP_ZIP_PATH.exists():
                TMP_ZIP_PATH.unlink()
        except Exception as _:
            logger.warning("Pre-clean of /tmp failed; continuing")

        # ---- CHANGED: time-boxed download streamed to file in /tmp
        download_zip_to_file_with_budget(
            ZIP_URL,
            TMP_ZIP_PATH,
            TOTAL_DOWNLOAD_BUDGET_S
        )
        logger.info(f"ZIP downloaded to {TMP_ZIP_PATH} (size={TMP_ZIP_PATH.stat().st_size} bytes)")

        # Safely extract only .json files into /tmp workspace
        extracted_paths = _safe_extract_jsons(TMP_ZIP_PATH, TMP_EXTRACT_DIR)
        logger.info(f"Extracted {len(extracted_paths)} JSON files")



        # Upload each extracted JSON, preserving subfolders under DATA_PREFIX
        for local_path in extracted_paths:
            rel = local_path.relative_to(TMP_EXTRACT_DIR).as_posix()
            s3_key = f"{DATA_PREFIX.rstrip('/')}/{rel}"

            # Compute checksum for idempotency; skip if unchanged
            sha256 = compute_sha256_file(local_path)
            existing_meta = get_s3_obj_metadata(s3_key)

            ingested_iso = datetime.utcnow().isoformat(timespec="seconds") + "Z"
            upload_needed = True

            if existing_meta is not None:
                if existing_meta.get("sha256") == sha256:
                    skipped_files += 1
                    upload_needed = False
                    logger.info(f"⏩ Skipped (unchanged): {rel}")
                    continue
                else:
                    logger.info(f"🔁 Overwriting changed file: {rel}")

            metadata = {
                "sha256": sha256,
                "source_url": ZIP_URL,
                "ingested_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            }

            upload_file_to_s3(s3_key, local_path, metadata=metadata)
            new_files += 1
            logger.info(f"✅ Uploaded: {rel} -> s3://{BUCKET_NAME}/{s3_key}")

            # NEW: add to manifest rows regardless
            manifest_rows.append({
                "s3_key": s3_key,
                "s3_uri": f"s3://{BUCKET_NAME}/{s3_key}",
                "stg_file_name": Path(rel).name,
                "stg_file_hashkey": sha256,
                "ingested_at": ingested_iso,
                "size_bytes": local_path.stat().st_size,
                "source_url": ZIP_URL,
                "action": ("uploaded" if upload_needed else "skipped")
            })

        # ------ NEW: write the manifest JSONL for this run
        run_dt = datetime.utcnow()
        part_date = run_dt.strftime("%Y-%m-%d")
        ts_stamp = run_dt.strftime("%Y%m%d_%H%M%S")
        manifest_prefix = f"{DATA_PREFIX.rstrip('/')}/_manifest/ingest_date={part_date}/"
        manifest_key = f"{manifest_prefix}manifest_{ts_stamp}.jsonl"

        jsonl_bytes = "\n".join(json.dumps(r, separators=(",", ":")) for r in manifest_rows).encode("utf-8")
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=manifest_key,
            Body=jsonl_bytes,
            ContentType="application/json"
        )
        logger.info(f"🗂️ Manifest written to s3://{BUCKET_NAME}/{manifest_key}")


        elapsed = round(time.time() - start_time, 2)
        logger.info(f"✅ Run completed. {new_files} new files uploaded, {skipped_files} skipped.")
        logger.info(f"🕒 Total time taken: {elapsed} seconds")

        # Push run logs to S3
        write_log_to_s3()

        return {
            "statusCode": 200,
            "body": json.dumps({
                "new_files": new_files,
                "skipped_files": skipped_files,
                "elapsed_seconds": elapsed,
            }),
        }

    except Exception as e:
        logger.exception("❌ ERROR during ingestion")
        try:
            write_log_to_s3()
        except Exception:
            logger.error("Also failed to upload logs")
        return {
            "statusCode": 500,
            "body": f"Failed: {str(e)}",
        }
