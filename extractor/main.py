"""
KSA flaring monitor: extractor (Cloud Run Job).

Modes (env MODE or first CLI arg):
  nrt         every 3 hours: last 2 days of NRT detections for Saudi Arabia
  sp          weekly: Standard Processing (final) data for days not loaded yet
  backfill    one-off: SP from BACKFILL_START to the latest SP day, plus all NRT days NASA still serves
  wb_sites    weekly check: World Bank flare-site file, loaded only when its content changes

Every response is stored unchanged in Cloud Storage (raw layer), validated, then
loaded into bronze. ops.ingest_manifest records every file. Re-running is safe:
a file with the same content is never loaded twice (BigQuery job id = content hash).
If anything new was loaded, a Dataform workflow is started and followed to the end.
"""
import csv
import datetime as dt
import hashlib
import io
import json
import logging
import os
import sys
import time
import uuid

import requests
from google.api_core.exceptions import Conflict, NotFound
from google.cloud import bigquery, storage

# ------------------------------------------------------------------ config
PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "ksa-flaring-01")
REGION = os.environ.get("REGION", "us-central1")
BUCKET = os.environ.get("BUCKET", f"{PROJECT}-raw")
FIRMS_KEY = os.environ.get("FIRMS_KEY", "").strip()  # injected from Secret Manager
NRT_SOURCES = os.environ.get("NRT_SOURCES", "VIIRS_NOAA20_NRT,VIIRS_NOAA21_NRT").split(",")
SP_SOURCES = os.environ.get("SP_SOURCES", "VIIRS_NOAA20_SP").split(",")
COUNTRY = "SAU"
BACKFILL_START = os.environ.get("BACKFILL_START", "2024-01-01")
DATAFORM_REPO = os.environ.get("DATAFORM_REPO", "")  # e.g. ksa-flaring-monitor; empty = skip
DATAFORM_BRANCH = os.environ.get("DATAFORM_BRANCH", "main")
WB_SITES_URL = os.environ.get(
    "WB_SITES_URL",
    "https://thedocs.worldbank.org/en/doc/b34e0c054bb3fe3695e70154c28eef3f-0400072026/related/"
    "Flare-Volume-Estimates-by-individual-Flare-Location-2012-2025.xlsx",
)
FIRMS_API = "https://firms.modaps.eosdis.nasa.gov/api"
SP_CHUNK_DAYS = 5  # FIRMS day_range maximum

# Columns we accept from FIRMS. API and archive downloads name the two brightness
# columns differently; both are accepted and unified later in silver.
FIRMS_REQUIRED = {"latitude", "longitude", "acq_date", "acq_time", "satellite",
                  "confidence", "version", "frp", "daynight"}
FIRMS_KNOWN = FIRMS_REQUIRED | {"bright_ti4", "bright_ti5", "brightness", "bright_t31",
                                "scan", "track", "instrument", "type"}

WB_REQUIRED = {"id", "lat", "lon", "Country", "BCM", "Year"}

# ------------------------------------------------------------------ logging
class JsonFormatter(logging.Formatter):
    """One JSON object per line: Cloud Logging turns it into structured fields."""
    def format(self, record):
        payload = {"severity": record.levelname, "message": record.getMessage()}
        payload.update(getattr(record, "fields", {}))
        line = json.dumps(payload, ensure_ascii=False, default=str)
        return line.replace(FIRMS_KEY, "***") if FIRMS_KEY else line  # never log the key


handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(JsonFormatter())
log = logging.getLogger("extractor")
log.addHandler(handler)
log.setLevel(logging.INFO)


def info(msg, **fields):
    log.info(msg, extra={"fields": fields})


def error(msg, **fields):
    log.error(msg, extra={"fields": fields})


# ------------------------------------------------------------------ clients
bq = bigquery.Client(project=PROJECT)
gcs = storage.Client(project=PROJECT).bucket(BUCKET)
http = requests.Session()
http.headers.update({"User-Agent": "ksa-flaring-monitor/1.0"})

RUN_ID = os.environ.get("CLOUD_RUN_EXECUTION") or f"local-{uuid.uuid4().hex[:8]}"
RUN_TS = dt.datetime.now(dt.timezone.utc)

MANIFEST = f"{PROJECT}.ops.ingest_manifest"
BRONZE_FIRMS = f"{PROJECT}.bronze.firms_detections"
BRONZE_WB = f"{PROJECT}.bronze.wb_flare_sites"


def fetch(url, what, timeout=120):
    """GET with retries. The URL may contain the MAP_KEY, so only `what` is logged."""
    for attempt in range(1, 4):
        try:
            r = http.get(url, timeout=timeout)
            r.raise_for_status()
            return r.content
        except Exception as e:  # noqa: BLE001
            msg = str(e).replace(FIRMS_KEY, "***") if FIRMS_KEY else str(e)
            error("fetch failed", what=what, attempt=attempt, error=msg)
            time.sleep(10 * attempt)
    raise RuntimeError(f"fetch failed after 3 attempts: {what}")


# ------------------------------------------------------------------ setup
def ensure_tables():
    """Create ops/bronze tables on first run (all bronze columns are STRING on purpose)."""
    manifest_schema = [
        bigquery.SchemaField("run_id", "STRING"),
        bigquery.SchemaField("mode", "STRING"),
        bigquery.SchemaField("dataset", "STRING"),     # firms | worldbank
        bigquery.SchemaField("source", "STRING"),
        bigquery.SchemaField("product", "STRING"),     # nrt | sp | sites
        bigquery.SchemaField("request", "STRING"),     # request description, never the key
        bigquery.SchemaField("file_uri", "STRING"),
        bigquery.SchemaField("sha256", "STRING"),
        bigquery.SchemaField("bytes", "INT64"),
        bigquery.SchemaField("row_count", "INT64"),
        bigquery.SchemaField("status", "STRING"),      # loaded | duplicate | empty | quarantined | failed
        bigquery.SchemaField("detail", "STRING"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ]
    firms_cols = sorted(FIRMS_KNOWN)
    firms_schema = [bigquery.SchemaField(c, "STRING") for c in firms_cols] + [
        bigquery.SchemaField("_run_id", "STRING"),
        bigquery.SchemaField("_source", "STRING"),
        bigquery.SchemaField("_product", "STRING"),
        bigquery.SchemaField("_file_uri", "STRING"),
        bigquery.SchemaField("_sha256", "STRING"),
        bigquery.SchemaField("_row_num", "INT64"),
        bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
    ]
    wb_schema = [bigquery.SchemaField(c, "STRING") for c in [
        "id", "lat", "lon", "Country", "BCM", "MMSCFD", "Year", "Field_type", "Operator",
        "Field_name", "Location", "Flare_Level"]] + [
        bigquery.SchemaField("_file_uri", "STRING"),
        bigquery.SchemaField("_sha256", "STRING"),
        bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
    ]
    for table_id, schema, part in [(MANIFEST, manifest_schema, "ingested_at"),
                                   (BRONZE_FIRMS, firms_schema, "_ingested_at"),
                                   (BRONZE_WB, wb_schema, "_ingested_at")]:
        t = bigquery.Table(table_id, schema=schema)
        t.time_partitioning = bigquery.TimePartitioning(field=part)
        bq.create_table(t, exists_ok=True)


def already_loaded(sha):
    q = f"SELECT COUNT(*) n FROM `{MANIFEST}` WHERE sha256 = @sha AND status = 'loaded'"
    job = bq.query(q, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("sha", "STRING", sha)]))
    return next(iter(job.result())).n > 0


def record(**row):
    row.setdefault("run_id", RUN_ID)
    row.setdefault("mode", MODE)
    row["ingested_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    errors = bq.insert_rows_json(MANIFEST, [row])
    if errors:
        error("manifest insert failed", errors=errors)
    info("manifest", **{k: v for k, v in row.items() if k != "ingested_at"})


# ------------------------------------------------------------------ landing
def land(dataset, product, source, body, ext, request_desc):
    """Store the raw response unchanged. Path is deterministic per content."""
    sha = hashlib.sha256(body).hexdigest()
    day = RUN_TS.strftime("%Y-%m-%d")
    name = f"raw/{dataset}/product={product}/source={source}/ingest_date={day}/{sha[:16]}.{ext}"
    blob = gcs.blob(name)
    if not blob.exists():
        blob.metadata = {"run_id": RUN_ID, "request": request_desc, "sha256": sha}
        blob.upload_from_string(body, content_type="text/csv" if ext == "csv" else
                                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    return f"gs://{BUCKET}/{name}", sha


def quarantine(file_uri, reason):
    src = gcs.blob(file_uri.replace(f"gs://{BUCKET}/", ""))
    gcs.copy_blob(src, gcs, src.name.replace("raw/", "quarantine/", 1))
    error("quarantined", file_uri=file_uri, reason=reason)


def load_rows(table_id, rows, sha, tag):
    """Load NDJSON rows. Job id is derived from the content hash, so a retry of the
    same content is rejected by BigQuery instead of loading duplicates."""
    if not rows:
        return 0
    data = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows).encode("utf-8")
    job_id = f"load_{tag}_{sha[:32]}"
    cfg = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                                 write_disposition="WRITE_APPEND", ignore_unknown_values=False)
    try:
        job = bq.load_table_from_file(io.BytesIO(data), table_id, job_id=job_id, job_config=cfg)
        job.result()
    except Conflict:
        old = bq.get_job(job_id)
        if old.error_result:  # an earlier attempt with this content failed: surface it
            raise RuntimeError(f"earlier load {job_id} failed: {old.error_result}")
        info("load skipped: same content already loaded", job_id=job_id)
        return -1
    return len(rows)


# ------------------------------------------------------------------ FIRMS
def ingest_firms_csv(product, source, body, request_desc):
    file_uri, sha = land("firms", product, source, body, "csv", request_desc)
    base = dict(dataset="firms", source=source, product=product, request=request_desc,
                file_uri=file_uri, sha256=sha, bytes=len(body))
    text = body.decode("utf-8", errors="replace")
    if text.lstrip().lower().startswith("<"):
        quarantine(file_uri, "HTML instead of CSV")
        record(**base, row_count=0, status="quarantined", detail="HTML instead of CSV")
        return 0
    reader = csv.reader(io.StringIO(text))
    header = [h.strip() for h in next(reader, [])]
    missing, unknown = FIRMS_REQUIRED - set(header), set(header) - FIRMS_KNOWN
    if missing or unknown:
        detail = f"schema change: missing={sorted(missing)} unknown={sorted(unknown)}"
        quarantine(file_uri, detail)
        record(**base, row_count=0, status="quarantined", detail=detail)
        return 0
    if already_loaded(sha):
        record(**base, row_count=0, status="duplicate", detail="same content loaded before")
        return 0
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    rows = []
    for i, vals in enumerate(reader, start=1):
        if len(vals) != len(header):
            continue  # counted below as dropped
        r = dict(zip(header, (v.strip() for v in vals)))
        r.update(_run_id=RUN_ID, _source=source, _product=product, _file_uri=file_uri,
                 _sha256=sha, _row_num=i, _ingested_at=now)
        rows.append(r)
    if not rows:
        record(**base, row_count=0, status="empty", detail="valid response with no detections")
        return 0
    n = load_rows(BRONZE_FIRMS, rows, sha, "firms")
    record(**base, row_count=len(rows), status="loaded" if n >= 0 else "duplicate",
           detail="" if n >= 0 else "BigQuery job already existed")
    return max(n, 0)


def firms_area(source, day_range, date=None):
    path = f"country/csv/{FIRMS_KEY}/{source}/{COUNTRY}/{day_range}" + (f"/{date}" if date else "")
    desc = f"country/csv/{source}/{COUNTRY}/{day_range}" + (f"/{date}" if date else "")
    return fetch(f"{FIRMS_API}/{path}", desc), desc


def availability():
    body = fetch(f"{FIRMS_API}/data_availability/csv/{FIRMS_KEY}/all", "data_availability")
    out = {}
    for r in csv.DictReader(io.StringIO(body.decode())):
        out[r["data_id"]] = (r["min_date"], r["max_date"])
    return out


def loaded_sp_dates(source):
    q = (f"SELECT DISTINCT acq_date FROM `{BRONZE_FIRMS}` "
         f"WHERE _product = 'sp' AND _source = @s")
    job = bq.query(q, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("s", "STRING", source)]))
    return {r.acq_date for r in job.result()}


def run_nrt():
    total = 0
    for src in NRT_SOURCES:
        body, desc = firms_area(src, 2)
        total += ingest_firms_csv("nrt", src, body, desc)
        time.sleep(2)
    return total


def run_sp(start=None):
    """Load SP days not loaded yet, in 5-day chunks, up to NASA's latest SP day."""
    avail, total = availability(), 0
    for src in SP_SOURCES:
        if src not in avail:
            error("source not offered by FIRMS", source=src)
            continue
        lo, hi = avail[src]
        lo = max(lo, start or BACKFILL_START)
        have = loaded_sp_dates(src)
        day, end = dt.date.fromisoformat(lo), dt.date.fromisoformat(hi)
        while day <= end:
            chunk = [day + dt.timedelta(d) for d in range(SP_CHUNK_DAYS) if day + dt.timedelta(d) <= end]
            if any(d.isoformat() not in have for d in chunk):
                body, desc = firms_area(src, len(chunk), day.isoformat())
                total += ingest_firms_csv("sp", src, body, desc)
                time.sleep(1)
            day += dt.timedelta(SP_CHUNK_DAYS)
    return total


def run_backfill():
    total = run_sp(BACKFILL_START)
    avail = availability()
    for src in NRT_SOURCES:  # NRT that NASA still serves, oldest first
        if src not in avail:
            continue
        day, end = dt.date.fromisoformat(avail[src][0]), dt.date.fromisoformat(avail[src][1])
        while day <= end:
            n = min(SP_CHUNK_DAYS, (end - day).days + 1)
            body, desc = firms_area(src, n, day.isoformat())
            total += ingest_firms_csv("nrt", src, body, desc)
            day += dt.timedelta(n)
            time.sleep(1)
    return total


# ------------------------------------------------------------------ World Bank
def run_wb_sites():
    import openpyxl  # imported here: only this mode needs it

    body = fetch(WB_SITES_URL, "worldbank flare sites", timeout=300)
    file_uri, sha = land("worldbank", "sites", "ggfr", body, "xlsx", "flare-sites xlsx")
    base = dict(dataset="worldbank", source="ggfr", product="sites", request="flare-sites xlsx",
                file_uri=file_uri, sha256=sha, bytes=len(body))
    if already_loaded(sha):
        record(**base, row_count=0, status="duplicate", detail="file unchanged")
        return 0
    wb = openpyxl.load_workbook(io.BytesIO(body), read_only=True, data_only=True)
    if "Dashboard_file" not in wb.sheetnames:
        quarantine(file_uri, f"sheet missing; sheets={wb.sheetnames}")
        record(**base, row_count=0, status="quarantined", detail="Dashboard_file sheet missing")
        return 0
    it = wb["Dashboard_file"].iter_rows(values_only=True)
    header = [str(h).strip() if h is not None else "" for h in next(it)]
    if WB_REQUIRED - set(header):
        detail = f"schema change: missing={sorted(WB_REQUIRED - set(header))}"
        quarantine(file_uri, detail)
        record(**base, row_count=0, status="quarantined", detail=detail)
        return 0
    rename = {"Field type": "Field_type", "Field name": "Field_name", "Flare Level": "Flare_Level"}
    keep = ["id", "lat", "lon", "Country", "BCM", "MMSCFD", "Year", "Field type", "Operator",
            "Field name", "Location", "Flare Level"]
    idx = {c: header.index(c) for c in keep if c in header}
    now, rows = dt.datetime.now(dt.timezone.utc).isoformat(), []
    for vals in it:
        if vals[idx["Country"]] != "Saudi Arabia":
            continue
        r = {rename.get(c, c): (None if vals[i] is None else str(vals[i])) for c, i in idx.items()}
        r.update(_file_uri=file_uri, _sha256=sha, _ingested_at=now)
        rows.append(r)
    n = load_rows(BRONZE_WB, rows, sha, "wb")
    record(**base, row_count=len(rows), status="loaded" if n >= 0 else "duplicate", detail="Saudi rows only")
    return max(n, 0)


# ------------------------------------------------------------------ Dataform
def run_dataform():
    """Compile the repo and run all actions; wait for the final state."""
    import google.auth
    from google.auth.transport.requests import AuthorizedSession

    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    s = AuthorizedSession(creds)
    base = f"https://dataform.googleapis.com/v1beta1/projects/{PROJECT}/locations/{REGION}/repositories/{DATAFORM_REPO}"
    comp = s.post(f"{base}/compilationResults", json={"gitCommitish": DATAFORM_BRANCH}, timeout=120)
    comp.raise_for_status()
    comp = comp.json()
    if comp.get("compilationErrors"):
        raise RuntimeError(f"Dataform compilation errors: {comp['compilationErrors'][:3]}")
    inv = s.post(f"{base}/workflowInvocations", json={"compilationResult": comp["name"]}, timeout=120)
    inv.raise_for_status()
    name = inv.json()["name"]
    info("dataform started", invocation=name)
    for _ in range(120):  # up to 20 minutes
        time.sleep(10)
        state = s.get(f"https://dataform.googleapis.com/v1beta1/{name}", timeout=60).json().get("state")
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            info("dataform finished", invocation=name, state=state)
            if state != "SUCCEEDED":
                raise RuntimeError(f"Dataform workflow {state}: {name}")
            return
    raise RuntimeError(f"Dataform workflow timed out: {name}")


# ------------------------------------------------------------------ main
MODE = (sys.argv[1] if len(sys.argv) > 1 else os.environ.get("MODE", "nrt")).lower()


def main():
    if MODE != "wb_sites" and not FIRMS_KEY:
        error("FIRMS_KEY is not set")
        return 2
    ensure_tables()
    runners = {"nrt": run_nrt, "sp": run_sp, "backfill": run_backfill, "wb_sites": run_wb_sites}
    if MODE not in runners:
        error("unknown mode", mode=MODE)
        return 2
    try:
        loaded = runners[MODE]()
        info("extract finished", mode=MODE, rows_loaded=loaded)
        if loaded and DATAFORM_REPO:
            run_dataform()
        elif loaded:
            info("DATAFORM_REPO not set: transformations not started")
        return 0
    except Exception as e:  # noqa: BLE001
        error("run failed", mode=MODE, error=str(e))
        return 1


if __name__ == "__main__":
    sys.exit(main())
