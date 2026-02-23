import os
import sys
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd
from pymongo import MongoClient, ASCENDING, UpdateOne
from pymongo.errors import BulkWriteError


# ---------- Logging ----------
def log(level: str, msg: str) -> None:
    # Format stable, easy to grep
    print(f"[migration:{level}] {msg}", flush=True)


# ---------- Config ----------
@dataclass(frozen=True)
class Config:
    mongo_host: str
    mongo_port: int
    mongo_db: str
    mongo_collection: str
    app_user: str
    app_password: str
    csv_path: str
    dry_run: bool
    batch_size: int


def load_config() -> Config:
    def getenv_bool(name: str, default: str = "0") -> bool:
        return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "y", "on")

    return Config(
        mongo_host=os.getenv("MONGO_HOST", "mongodb"),
        mongo_port=int(os.getenv("MONGO_PORT", "27017")),
        mongo_db=os.getenv("MONGO_DB", "healthcare"),
        mongo_collection=os.getenv("MONGO_COLLECTION", "patients"),
        app_user=os.getenv("APP_USER", "appuser"),
        app_password=os.getenv("APP_PASSWORD", "appsecret"),
        csv_path=os.getenv("CSV_PATH", "/data/healthcare_dataset.csv"),
        dry_run=getenv_bool("DRY_RUN", "0"),
        batch_size=int(os.getenv("BATCH_SIZE", "2000")),
    )


# ---------- Transform ----------
def normalize_column_name(name: str) -> str:
    # More robust and less "template": keep alnum, collapse separators, lowercase
    s = name.strip().lower()
    s = s.replace("'", "")
    out = []
    prev_underscore = False
    for ch in s:
        if ch.isalnum():
            out.append(ch)
            prev_underscore = False
        else:
            if not prev_underscore:
                out.append("_")
                prev_underscore = True
    normalized = "".join(out).strip("_")
    return normalized or "col"


def normalize_columns(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, str]]:
    mapping: Dict[str, str] = {}
    new_cols: List[str] = []
    used = set()

    for c in df.columns:
        base = normalize_column_name(str(c))
        candidate = base
        i = 2
        while candidate in used:
            candidate = f"{base}_{i}"
            i += 1
        used.add(candidate)
        mapping[str(c)] = candidate
        new_cols.append(candidate)

    df2 = df.copy()
    df2.columns = new_cols
    return df2, mapping


def parse_date_like_columns(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    Parse columns that *look like* dates/timestamps.
    Returns df + stats of how many values were converted per column (non-null after parsing).
    """
    stats: Dict[str, int] = {}
    df2 = df.copy()

    date_hints = ("date", "dob", "birth", "timestamp", "created", "updated")
    for col in df2.columns:
        if any(h in col for h in date_hints):
            before_nonnull = df2[col].notna().sum()
            # coerce errors -> NaT
            parsed = pd.to_datetime(df2[col], errors="coerce", utc=True)
            after_nonnull = parsed.notna().sum()
            # Only replace if it actually looks like a date column (some conversions succeeded)
            if after_nonnull > 0 and after_nonnull >= int(0.2 * max(1, before_nonnull)):
                df2[col] = parsed
                stats[col] = after_nonnull
    return df2, stats


def choose_primary_key(df: pd.DataFrame) -> Optional[str]:
    """
    Pick an identifier column if present. This impacts idempotence.
    Priority is explicit patient_id variants first, then generic id.
    """
    candidates_priority = [
        "patient_id",
        "patientid",
        "patient",
        "member_id",
        "memberid",
        "record_id",
        "recordid",
        "id",
        "_id",
    ]
    cols = set(df.columns)
    for c in candidates_priority:
        if c in cols:
            return c
    return None


def drop_duplicate_rows(df: pd.DataFrame, key: Optional[str]) -> Tuple[pd.DataFrame, int]:
    before = len(df)
    if key and key in df.columns:
        df2 = df.drop_duplicates(subset=[key], keep="first")
    else:
        df2 = df.drop_duplicates(keep="first")
    removed = before - len(df2)
    return df2, removed


def convert_nan_to_none(df: pd.DataFrame) -> pd.DataFrame:
    # More explicit: keep None for mongo
    return df.where(pd.notnull(df), None)


# ---------- Mongo IO ----------
def mongo_uri(cfg: Config) -> str:
    return f"mongodb://{cfg.app_user}:{cfg.app_password}@{cfg.mongo_host}:{cfg.mongo_port}/{cfg.mongo_db}"


def connect(cfg: Config) -> Tuple[MongoClient, str]:
    uri = mongo_uri(cfg)
    safe_uri = uri.replace(cfg.app_password, "***")
    log("info", f"mongo_connect uri={safe_uri}")
    client = MongoClient(uri)
    return client, safe_uri


def ensure_index(coll, key: str) -> None:
    # Make it unique to enforce idempotence
    log("info", f"mongo_index ensure unique index on '{key}'")
    coll.create_index([(key, ASCENDING)], name=f"ux_{key}", unique=True)


def bulk_upsert(coll, records: List[Dict], key: str, batch_size: int) -> Tuple[int, int, int]:
    """
    Upsert records by key in batches.
    Returns: (matched, modified, upserted)
    """
    matched = modified = upserted = 0

    def chunks(lst: List[Dict], n: int):
        for i in range(0, len(lst), n):
            yield lst[i : i + n]

    for i, batch in enumerate(chunks(records, batch_size), start=1):
        ops = []
        for doc in batch:
            k = doc.get(key)
            if k is None:
                # If key missing, skip to avoid creating many null keys
                continue
            ops.append(UpdateOne({key: k}, {"$set": doc}, upsert=True))

        if not ops:
            continue

        try:
            res = coll.bulk_write(ops, ordered=False)
            matched += res.matched_count
            modified += res.modified_count
            upserted += len(res.upserted_ids or {})
            log("info", f"mongo_write batch={i} ops={len(ops)} matched={res.matched_count} upserted={len(res.upserted_ids or {})}")
        except BulkWriteError as bwe:
            # Still report details but don’t crash
            log("warn", f"mongo_write bulk_error details_keys={list((bwe.details or {}).keys())}")
    return matched, modified, upserted


def insert_fallback(coll, records: List[Dict]) -> int:
    if not records:
        return 0
    res = coll.insert_many(records, ordered=False)
    return len(res.inserted_ids)


# ---------- Main ----------
def main() -> int:
    t0 = time.time()
    cfg = load_config()

    if not os.path.exists(cfg.csv_path):
        log("error", f"csv_missing path={cfg.csv_path}")
        return 2

    log("info", f"csv_read path={cfg.csv_path}")
    df = pd.read_csv(cfg.csv_path)
    rows_in = len(df)

    df, colmap = normalize_columns(df)
    df, date_stats = parse_date_like_columns(df)
    key = choose_primary_key(df)
    df, removed_dupes = drop_duplicate_rows(df, key)
    df = convert_nan_to_none(df)

    rows_out = len(df)
    records = df.to_dict(orient="records")

    log(
        "info",
        "transform_done "
        f"rows_in={rows_in} rows_out={rows_out} dedup_removed={removed_dupes} "
        f"key={key or 'none'} date_cols={len(date_stats)}",
    )

    if cfg.dry_run:
        log("info", "dry_run enabled: skipping mongo write")
        log("info", f"sample_columns={list(df.columns)[:10]}")
        return 0

    client, _safe_uri = connect(cfg)
    try:
        coll = client[cfg.mongo_db][cfg.mongo_collection]

        if key:
            ensure_index(coll, key)
            matched, modified, upserted = bulk_upsert(coll, records, key=key, batch_size=cfg.batch_size)
            total = coll.count_documents({})
            log("info", f"mongo_summary matched={matched} modified={modified} upserted={upserted} total={total}")
        else:
            inserted = insert_fallback(coll, records)
            total = coll.count_documents({})
            log("info", f"mongo_summary inserted={inserted} total={total}")

    finally:
        client.close()

    dt = time.time() - t0
    log("info", f"done seconds={dt:.2f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())