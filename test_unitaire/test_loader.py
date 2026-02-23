from loader import loader
import pandas as pd
import mongomock


# ---------- log() ----------
def test_log_format(capsys):
    loader.log("info", "hello-world")
    out = capsys.readouterr().out
    assert "[migration:info] hello-world" in out


# ---------- normalize_column_name() ----------
def test_normalize_column_name_collapses_separators_and_lowercases():
    assert loader.normalize_column_name("  Visit Date  ") == "visit_date"
    assert loader.normalize_column_name("E-mail.Address") == "e_mail_address"
    assert loader.normalize_column_name("A---B...C") == "a_b_c"


# ---------- normalize_columns() ----------
def test_normalize_columns_returns_mapping_and_avoids_collisions():
    df = pd.DataFrame({"A B": [1], "A-B": [2], "A.B": [3]})
    df2, mapping = loader.normalize_columns(df)

    # all normalized names exist & are unique
    assert len(set(df2.columns)) == 3
    assert set(mapping.keys()) == {"A B", "A-B", "A.B"}
    assert all(v.startswith("a_b") for v in mapping.values())


# ---------- parse_date_like_columns() ----------
def test_parse_date_like_columns_only_converts_when_plausible():
    df = pd.DataFrame(
        {
            "visit_date": ["2024-01-01", "not a date", "2024-02-02", None],
            "name": ["a", "b", "c", "d"],
        }
    )
    out, stats = loader.parse_date_like_columns(df)

    # should convert visit_date to datetime (UTC) and keep invalid as NaT
    assert "visit_date" in stats
    assert str(out["visit_date"].iloc[0]).startswith("2024-01-01")
    assert pd.isna(out["visit_date"].iloc[1])


# ---------- choose_primary_key() ----------
def test_choose_primary_key_prefers_patient_id_variants():
    df = pd.DataFrame({"id": [1], "patientid": [10]})
    assert loader.choose_primary_key(df) == "patientid"

    df2 = pd.DataFrame({"record_id": [1]})
    assert loader.choose_primary_key(df2) == "record_id"

    df3 = pd.DataFrame({"name": ["x"]})
    assert loader.choose_primary_key(df3) is None


# ---------- drop_duplicate_rows() ----------
def test_drop_duplicate_rows_with_key():
    df = pd.DataFrame({"patientid": [1, 1, 2], "name": ["a", "a", "b"]})
    out, removed = loader.drop_duplicate_rows(df, key="patientid")
    assert len(out) == 2
    assert removed == 1


# ---------- main() : DRY RUN ----------
def test_main_dry_run(monkeypatch):
    fake_df = pd.DataFrame({"PatientID": [1, 1, 2], "Name": ["A", "A", "B"]})

    monkeypatch.setattr(loader.pd, "read_csv", lambda path: fake_df)
    monkeypatch.setattr(loader.os.path, "exists", lambda p: True)

    monkeypatch.setenv("CSV_PATH", "/fake.csv")
    monkeypatch.setenv("DRY_RUN", "1")  # <- important

    rc = loader.main()
    assert rc == 0


# ---------- main() : upsert idempotent (mongomock) ----------
def test_main_upsert_happy_path(monkeypatch):
    fake_df = pd.DataFrame(
        {
            "PatientID": [1, 1, 2],
            "Name": ["Alice", "Alice", "Bob"],
            "Age": [30, 30, 40],
        }
    )

    # CSV + exists
    monkeypatch.setattr(loader.pd, "read_csv", lambda path: fake_df)
    monkeypatch.setattr(loader.os.path, "exists", lambda p: True)

    # env
    monkeypatch.setenv("CSV_PATH", "/fake.csv")
    monkeypatch.setenv("MONGO_HOST", "mongodb")
    monkeypatch.setenv("MONGO_PORT", "27017")
    monkeypatch.setenv("MONGO_DB", "healthcare")
    monkeypatch.setenv("MONGO_COLLECTION", "patients")
    monkeypatch.setenv("APP_USER", "appuser")
    monkeypatch.setenv("APP_PASSWORD", "appsecret")
    monkeypatch.setenv("DRY_RUN", "0")
    monkeypatch.setenv("BATCH_SIZE", "1000")

    # mongomock client
    mock_client = mongomock.MongoClient()
    monkeypatch.setattr(loader, "MongoClient", lambda *a, **k: mock_client)

    rc = loader.main()
    assert rc == 0

    coll = mock_client["healthcare"]["patients"]

    # After normalization: PatientID -> patientid (primary key)
    # Upsert should store 2 docs (id 1 and 2)
    assert coll.count_documents({}) == 2

    # Check one record exists
    assert coll.find_one({"patientid": 1}) is not None
    assert coll.find_one({"patientid": 2}) is not None


# ---------- main() : CSV missing ----------
def test_main_returns_nonzero_when_csv_missing(monkeypatch):
    monkeypatch.setattr(loader.os.path, "exists", lambda p: False)
    monkeypatch.setenv("CSV_PATH", "/missing.csv")

    rc = loader.main()
    assert rc == 2