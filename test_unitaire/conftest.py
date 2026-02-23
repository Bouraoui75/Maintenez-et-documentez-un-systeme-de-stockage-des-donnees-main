import pytest
import mongomock
from pathlib import Path

@pytest.fixture
def sample_csv(tmp_path: Path):
    p = tmp_path / "sample.csv"
    p.write_text(
        "PatientID,Name,Age,Billing Amount,Is Credit,Visit Date\n"
        "1,Alice,30,120.5,true,2024-03-01\n"
        "1,Alice,30,120.5,true,2024-03-01\n"  # duplicate on purpose
        "2,Bob,40,200,false,2024-04-01\n",
        encoding="utf-8"
    )
    return str(p)

@pytest.fixture
def mongo_client_mock(monkeypatch):
    import loader
    mock_client = mongomock.MongoClient()
    monkeypatch.setattr(loader, "MongoClient", lambda *a, **k: mock_client)
    return mock_client
