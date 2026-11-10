from pathlib import Path

import report

FIXTURE = Path(__file__).parent / "fixtures" / "transactions.csv"


def test_totals():
    rows = report.load(FIXTURE)
    assert len(rows) == 5
    assert report.totals(rows) == {"groceries": 71.05, "rent": 1150.00, "transport": 24.00}


def test_load():
    rows = report.load(FIXTURE)
    assert ("2026-10-07", "rent", -50.0) in rows
