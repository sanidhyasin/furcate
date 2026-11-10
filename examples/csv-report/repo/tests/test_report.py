from pathlib import Path

import report

FIXTURE = Path(__file__).parent / "fixtures" / "transactions.csv"


def test_totals_skip_rows_without_a_valid_amount():
    # Two rows have no usable amount (empty and "n/a"); they must be skipped,
    # not crash the report and not count as zero.
    rows = report.load(FIXTURE)
    assert len(rows) == 5
    assert report.totals(rows) == {"groceries": 71.05, "rent": 1150.00, "transport": 24.00}


def test_refunds_are_negative():
    rows = report.load(FIXTURE)
    assert ("2026-10-07", "rent", -50.0) in rows
