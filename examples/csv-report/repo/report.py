"""Summarise a transactions CSV: total amount per category."""

import csv
import sys
from collections import defaultdict


def load(path):
    """Read rows of (date, category, amount) from a CSV file with a header."""
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        return [(row["date"], row["category"], float(row["amount"])) for row in reader]


def totals(rows):
    """Sum amounts per category, rounded to cents."""
    out = defaultdict(float)
    for _, category, amount in rows:
        out[category] += amount
    return {k: round(v, 2) for k, v in out.items()}


def main(path):
    for category, total in sorted(totals(load(path)).items()):
        print(f"{category}\t{total:.2f}")


if __name__ == "__main__":
    main(sys.argv[1])
