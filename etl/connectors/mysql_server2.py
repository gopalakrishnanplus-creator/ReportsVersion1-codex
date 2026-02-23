import csv
from pathlib import Path
from django.conf import settings


def extract_table(table: str):
    path = Path(settings.BASE_DIR) / f"{table}.csv"
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))
