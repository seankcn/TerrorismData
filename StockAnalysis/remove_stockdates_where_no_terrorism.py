import csv
from datetime import datetime

terrorism_file = "globalterrorism_sorted_us_only.csv"
stocks_file = "stocksbydate.csv"
output_file = "stocksbydate_filtered.csv"

# Read terrorism .csv, parse dates (YYYY/MM/DD)
terrorism_dates = set()
with open(terrorism_file, "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        raw_date = row.get("date", "")
        if not raw_date:
            continue
        try:
            dt = datetime.strptime(raw_date, "%Y-%m-%d").date()
            terrorism_dates.add(dt)
        except ValueError as e:
            continue

# Read stocks, parse dates (YYYY-MM-DD), filter only dates that have an attack on that day
with open(stocks_file, "r", encoding="utf-8") as fin, \
        open(output_file, "w", encoding="utf-8", newline="") as fout:

    reader = csv.DictReader(fin)
    fieldnames = reader.fieldnames  # ["Date","Stock","Open","Close","Difference"]
    writer = csv.DictWriter(fout, fieldnames=fieldnames)
    writer.writeheader()

    for row in reader:
        raw_stock_date = row.get("Date", "")
        if not raw_stock_date:
            continue

        try:
            dt = datetime.strptime(raw_stock_date, "%Y-%m-%d").date()
        except ValueError as e:
            continue

        if dt in terrorism_dates:
            print(f"[STOCK DEBUG] MATCH: {dt} is in terrorism dates.")
            writer.writerow(row)
        else:
            print(f"[STOCK DEBUG] NO MATCH: {dt} not found in terrorism dates.")
