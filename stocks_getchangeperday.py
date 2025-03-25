import csv
from collections import defaultdict

input_file = "stocksbydate_attackdays_withdifference.csv"
output_file = "average_stockdifference_on_attack.csv"

date_data = defaultdict(lambda: {"sum": 0.0, "count": 0})

with open(input_file, "r", encoding="utf-8", newline="") as fin:
    reader = csv.DictReader(fin)
    for row in reader:
        date = row["Date"].strip()
        try:
            percent_diff = float(row["% Difference"])
        except ValueError as e:
            print(f"Error parsing '% Difference' on date {date}: {e}")
            continue

        date_data[date]["sum"] += percent_diff
        date_data[date]["count"] += 1

averages = []
for date, values in date_data.items():
    if values["count"] > 0:
        avg = values["sum"] / values["count"]
    else:
        avg = 0.0
    averages.append((date, avg))

with open(output_file, "w", encoding="utf-8", newline="") as fout:
    writer = csv.writer(fout)
    writer.writerow(["Date", "Avg%Difference"])
    for date, avg in averages:
        writer.writerow([date, f"{avg:.4f}"])

print(f"Average % difference per date written to {output_file}.")
