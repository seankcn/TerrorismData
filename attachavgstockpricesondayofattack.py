import csv

avg_file = "average_stockdifference_on_attack.csv"
terror_file = "globalterrorism_sorted_us_only.csv"
output_file = "globalterrorism_with_avg_stock.csv"

avg_dict = {}
with open(avg_file, "r", encoding="utf-8", newline="") as fin:
    reader = csv.DictReader(fin)
    for row in reader:
        date = row["Date"].strip()
        avg_diff = row["Avg%Difference"].strip()
        avg_dict[date] = avg_diff

print("Loaded average stock differences for", len(avg_dict), "dates.")

with open(terror_file, "r", encoding="utf-8", newline="") as fin, \
        open(output_file, "w", encoding="utf-8", newline="") as fout:

    reader = csv.DictReader(fin)
    fieldnames = reader.fieldnames + ["Avg%Difference"]
    writer = csv.DictWriter(fout, fieldnames=fieldnames)
    writer.writeheader()

    row_count = 0
    for row in reader:
        row_count += 1
        date = row["date"].strip()
        row["Avg%Difference"] = avg_dict.get(date, "")
        writer.writerow(row)

print(f"Join complete. Processed {row_count} rows from terrorism data.")
print(f"Output written to {output_file}.")
