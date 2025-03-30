#!/bin/bash

input_file="stocksbydate_attackdays.csv"
output_file="stocksbydate_attackdays_withdifference.csv"

echo "Date,Stock,Open,Close,Difference,% Difference" > "$output_file"

# Calculate % Difference = (Difference/Open)*100, skipping rows where Open is zero.
tail -n +2 "$input_file" | awk -F, '{
    # $1 = Date, $2 = Stock, $3 = Open, $4 = Close, $5 = Difference
    if ($3 == 0) {
        next;  # Skip this record to avoid division by 0
    }
    percdiff = ($5 / $3) * 100;
    printf "%s,%s,%.4f,%.4f,%.4f,%.4f\n", $1, $2, $3, $4, $5, percdiff;
}' >> "$output_file"

echo "Finished. Output written to $output_file."
