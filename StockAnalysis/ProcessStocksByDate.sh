#!/bin/bash

output_file="combined.csv"
temp_file="temp_combined.csv"
input_folder="../StockPrices/1 Day/Stocks"

echo "Date,Stock,Open,Close,Difference" > "$temp_file"

for file in "$input_folder"/*.txt; do
    stock=$(basename "$file" .txt)

    echo "Processing stock: $stock"

    tail -n +2 "$file" | awk -F, -v stock="$stock" '{
        diff = $5 - $2;
        # Date, Stock, Open, Close, and Difference formatted to 4 decimal places
        printf "%s,%s,%.4f,%.4f,%.4f\n", $1, stock, $2, $5, diff
    }' >> "$temp_file"
done

head -n 1 "$temp_file" > "$output_file"
tail -n +2 "$temp_file" | sort -t, -k1,1 >> "$output_file"
rm "$temp_file"

echo "Process completed in $SECONDS seconds."
