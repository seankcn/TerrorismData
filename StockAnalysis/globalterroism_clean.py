import csv
import subprocess
import platform
import os

input_file = '../globalterrorism.csv'
temp_output_file = '../globalterrorism_prepared.csv'
sorted_output_file = '../globalterrorism_sorted.csv'

with open(input_file, newline='', encoding='latin1') as infile, \
        open(temp_output_file, 'w', newline='', encoding='latin1') as outfile:
    reader = csv.reader(infile)
    writer = csv.writer(outfile)

    writer.writerow([
        "date", "country", "city", "latitude", "longitude",
        "success", "suicide", "attacktype1_txt", "nkill", "nwound", "severity"
    ])

    header = next(reader, None)  # Skip input header

    for row in reader:
        # Skip rows that do not have enough columns
        if len(row) < 102:
            continue

        try:
            year = row[1].strip()
            month = row[2].strip()
            day = row[3].strip()
            date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"

            country = row[8].strip()  # column 9

            if country != "United States":
                continue

            city = row[12].strip()  # column 13
            latitude = row[13].strip()  # column 14
            longitude = row[14].strip()  # column 15
            success = row[26].strip()  # column 27
            suicide = row[27].strip()  # column 28
            attacktype1_txt = row[29].strip()  # column 30

            nkill_str = row[98].strip()
            nwound_str = row[101].strip()
            nkill = int(nkill_str) if nkill_str else 0
            nwound = int(nwound_str) if nwound_str else 0

            # Calculate severity giving a weight
            severity = nkill + nwound * 0.5

            writer.writerow([
                date, country, city, latitude, longitude,
                success, suicide, attacktype1_txt, nkill, nwound, severity
            ])
        except Exception as e:
            continue


# Assuming windows system, have to run through WSL to use tail command
def convert_to_wsl_path(win_path):
    result = subprocess.run(["wsl", "wslpath", "-a", win_path],
                            stdout=subprocess.PIPE,
                            encoding='utf-8',
                            shell=False)
    return result.stdout.strip()


temp_output_file_abs = os.path.abspath(temp_output_file)
sorted_output_file_abs = os.path.abspath(sorted_output_file)
wsl_temp_output_file = convert_to_wsl_path(temp_output_file_abs)
wsl_sorted_output_file = convert_to_wsl_path(sorted_output_file_abs)

wsl_command = (
    f"wsl bash -c \"(head -n 1 '{wsl_temp_output_file}' && "
    f"tail -n +2 '{wsl_temp_output_file}' | sort -t',' -k11,11nr) > '{wsl_sorted_output_file}'\""
)
subprocess.run(wsl_command, shell=True, check=True)

print(f"Sorted output written to {sorted_output_file}")
