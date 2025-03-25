import csv
import time
from collections import defaultdict

starttime = time.time()

#files
real_infile = "realtor-data.csv"
terror_infile = "globalterrorism.csv"

#dictionaries for total price per state/count of states, attacks per state
state_price = defaultdict(lambda: {"total_price": 0, "count": 0})
state_attacks = defaultdict(int)

#open files and merge to the output
with open(real_infile) as realfile, open(terror_infile) as terrorfile, open("realestate_processed.csv", 'w', newline='') as outfile:

    realReader = csv.reader(realfile)
    terrorReader = csv.reader(terrorfile)
    writer = csv.writer(outfile)

    #real estate data sort by state and avg price
    realHeader = next(realReader, None)

    for row in realReader:
        try:
            state = row[8].strip()
            price = row[2].strip()

            if not state or not price:
                continue #skip if missing data for price/state

            price = float(price)
            state_price[state]["total_price"] += price
            state_price[state]["count"] += 1
        except ValueError:
            continue

    print(f"real estate processed")

    #global terrorism data process
    terrorHeader = next(terrorReader, None)

    for row in terrorReader:
        if len(row) < 12:
            continue

        try:
            country = row[8].strip()
            if country != "United States":
                continue

            state = row[11].strip()
            if state:
                state_attacks[state] += 1 #count num attacks per state
        except Exception:
            continue

    print(f"terrorism database processed")

    #merge the databases
    writer.writerow(["state", "avg_price", "num_attacks"])

    #merge terrorism and realestate by "state"
    for state, data in state_price.items():
        price = data["total_price"]/data["count"]
        attacks = state_attacks.get(state, 0)  #default 0 if no data for the stae
        writer.writerow([state, round(price, 2), attacks]) #write avg price and attack count

    print(f"databases merged")

print(f"Elapsed Time: {time.time() - starttime}")
