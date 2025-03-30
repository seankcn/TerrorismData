import csv
 import time
 
 starttime = time.time()
 
 disney_infile = 'disney.csv'
 terror_infile = 'terror.csv'
 
 with open(terror_infile) as terrorfile, open(disney_infile) as disneyfile, open("output.csv", 'w') as outfile:
     terrorReader = csv.reader(terrorfile)
     disneyReader = csv.reader(disneyfile)
     writer = csv.writer(outfile)
     
     header1 = next(terrorReader, None)
     header2 = next(disneyReader, None)
     
     monthDict = dict()
     attacks = 0
     reviews = 0
     
     for row in terrorReader:
         if len(row) < 102:
             continue
         try:
             year = int(row[1].strip())
             if year > 2017 or year < 2010:
                 continue
             month = int(row[2].strip())
             if (month, year) in monthDict:
                 monthDict[(month, year)] = [monthDict[(month, year)][0]+1,0,0];
             else:
                 monthDict[(month, year)] = [1,0,0];
             attacks+=1
         except Exception as e:
             print(e)
             continue
     for row in disneyReader:
         if len(row) < 6:
             continue
         try:
             monthyear = row[2].strip();
             x = monthyear.split("-");
             year = int(x[0])
             if year > 2017 or year < 2010:
                 continue
             month = int(x[1])
             review = int(row[1].strip());
             if (month, year) in monthDict:
                 monthDict[(month, year)] = [monthDict[(month, year)][0],monthDict[(month, year)][1]+review,monthDict[(month, year)][2]+1];
             else:
                 monthDict[(month, year)] = [0,review,1];
             reviews+=1
         except Exception as e:
             continue
     writer.writerow(["year", "month", "terror_attacks", "avg_reviews"])
     for key, value in monthDict.items():
         writer.writerow([key[1], key[0], value[0], value[1]/max(value[2],1)])
         print([key[1], key[0], value[0], value[1]/max(value[2],1)])
     print(f"Reviews: {reviews}\nAttacks: {attacks}\nElapsed Time: {time.time()-starttime}")
