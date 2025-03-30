
#I used the pyspark lab to run this
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.3.1-bin-hadoop3"

from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True) #  This will format our output tables a bit nicer when not using the show() method
spark

#end of pyspark initialisation from the lab

import shutil
import time
starttime = time.time()

sc = spark.sparkContext

real_infile = "realtor-data.csv"
terror_infile = "globalterrorism.csv"
outfile= "merged_data"

#delete outfile dir if already exists
if os.path.exists(outfile):
    shutil.rmtree(outfile)

#load real and terror files into rdd
real_rdd = sc.textFile(real_infile)
terror_rdd = sc.textFile(terror_infile)

#(state, price) aggregation for realestate file
def process_real_estate(line):
    try
        row = line.split(',') #split columns
        state = row[8].strip() #get price and state
        price_str = row[2].strip()
        if state and price_str:
            price = float(price_str)
            return (state, price) #return tuple
        return None
    except Exception:
        return None

real_data = (
    real_rdd
    .map(process_real_estate)  #extract the pairs
    .filter(lambda x: x is not None)  #filter empty val
)

#aggregate prices per state to get average
state_price_rdd = (
    real_data
    .mapValues(lambda price: (price, 1))  #(price, count)
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))  #sum all of it
    .mapValues(lambda x: x[0] / x[1])  #average
)

#state and attack count aggregate, for terrorfile
def process_terrorism(line):
    try:
        row = line.split(',')
        country = row[8].strip()
        if country != "United States":
            return None
        state = row[11].strip() #get state
        return (state, 1)
    except Exception:
        return None

terror_data = (
    terror_rdd
    .map(process_terrorism)  #get the pairs
    .filter(lambda x: x is not None)  #filter empty vals
    .reduceByKey(lambda a, b: a + b)  #sum attack count per state
)

#join both rdds
merged_rdd = state_price_rdd.leftOuterJoin(terror_data)

#format and save as csv
try:
    (
        merged_rdd
        .map(lambda x: f"{x[0]},{x[1][0]},{x[1][1] or 0}")  #"state,avg_price,num_attacks"
        .coalesce(1)  #combine into one file
        .saveAsTextFile(outfile)
    )
    print(f"file saved: {outfile}")
except Exception as e:
    print(f"error: {e}")

print(f"Elapsed Time: {time.time() - starttime} seconds")

sc.stop()
