
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

reviews_infile = "disney.csv"
terror_infile = "terror.csv"
outfile= "output.csv"

#delete outfile dir if already exists
if os.path.exists(outfile):
    shutil.rmtree(outfile)

#load real and terror files into rdd
reviews_rdd = sc.textFile(reviews_infile)
terror_rdd = sc.textFile(terror_infile)

#(state, price) aggregation for realestate file
def process_reviews(line):
    try:
        row = line.split(',') #split columns
        monthyear = row[2].strip();
        x = monthyear.split("-");
        year = int(x[0])
        if year > 2017 or year < 2010:
            return None
        month = int(x[1])
        review = int(row[1].strip());
        return ((year, month), review) #return tuple
    except Exception:
        return None

reviews_data = (
    reviews_rdd
    .map(process_reviews)  #extract the pairs
    .filter(lambda x: x is not None)  #filter empty val
)

#aggregate prices per month to get average
month_reviews_rdd = (
    reviews_data
    .mapValues(lambda review: (1, review))  #(price, count)
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))  #sum all of it
    .mapValues(lambda x: x[1] / x[0])  #average
)

#state and attack count aggregate, for terrorfile
def process_terrorism(line):
    try:
        row = line.split(',')
        year = int(row[1].strip())
        if year > 2017 or year < 2010:
            return None
        month = int(row[2].strip())
        return ((year, month), 1)
    except Exception:
        return None

terror_data = (
    terror_rdd
    .map(process_terrorism)  #get the pairs
    .filter(lambda x: x is not None)  #filter empty vals
    .reduceByKey(lambda a, b: a + b)  #sum attack count per state
)

#join both rdds
merged_rdd = month_reviews_rdd.leftOuterJoin(terror_data)

#format and save as csv
try:
    (
        merged_rdd
        .map(lambda x: f"{x[0][0]},{x[0][1]},{x[1][0]},{x[1][1] or 0}")  #"state,avg_price,num_attacks"
        .coalesce(1)  #combine into one file
        .saveAsTextFile(outfile)
    )
    print(f"file saved: {outfile}")
except Exception as e:
    print(f"error: {e}")

print(f"Elapsed Time: {time.time() - starttime} seconds")

sc.stop()
