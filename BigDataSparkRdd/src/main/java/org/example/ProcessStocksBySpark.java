package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ProcessStocksBySpark {
    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        // Spark config and context
        SparkConf conf = new SparkConf()
                .setAppName("ProcessStocksBySpark")
                // For local testing, use local[*]
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String inputFolder = "C:\\Users\\cians\\Desktop\\BigData DataSets\\StockPrices\\1 Day\\Stocks";
        String outputDir = "combined_output";

        JavaPairRDD<String, String> filesRDD = sc.wholeTextFiles(inputFolder + "/*.txt");

        // Skip header, calculate difference, format CSV line.
        JavaRDD<String> processedRecords = filesRDD.flatMap(tuple -> {
            String filePath = tuple._1;
            String fileContent = tuple._2;

            // Get Stock name
            String fileName = new File(filePath).getName();
            String stockSymbol = fileName.replaceAll("\\.txt$", "");

            // Split content into lines
            String[] lines = fileContent.split("\\r?\\n");
            List<String> outputLines = new ArrayList<>();


            for (int i = 1; i < lines.length; i++) {
                String line = lines[i].trim();
                if (line.isEmpty()) {
                    continue;
                }
                // columns: 0=Date, 1=Open, 2=High, 3=Low, 4=Close, 5=Difference
                String[] tokens = line.split(",");
                if (tokens.length < 5) {
                    continue;
                }
                double open = Double.parseDouble(tokens[1]);
                double close = Double.parseDouble(tokens[4]);
                double diff = close - open;

                String formatted = String.format("%s,%s,%.4f,%.4f,%.4f",
                        tokens[0], stockSymbol, open, close, diff);
                outputLines.add(formatted);
            }
            return outputLines.iterator();
        });

        // Repartition and sort the records by the Date
        JavaRDD<String> repartitionedRecords = processedRecords.repartition(100);
        JavaRDD<String> sortedRecords = repartitionedRecords.sortBy(
                line -> line.split(",")[0],
                true,   // ascending order
                1       // number of partitions for the sort
        );

        List<String> header = Arrays.asList("Date,Stock,Open,Close,Difference");
        JavaRDD<String> headerRDD = sc.parallelize(header);
        JavaRDD<String> finalRDD = headerRDD.union(sortedRecords);

        finalRDD.coalesce(1).saveAsTextFile(outputDir);

        sc.stop();

        long endTime = System.currentTimeMillis();
        double elapsedSeconds = (endTime - startTime) / 1000.0;
        System.out.println("Process completed in " + elapsedSeconds + " seconds. Output saved to directory: " + outputDir);
    }
}
