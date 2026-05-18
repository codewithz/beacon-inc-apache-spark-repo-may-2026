package com.beacon;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class CreateRDD {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("RDD Creation App")
                .master("local[4]")
                .getOrCreate();


        JavaSparkContext context= JavaSparkContext.fromSparkContext(spark.sparkContext());


        List<Integer> list= Arrays.asList(1,2,3,4,5);

        JavaRDD<Integer> rdd=context.parallelize(list);

        int numberOfPartitions= rdd.getNumPartitions();
        System.out.println("Number of partitions: " + numberOfPartitions);

        List<Integer> output= rdd.collect();
        System.out.println("RDD contents: " + output);

        System.out.println("------------------------------------------------------------------------------");

        String filePath="C:\\Datasets\\TaxiZones.csv";

        JavaRDD<String> taxiZoneRDD=context.textFile(filePath);

        System.out.println("Number of lines in TaxiZone file: " + taxiZoneRDD.count());
        taxiZoneRDD.take(5).forEach(System.out::println);

        taxiZoneRDD.collect();

        // Load RDD data from names.csv  -- Task




        try (final var scanner = new Scanner(System.in)) {
            scanner.nextLine();
        }
    }
}
