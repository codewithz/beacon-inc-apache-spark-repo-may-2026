package com.beacon;

import org.apache.spark.sql.SparkSession;

public class SparkTest {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("TestApp")
                .master("local[*]")
                .getOrCreate();

        System.out.println("Spark version: " + spark.version());
        spark.stop();
    }
}
