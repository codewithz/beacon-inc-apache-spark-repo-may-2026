package com.beacon;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CreateDataframes {
    public static void main(String[] args) {

        SparkSession spark=SparkSession.builder()
                .appName("DataFrame/Dataset Creation App")
                .master("local[4]")
                .getOrCreate();


        String filePath="C:\\Datasets\\TaxiZones.csv";
        Dataset<Row> df=spark
                                            .read()
                                           .option("header","false")
                                             .option("inferSchema", "true")
                                           .csv(filePath);

        df.printSchema();
        df.show();

    }
}
