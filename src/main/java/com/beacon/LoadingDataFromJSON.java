package com.beacon;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Scanner;

import static org.apache.spark.sql.functions.*;

/**
 * ============================================================
 *  LOADING DATA FROM JSON FILES
 *  Beacon Solutions, Inc.
 * ============================================================
 *
 *  WHY JSON IS DIFFERENT FROM CSV:
 *  --------------------------------
 *  CSV  → flat, every row has the same columns, all strings
 *  JSON → can be nested, a column can contain another object,
 *         a column can contain an array of values
 *
 *  THIS FILE COVERS:
 *  ------------------
 *  Part 1 — Flat JSON (TaxiBases.json)
 *           Simple key-value pairs, some columns nested one level
 *
 *  Part 2 — Nested JSON (employee.json)
 *           Objects inside objects, arrays inside objects
 *           How to define a nested schema
 *           How to access nested columns using dot notation
 *
 *  THE KEY NEW CONCEPT:
 *  ---------------------
 *  When JSON has nested fields, your StructType must mirror
 *  the nesting. You put a StructType INSIDE a StructField.
 *
 *  FILES:
 *    C:\Spark\DataFiles\TaxiBases.json   (flat-ish JSON)
 *    C:\Spark\DataFiles\employee.json    (deeply nested JSON)
 * ============================================================
 */
public class LoadingDataFromJSON {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Loading JSON Files - Beacon")
                .master("local[4]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");
        System.out.println("\n>>> Spark UI: http://localhost:4040 <<<\n");

        // ============================================================
        // PART 1 — Flat JSON: TaxiBases.json
        // ============================================================
        /*
         * TaxiBases.json structure — each record looks like:
         *
         * {
         *   "License Number": "B00001",
         *   "Entity Name": "UBER USA LLC",
         *   "Telephone Number": 3477893801,
         *   "SHL Endorsed": "No",
         *   "Type of Base": "Black Car",
         *   "Address": {
         *     "Building": "636",
         *     "Street": "W 28 ST",
         *     "City": "NEW YORK",
         *     "State": "NY",
         *     "Postcode": "10001"
         *   },
         *   "GeoLocation": {
         *     "Latitude": "40.75007",
         *     "Longitude": "-74.00107",
         *     "Location": "(40.75007, -74.00107)"
         *   }
         * }
         *
         * Notice: Address and GeoLocation are NESTED objects.
         * In the schema, we represent them as StructType inside StructField.
         */

        printSection("PART 1 — Flat JSON with one level of nesting: TaxiBases.json");

        // ── Step 1: Define the schema ─────────────────────────────
        /*
         * For flat fields → DataTypes.createStructField("name", DataTypes.StringType, true)
         *
         * For nested objects → DataTypes.createStructField("Address",
         *                          DataTypes.createStructType(new StructField[]{
         *                              ... inner fields here ...
         *                          }), true)
         *
         * The inner StructType mirrors exactly what's inside the JSON object.
         */

        StructType taxiBasesSchema = DataTypes.createStructType(new StructField[]{

                // Flat fields — simple types
                DataTypes.createStructField("License Number",    DataTypes.StringType,  true),
                DataTypes.createStructField("Entity Name",       DataTypes.StringType,  true),
                DataTypes.createStructField("Telephone Number",  DataTypes.LongType,    true),
                DataTypes.createStructField("SHL Endorsed",      DataTypes.StringType,  true),
                DataTypes.createStructField("Type of Base",      DataTypes.StringType,  true),

                // Nested object: Address → defined as StructType inside StructField
                DataTypes.createStructField("Address",
                        DataTypes.createStructType(new StructField[]{
                                DataTypes.createStructField("Building", DataTypes.StringType, true),
                                DataTypes.createStructField("Street",   DataTypes.StringType, true),
                                DataTypes.createStructField("City",     DataTypes.StringType, true),
                                DataTypes.createStructField("State",    DataTypes.StringType, true),
                                DataTypes.createStructField("Postcode", DataTypes.StringType, true)
                        }), true),

                // Nested object: GeoLocation → another StructType inside StructField
                DataTypes.createStructField("GeoLocation",
                        DataTypes.createStructType(new StructField[]{
                                DataTypes.createStructField("Latitude",  DataTypes.StringType, true),
                                DataTypes.createStructField("Longitude", DataTypes.StringType, true),
                                DataTypes.createStructField("Location",  DataTypes.StringType, true)
                        }), true)
        });

        // ── Step 2: Read the JSON file ────────────────────────────
        /*
         * .json(filePath) → reads JSON files
         *
         * KEY OPTION: "multiline" = "true"
         *   By default Spark expects each line to be one complete JSON record.
         *   If your JSON is formatted across multiple lines (pretty-printed),
         *   you MUST add option("multiline", "true").
         *   Without it — Spark tries to parse each line individually and fails.
         */

        String taxiBasesPath = "C:\\Datasets\\TaxiBases.json";

        Dataset<Row> taxiBasesDF = spark
                .read()
                .option("multiline", "true")   // JSON spans multiple lines
                .schema(taxiBasesSchema)        // explicit schema
                .json(taxiBasesPath);

        // ── Step 3: Inspect ──────────────────────────────────────

        System.out.println("--- printSchema() ---");
        taxiBasesDF.printSchema();
        /*
         * root
         *  |-- License Number: string (nullable = true)
         *  |-- Entity Name: string (nullable = true)
         *  |-- Telephone Number: long (nullable = true)
         *  |-- SHL Endorsed: string (nullable = true)
         *  |-- Type of Base: string (nullable = true)
         *  |-- Address: struct (nullable = true)         ← nested object
         *  |    |-- Building: string (nullable = true)
         *  |    |-- Street: string (nullable = true)
         *  |    |-- City: string (nullable = true)
         *  |    |-- State: string (nullable = true)
         *  |    |-- Postcode: string (nullable = true)
         *  |-- GeoLocation: struct (nullable = true)     ← nested object
         *  |    |-- Latitude: string (nullable = true)
         *  |    |-- Longitude: string (nullable = true)
         *  |    |-- Location: string (nullable = true)
         *
         * The tree structure in printSchema() directly mirrors
         * the nesting in your JSON file. This is how you verify
         * your schema was defined correctly.
         */

        System.out.println("\n--- show() — notice nested columns appear as structs ---");
        taxiBasesDF.show(5, false);
        /*
         * Nested columns show as {value1, value2, ...}
         * Address column: {636, W 28 ST, NEW YORK, NY, 10001}
         */

        // ── Step 4: Access nested fields using dot notation ──────
        /*
         * To select a field INSIDE a nested struct, use dot notation:
         *   col("Address.City")     → the City field inside Address
         *   col("GeoLocation.Latitude") → Latitude inside GeoLocation
         *
         * Or in SQL: Address.City, GeoLocation.Latitude
         */

        System.out.println("\n--- Accessing nested fields with dot notation ---");
        taxiBasesDF.select(
                col("Entity Name"),
                col("Type of Base"),
                col("Address.Street").alias("Street"),
                col("Address.City").alias("City"),
                col("GeoLocation.Latitude").alias("Lat"),
                col("GeoLocation.Longitude").alias("Long")
        ).show(5, false);

        // Same in SQL
        taxiBasesDF.createOrReplaceTempView("TaxiBases");

        System.out.println("\n--- Same thing using SQL dot notation ---");
        spark.sql(
                "SELECT `Entity Name`, `Type of Base`, " +
                        "Address.City AS City, " +
                        "Address.Street AS Street, " +
                        "GeoLocation.Latitude AS Lat " +
                        "FROM TaxiBases " +
                        "LIMIT 5"
        ).show(false);
        /*
         * Note: Column names with spaces (like 'Entity Name') need
         * backticks in SQL: `Entity Name`
         */
        System.out.println("\n>>> PAUSED — Check http://localhost:4040 — Press ENTER to exit <<<");
        try (final var scanner = new Scanner(System.in)) {
            scanner.nextLine();
        }
    }

    private static void printSection(String title) {
        System.out.println("\n");
        System.out.println("=".repeat(65));
        System.out.println("  " + title);
        System.out.println("=".repeat(65));
    }
}