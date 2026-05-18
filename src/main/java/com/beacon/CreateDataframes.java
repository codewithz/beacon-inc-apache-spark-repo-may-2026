package com.beacon;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Scanner;

import static org.apache.spark.sql.functions.*;

public class CreateDataframes {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("DataFrame/Dataset Creation App")
                .master("local[4]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");
        System.out.println("\n>>> Spark UI: http://localhost:4040 <<<\n");

        // ============================================================
        // STEP 1 — Your original code (header=false, inferSchema)
        // ============================================================
        /*
         * Spark auto-generates column names: _c0, _c1, _c2, _c3
         * These names tell us nothing about the data.
         * We need to fix this — two approaches shown below.
         */

        String filePath = "C:\\Datasets\\TaxiZones.csv";

        Dataset<Row> df = spark
                .read()
                .option("header", "false")
                .option("inferSchema", "true")
                .csv(filePath);

        System.out.println("=== STEP 1: Your original read — header=false, inferSchema ===");
        df.printSchema();
        /*
         * root
         *  |-- _c0: integer (nullable = true)   ← meaningless
         *  |-- _c1: string (nullable = true)    ← meaningless
         *  |-- _c2: string (nullable = true)    ← meaningless
         *  |-- _c3: string (nullable = true)    ← meaningless
         */

        df.show(5);
        /*
         * +---+----------+------------------------+-----------+
         * |_c0|       _c1|                     _c2|        _c3|
         * +---+----------+------------------------+-----------+
         * |  1|       EWR|         Newark Airport |        EWR|
         * |  2|    Queens|             Jamaica Bay|  Boro Zone|
         * |  3|     Bronx| Allerton/Pelham Gardens|  Boro Zone|
         * +---+----------+------------------------+-----------+
         *
         * Data is there — but column names are useless.
         */

        // ============================================================
        // STEP 2 — Fix column names using toDF()
        // ============================================================
        /*
         * toDF("col1", "col2", ...) renames all columns in order.
         * Quick fix — no need to redefine the schema.
         * TRANSFORMATION — lazy, no job fires.
         */

        System.out.println("\n=== STEP 2: Rename columns using toDF() ===");

        Dataset<Row> namedDF = df.toDF(
                "LocationID",
                "Borough",
                "Zone",
                "ServiceZone"
        );

        namedDF.printSchema();
        /*
         * root
         *  |-- LocationID: integer (nullable = true)
         *  |-- Borough: string (nullable = true)
         *  |-- Zone: string (nullable = true)
         *  |-- ServiceZone: string (nullable = true)
         *
         * Meaningful names now. Same data, better schema.
         */

        namedDF.show(5, false);

        // ============================================================
        // STEP 3 — The RIGHT way: explicit schema + header=true
        // ============================================================
        /*
         * Production approach:
         *   - header=true  → Spark reads first row as column names
         *   - schema(...)  → We define the types ourselves
         *
         * Why better than inferSchema?
         *   inferSchema reads the file TWICE (once to guess, once to load)
         *   It can guess wrong — "94109" guessed as Integer, not String
         *   Explicit schema = one read, correct types, guaranteed
         */

        System.out.println("\n=== STEP 3: Proper approach — explicit schema + header=true ===");

        StructType taxiZoneSchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("LocationID",   DataTypes.IntegerType, true),
                DataTypes.createStructField("Borough",      DataTypes.StringType,  true),
                DataTypes.createStructField("Zone",         DataTypes.StringType,  true),
                DataTypes.createStructField("ServiceZone",  DataTypes.StringType,  true)
        });

        Dataset<Row> taxiZonesDF = spark
                .read()
                .option("header", "true")
                .schema(taxiZoneSchema)
                .csv(filePath);

        taxiZonesDF.printSchema();
        taxiZonesDF.show(5, false);

        System.out.println("Total zones: " + taxiZonesDF.count());
        // Output: 265

        // ============================================================
        // STEP 4 — select() — pick the columns you need
        // ============================================================
        /*
         * select() picks specific columns.
         * TRANSFORMATION — lazy, no job.
         *
         * col("Borough")          → reference a column — preferred
         * col("Zone").alias("ZN") → reference AND rename inline
         */

        System.out.println("\n=== STEP 4: select() ===");

        // Pick just two columns
        taxiZonesDF
                .select("Borough", "Zone")
                .show(5, false);

        // Pick and rename inline using alias
        System.out.println("--- select with alias ---");
        taxiZonesDF.select(
                col("LocationID").alias("ID"),
                col("Borough"),
                col("Zone").alias("ZoneName"),
                col("ServiceZone").alias("Type")
        ).show(5, false);

        // ============================================================
        // STEP 5 — filter() / where() — keep rows matching a condition
        // ============================================================
        /*
         * filter() and where() are identical — pick whichever reads better.
         * TRANSFORMATION — lazy.
         *
         * equalTo("val")    →  =
         * notEqual("val")   →  !=
         * startsWith("val") →  LIKE 'val%'
         * contains("val")   →  LIKE '%val%'
         * isin("a","b","c") →  IN (a, b, c)
         * isNull()          →  IS NULL
         * isNotNull()       →  IS NOT NULL
         * .and()            →  AND
         * .or()             →  OR
         */

        System.out.println("\n=== STEP 5: filter() and where() ===");

        // Filter 1: Manhattan only
        Dataset<Row> manhattanDF = taxiZonesDF
                .filter(col("Borough").equalTo("Manhattan"));

        System.out.println("--- Manhattan zones ---");
        System.out.println("Count: " + manhattanDF.count());
        manhattanDF.show(5, false);

        // Filter 2: Manhattan AND Yellow Zone — two conditions with .and()
        System.out.println("--- Manhattan Yellow Zones (two conditions) ---");
        taxiZonesDF
                .filter(
                        col("Borough").equalTo("Manhattan")
                                .and(col("ServiceZone").equalTo("Yellow Zone"))
                )
                .show(10, false);

        // Filter 3: Zones whose name starts with "Central"
        System.out.println("--- Zones starting with 'Central' ---");
        taxiZonesDF
                .filter(col("Zone").startsWith("Central"))
                .show(false);

        // Filter 4: Multiple boroughs in one shot using isin()
        System.out.println("--- Queens, Bronx and Brooklyn only ---");
        taxiZonesDF
                .filter(col("Borough").isin("Queens", "Bronx", "Brooklyn"))
                .show(10, false);

        // ============================================================
        // STEP 6 — withColumn() — add a new computed column
        // ============================================================
        /*
         * withColumn("newName", expression) adds a column.
         * when(condition, value).otherwise(value) = CASE WHEN in SQL.
         * TRANSFORMATION — lazy.
         */

        System.out.println("\n=== STEP 6: withColumn() — add computed columns ===");

        Dataset<Row> enrichedDF = taxiZonesDF
                // Flag whether this is an outer borough
                .withColumn("IsOuterBorough",
                        when(col("Borough").equalTo("Manhattan"), "No")
                                .otherwise("Yes"))

                // Short code for service zone type
                .withColumn("ZoneCode",
                        when(col("ServiceZone").equalTo("Yellow Zone"),  "YZ")
                                .when(col("ServiceZone").equalTo("Boro Zone"),   "BZ")
                                .when(col("ServiceZone").equalTo("Airports"),    "AP")
                                .otherwise("OT"));

        enrichedDF.show(10, false);
        /*
         * +----------+---------+-------------------+-----------+--------------+--------+
         * |LocationID|  Borough|               Zone|ServiceZone|IsOuterBorough|ZoneCode|
         * +----------+---------+-------------------+-----------+--------------+--------+
         * |         1|      EWR|    Newark Airport |        EWR|           Yes|      OT|
         * |         2|   Queens|        Jamaica Bay|  Boro Zone|           Yes|      BZ|
         * |         4|Manhattan|      Alphabet City|Yellow Zone|            No|      YZ|
         * +----------+---------+-------------------+-----------+--------------+--------+
         */

        // ============================================================
        // STEP 7 — withColumnRenamed() — rename an existing column
        // ============================================================
        /*
         * withColumnRenamed("oldName", "newName")
         * Renames one column. Does not change any data.
         * TRANSFORMATION — lazy.
         */

        System.out.println("\n=== STEP 7: withColumnRenamed() ===");

        taxiZonesDF
                .withColumnRenamed("LocationID", "ID")
                .withColumnRenamed("ServiceZone", "Type")
                .show(5, false);

        // ============================================================
        // STEP 8 — groupBy() + agg() — count zones per borough
        // ============================================================
        /*
         * groupBy("col") groups rows by that column.
         * agg(...) applies aggregate functions to each group.
         *
         * IMPORTANT: This causes a SHUFFLE.
         * → Spark UI after this: you will see 2 Stages for the first time.
         *   Stage 1: scan + partial group-by + shuffle write
         *   Stage 2: final aggregation after shuffle read
         *
         * Go check the Stages tab and SQL tab after running this.
         */

        System.out.println("\n=== STEP 8: groupBy() + agg() — causes a shuffle! ===");

        taxiZonesDF
                .groupBy("Borough")
                .agg(count("*").alias("TotalZones"))
                .orderBy(col("TotalZones").desc())
                .show(false);
        /*
         * +---------+----------+
         * |  Borough|TotalZones|
         * +---------+----------+
         * |Manhattan|        69|
         * | Brooklyn|        61|
         * |   Queens|        60|
         * |    Bronx|        43|
         * |Staten Island|    19|
         * |      EWR|         1|
         * +---------+----------+
         *
         * → Spark UI: this job shows 2 Stages.
         *   SQL tab: look for Exchange node — that IS the shuffle.
         */

        // ============================================================
        // STEP 9 — distinct() — unique values
        // ============================================================
        /*
         * distinct() removes duplicate rows.
         * select() + distinct() = unique values in a column.
         */

        System.out.println("\n=== STEP 9: distinct() ===");

        System.out.println("Unique Boroughs:");
        taxiZonesDF
                .select("Borough")
                .distinct()
                .orderBy("Borough")
                .show(false);

        System.out.println("Unique ServiceZone types:");
        taxiZonesDF
                .select("ServiceZone")
                .distinct()
                .show(false);

        // ============================================================
        // STEP 10 — Full pipeline — chain everything together
        // ============================================================
        /*
         * In production, you chain all transformations in one expression.
         * One action at the end triggers the entire pipeline.
         *
         * QUESTION: How many Yellow Zones vs Boro Zones are there
         *           in each outer borough?
         */

        System.out.println("\n=== STEP 10: Full pipeline — all chained ===");

        taxiZonesDF
                .filter(col("Borough").notEqual("EWR"))
                .filter(col("ServiceZone").isin("Yellow Zone", "Boro Zone"))
                .groupBy("Borough", "ServiceZone")
                .agg(count("*").alias("ZoneCount"))
                .orderBy(col("Borough").asc(), col("ZoneCount").desc())
                .show(false);

        // ============================================================
        // SUMMARY
        // ============================================================

        System.out.println("\n=== SUMMARY — What we covered ===\n");
        System.out.println(
                "  df.toDF('col1','col2',...)        → rename _c0, _c1 columns\n" +
                        "  Explicit schema + header=true     → production approach\n" +
                        "\n" +
                        "  TRANSFORMATIONS (lazy — no job):\n" +
                        "    select(col('x'), col('y'))       → pick columns\n" +
                        "    col('x').alias('newName')        → rename in select\n" +
                        "    filter(col('x').equalTo('val'))  → keep rows where x = val\n" +
                        "    .and() / .or() / .isin()         → combine conditions\n" +
                        "    .startsWith() / .contains()      → string conditions\n" +
                        "    withColumn('name', expression)   → add computed column\n" +
                        "    when(cond, val).otherwise(val)   → CASE WHEN equivalent\n" +
                        "    withColumnRenamed('old','new')   → rename a column\n" +
                        "    distinct()                       → remove duplicates\n" +
                        "    groupBy().agg(count('*'))         → group and aggregate\n" +
                        "    orderBy(col.desc())              → sort results\n" +
                        "\n" +
                        "  Spark UI → SQL tab after groupBy:\n" +
                        "    Exchange node  = the shuffle happened here\n" +
                        "    Two stages     = Stage 1 shuffles, Stage 2 aggregates"
        );

        System.out.println("\n>>> PAUSED — Check http://localhost:4040 — Press ENTER to exit <<<");
        try (final var scanner = new Scanner(System.in)) {
            scanner.nextLine();
        }
    }
}