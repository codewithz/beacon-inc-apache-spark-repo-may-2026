package com.beacon.performance;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import java.util.Scanner;

import static org.apache.spark.sql.functions.*;

/**
 * ============================================================
 *  SPARK PERFORMANCE — PERSISTENCE AND CACHING
 *  Beacon Solutions, Inc.
 * ============================================================
 *
 *  THE PROBLEM:
 *  ------------
 *  Spark is lazy. Every time you call an action on a DataFrame,
 *  Spark recomputes the ENTIRE pipeline from scratch — reads the
 *  file again, applies every transformation again.
 *
 *  If you write the same DataFrame to two different locations,
 *  Spark reads and processes the source file TWICE.
 *  For a 3.6 million row CSV with dropDuplicates and groupBy,
 *  that double computation is expensive.
 *
 *  THE SOLUTION: persist() / cache()
 *  ----------------------------------
 *  Mark the DataFrame for persistence BEFORE the first action.
 *  On the first action, Spark computes the result AND stores it.
 *  On every subsequent action, Spark reads from the stored copy.
 *  The file is only read once. The transformations run only once.
 *
 *  STORAGE LEVELS:
 *  ---------------
 *  MEMORY_ONLY        → store in RAM only, fast, fails if not enough memory
 *  MEMORY_AND_DISK    → RAM first, spills to disk if RAM is full (recommended)
 *  DISK_ONLY          → disk only, slow but always works
 *  MEMORY_ONLY_SER    → serialized in RAM, less memory but slower to read
 *
 *  cache() is shorthand for MEMORY_AND_DISK in Spark 3.x.
 *
 *  WHAT WE DO IN THIS FILE:
 *  -------------------------
 *  1. Without cache: write the same grouped DataFrame twice
 *     → file read twice, transformations run twice
 *  2. With cache: persist before first write, write twice
 *     → file read once, transformations run once
 *  3. Compare job count and duration in Spark UI
 * ============================================================
 */
public class SparkPerformancePersistence {

    private static final Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Spark Performance - Persistence")
                .master("local[4]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");
        spark.conf().set("spark.sql.shuffle.partitions", 3);

        System.out.println("\n>>> Spark UI: http://localhost:4040 <<<\n");

        // ============================================================
        // SETUP — Load Yellow Taxi data
        // ============================================================

        String filePath = "C:\\Datasets\\YellowTaxis_202210.csv";

        StructType yellowTaxiSchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("VendorId",              DataTypes.IntegerType,   true),
                DataTypes.createStructField("lpep_pickup_datetime",  DataTypes.TimestampType, true),
                DataTypes.createStructField("lpep_dropoff_datetime", DataTypes.TimestampType, true),
                DataTypes.createStructField("passenger_count",       DataTypes.DoubleType,    true),
                DataTypes.createStructField("trip_distance",         DataTypes.DoubleType,    true),
                DataTypes.createStructField("RatecodeID",            DataTypes.DoubleType,    true),
                DataTypes.createStructField("store_and_fwd_flag",    DataTypes.StringType,    true),
                DataTypes.createStructField("PULocationID",          DataTypes.IntegerType,   true),
                DataTypes.createStructField("DOLocationID",          DataTypes.IntegerType,   true),
                DataTypes.createStructField("payment_type",          DataTypes.IntegerType,   true),
                DataTypes.createStructField("fare_amount",           DataTypes.DoubleType,    true),
                DataTypes.createStructField("extra",                 DataTypes.DoubleType,    true),
                DataTypes.createStructField("mta_tax",               DataTypes.DoubleType,    true),
                DataTypes.createStructField("tip_amount",            DataTypes.DoubleType,    true),
                DataTypes.createStructField("tolls_amount",          DataTypes.DoubleType,    true),
                DataTypes.createStructField("improvement_surcharge", DataTypes.DoubleType,    true),
                DataTypes.createStructField("total_amount",          DataTypes.DoubleType,    true),
                DataTypes.createStructField("congestion_surcharge",  DataTypes.DoubleType,    true),
                DataTypes.createStructField("airport_fee",           DataTypes.DoubleType,    true)
        });

        Dataset<Row> yellowTaxiDF = spark
                .read()
                .option("header", "true")
                .schema(yellowTaxiSchema)
                .csv(filePath);

        // ============================================================
        // PART 1 — WITHOUT CACHE (the problem)
        // ============================================================
        /*
         * We build a pipeline with two expensive operations:
         *   dropDuplicates() → wide transformation, causes a shuffle
         *   groupBy()        → wide transformation, causes another shuffle
         *
         * Then we write the result to TWO different CSV files.
         *
         * Without cache, each write() is an ACTION that triggers
         * the ENTIRE pipeline independently:
         *   Write 1: read CSV → dropDuplicates → groupBy → write
         *   Write 2: read CSV → dropDuplicates → groupBy → write
         *
         * The 3.6M row file is read TWICE.
         * dropDuplicates shuffles TWICE.
         * groupBy shuffles TWICE.
         * Total cost = 2x everything.
         */

        printSection("PART 1 — WITHOUT CACHE (double computation)");
        System.out.println("Building the pipeline: dropDuplicates + groupBy");
        System.out.println("Writing to TWO outputs WITHOUT caching.");
        System.out.println("Prediction: file read twice, transformations run twice.");
        System.out.println("Running...\n");

        Dataset<Row> yellowTaxiGroupedDF = yellowTaxiDF
                .dropDuplicates()
                .groupBy("PULocationID")
                .agg(sum("total_amount").alias("TotalAmount"));

        // Action 1 — triggers full pipeline: read → dedup → group → write
        long startTime1 = System.currentTimeMillis();

        yellowTaxiGroupedDF.write()
                .option("header", "true")
                .mode(SaveMode.Overwrite)
                .csv("C:\\Spark\\outputs\\NoCaching_FirstWrite.csv");

        System.out.println("First write complete.");

        // Action 2 — triggers full pipeline AGAIN: read → dedup → group → write
        yellowTaxiGroupedDF.write()
                .option("header", "true")
                .mode(SaveMode.Overwrite)
                .csv("C:\\Spark\\outputs\\NoCaching_SecondWrite.csv");

        long endTime1 = System.currentTimeMillis();
        System.out.println("Second write complete.");
        System.out.println("Time WITHOUT cache: " + (endTime1 - startTime1) + " ms");

        pause("PART 1 complete. Go to Spark UI → Jobs tab.\n" +
                "\n" +
                "  You will see multiple jobs for the two write() actions.\n" +
                "  Each write triggered the FULL pipeline independently.\n" +
                "\n" +
                "  Stages tab:\n" +
                "    Look for repeated stage patterns — same stages ran twice.\n" +
                "    Two separate sets of shuffle stages for dropDuplicates\n" +
                "    and groupBy — once per write.\n" +
                "\n" +
                "  Storage tab:\n" +
                "    Should be EMPTY — nothing was cached.\n" +
                "\n" +
                "  This is the problem: same expensive computation ran twice.");

        // ============================================================
        // PART 2 — WITH CACHE (the solution)
        // ============================================================
        /*
         * Now we do the same thing but call persist() BEFORE the first write.
         *
         * IMPORTANT: persist() is LAZY — it does not compute anything yet.
         * It just marks the DataFrame: "when you first compute this,
         * store the result so you don't have to compute it again."
         *
         * First write() → computes the pipeline AND stores result in memory/disk
         * Second write() → reads from the stored result directly
         *
         * The 3.6M row file is read ONCE.
         * dropDuplicates runs ONCE.
         * groupBy runs ONCE.
         * Total cost = 1x everything.
         *
         * STORAGE LEVELS:
         *   MEMORY_AND_DISK() → store in RAM, spill to disk if RAM is full
         *   This is the safest option — never fails due to memory pressure.
         *
         * cache() is shorthand for MEMORY_AND_DISK in Spark 3.x.
         * Both are equivalent:
         *   yellowTaxiGroupedDF.cache()
         *   yellowTaxiGroupedDF.persist(StorageLevel.MEMORY_AND_DISK())
         */

        printSection("PART 2 — WITH CACHE (single computation, reused result)");
        System.out.println("Same pipeline: dropDuplicates + groupBy");
        System.out.println("This time we persist() BEFORE the first write.");
        System.out.println("Prediction: file read once, transformations run once.");
        System.out.println("Running...\n");

        // Rebuild the same pipeline
        Dataset<Row> yellowTaxiGroupedCachedDF = yellowTaxiDF
                .dropDuplicates()
                .groupBy("PULocationID")
                .agg(sum("total_amount").alias("TotalAmount"));

        // Mark for persistence — LAZY, nothing runs yet
        yellowTaxiGroupedCachedDF.persist(StorageLevel.MEMORY_AND_DISK());
        System.out.println("persist() called — DataFrame marked for caching.");
        System.out.println("Nothing has run yet. Storage tab still empty.");

        long startTime2 = System.currentTimeMillis();

        // First write — computes the pipeline AND stores result in memory/disk
        yellowTaxiGroupedCachedDF.write()
                .option("header", "true")
                .mode(SaveMode.Overwrite)
                .csv("C:\\Spark\\outputs\\WithCaching_FirstWrite.csv");

        System.out.println("First write complete. Result is now cached.");

        pause("After first write. Go to Spark UI.\n" +
                "\n" +
                "  Storage tab:\n" +
                "    The DataFrame should now appear here.\n" +
                "    You will see: size in memory, fraction cached (100%).\n" +
                "    Storage level: Disk Memory Deserialized 1x Replicated\n" +
                "\n" +
                "  This means the result is stored and ready.\n" +
                "  The second write will NOT re-read the file.\n" +
                "  Watch the Jobs tab after you press ENTER.");

        // Second write — reads from cache, NO file read, NO recomputation
        yellowTaxiGroupedCachedDF.write()
                .option("header", "true")
                .mode(SaveMode.Overwrite)
                .csv("C:\\Spark\\outputs\\WithCaching_SecondWrite.csv");

        long endTime2 = System.currentTimeMillis();
        System.out.println("Second write complete.");
        System.out.println("Time WITH cache: " + (endTime2 - startTime2) + " ms");

        // Release the cache — frees memory for other DataFrames
        /*
         * Always unpersist() when you are done with a cached DataFrame.
         * Spark has limited executor memory. Unreleased cached DataFrames
         * take up space that other operations might need.
         * If memory fills up, Spark starts evicting cached data to disk —
         * then reading it back from disk, defeating the purpose of caching.
         */
        yellowTaxiGroupedCachedDF.unpersist();
        System.out.println("Cache released with unpersist().");

        pause("PART 2 complete. Go to Spark UI.\n" +
                "\n" +
                "  Jobs tab:\n" +
                "    Compare the job durations between Part 1 and Part 2.\n" +
                "    The second write in Part 2 should be dramatically faster.\n" +
                "    It had no file scan, no shuffle — just reading from memory.\n" +
                "\n" +
                "  SQL / DataFrame tab:\n" +
                "    Click the second write job from Part 2.\n" +
                "    The physical plan will show InMemoryTableScan at the bottom.\n" +
                "    Compare to Part 1's second write — it shows FileScan csv.\n" +
                "    That difference IS the value of caching.\n" +
                "\n" +
                "  Storage tab:\n" +
                "    Should be EMPTY again — unpersist() released the cache.");

        // ============================================================
        // SUMMARY
        // ============================================================

        printSection("SUMMARY — persist() and cache()");
        System.out.println(
                "  THE RULE:\n" +
                        "    If a DataFrame is used more than once → cache it.\n" +
                        "    If it is used only once → do not cache (wastes memory).\n" +
                        "\n" +
                        "  HOW IT WORKS:\n" +
                        "    persist() / cache()  → lazy, just marks the DataFrame\n" +
                        "    First action         → computes AND stores the result\n" +
                        "    Subsequent actions   → read from stored result (fast)\n" +
                        "    unpersist()          → release the stored result from memory\n" +
                        "\n" +
                        "  STORAGE LEVELS:\n" +
                        "    MEMORY_ONLY          → RAM only, fastest, may fail if full\n" +
                        "    MEMORY_AND_DISK      → RAM first, spills to disk if needed\n" +
                        "    DISK_ONLY            → disk only, slowest but always works\n" +
                        "    MEMORY_ONLY_SER      → serialized in RAM, smaller footprint\n" +
                        "\n" +
                        "  RECOMMENDATION: Use MEMORY_AND_DISK for production.\n" +
                        "  cache() is shorthand for MEMORY_AND_DISK in Spark 3.x.\n" +
                        "\n" +
                        "  WHEN NOT TO CACHE:\n" +
                        "    DataFrame is used only once in the pipeline\n" +
                        "    DataFrame is very large and barely fits in executor memory\n" +
                        "    Pipeline is a single linear scan-and-write with no branching\n" +
                        "\n" +
                        "  SPARK UI SIGNALS:\n" +
                        "    Storage tab populated     → cache is active\n" +
                        "    InMemoryTableScan in plan → reading from cache\n" +
                        "    FileScan csv in plan      → reading from disk (no cache)"
        );

        System.out.println("\n>>> PAUSED — Check http://localhost:4040 — Press ENTER to exit <<<");
        scanner.nextLine();
        scanner.close();
    }

    private static void pause(String instructions) {
        System.out.println("\n");
        System.out.println("*".repeat(60));
        System.out.println("  >>> STOP — GO TO SPARK UI NOW <<<");
        System.out.println("  http://localhost:4040");
        System.out.println("*".repeat(60));
        System.out.println();
        System.out.println(instructions);
        System.out.println();
        System.out.println("Press ENTER when done exploring the UI...");
        scanner.nextLine();
    }

    private static void printSection(String title) {
        System.out.println("\n");
        System.out.println("=".repeat(60));
        System.out.println("  " + title);
        System.out.println("=".repeat(60));
    }
}