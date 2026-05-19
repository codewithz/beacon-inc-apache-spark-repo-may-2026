package com.beacon.performance;

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
 *  SPARK PERFORMANCE — JOIN STRATEGIES
 *  Beacon Solutions, Inc.
 * ============================================================
 *
 *  THE PROBLEM WITH JOINS:
 *  -----------------------
 *  Joins are the most expensive operation in Spark.
 *  To join two DataFrames, matching keys must land on the
 *  same executor. If both DataFrames are large, Spark must
 *  shuffle BOTH sides across the network — double the cost.
 *
 *  SPARK HAS THREE JOIN STRATEGIES:
 *  ----------------------------------
 *  1. BROADCAST HASH JOIN (fast — no shuffle)
 *     One side is small enough to fit in memory on every executor.
 *     Spark sends a full copy to every executor (broadcasts it).
 *     Each executor then joins its partition locally.
 *     No data movement for the large side. Very fast.
 *     Triggered when: table size < autoBroadcastJoinThreshold (10MB default)
 *
 *  2. SORT MERGE JOIN (expensive — shuffles both sides)
 *     Both sides are large. Spark shuffles both sides by join key,
 *     sorts both, then merges matching keys.
 *     Two full shuffles. Slowest but always correct.
 *     Triggered when: both sides exceed the broadcast threshold.
 *
 *  3. SHUFFLE HASH JOIN (one side hashed, then joined)
 *     One side is hashed into memory, other side streams through.
 *     Less common — Spark uses Sort Merge Join by default for large data.
 *
 *  WHAT WE COVER:
 *  --------------
 *  1. Broadcast join (large × small) — what Spark does automatically
 *  2. Seeing BroadcastHashJoin in Spark UI SQL tab
 *  3. Disabling broadcast to force Sort Merge Join
 *  4. Seeing SortMergeJoin in Spark UI SQL tab
 *  5. Large × Large join — the most expensive case
 *  6. Forcing a broadcast with hint
 *
 *  DATASETS:
 *    TaxiZones.csv          → 265 rows (very small — perfect for broadcast)
 *    YellowTaxis_202210.csv → 3.6M rows (large)
 * ============================================================
 */
public class SparkPerformanceJoins {

    private static final Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Spark Performance - Joins")
                .master("local[4]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");
        System.out.println("\n>>> Spark UI: http://localhost:4040 <<<\n");

        // ============================================================
        // SETUP — Load both datasets
        // ============================================================

        StructType taxiZonesSchema = new StructType()
                .add("LocationID",  DataTypes.IntegerType, true)
                .add("Borough",     DataTypes.StringType,  true)
                .add("Zone",        DataTypes.StringType,  true)
                .add("ServiceZone", DataTypes.StringType,  true);

        Dataset<Row> taxiZonesDF = spark
                .read()
                .option("header", "true")
                .schema(taxiZonesSchema)
                .csv("C:\\Datasets\\TaxiZones.csv");

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
                .csv("C:\\Datasets\\YellowTaxis_202210.csv");

        System.out.println("TaxiZones row count:  " + taxiZonesDF.count());
        System.out.println("YellowTaxi row count: " + yellowTaxiDF.count());

        // ============================================================
        // STEP 1 — Check the broadcast threshold
        // ============================================================
        /*
         * autoBroadcastJoinThreshold controls when Spark automatically
         * decides to broadcast a table.
         *
         * Default: 10485760 bytes = 10 MB
         *
         * TaxiZones.csv is a few KB — well under 10 MB.
         * Spark will automatically broadcast it without you asking.
         *
         * You can:
         *   Raise it  → more tables get broadcast automatically
         *   Lower it  → fewer tables broadcast (force Sort Merge Join)
         *   Set to -1 → disable all automatic broadcasting
         */

        printSection("STEP 1 — Broadcast join threshold");

        String threshold = spark.conf().get("spark.sql.autoBroadcastJoinThreshold");
        System.out.println("autoBroadcastJoinThreshold: " + threshold);
        System.out.println("Default is 10MB — TaxiZones is a few KB, well under the threshold.");
        System.out.println("Spark will AUTOMATICALLY broadcast TaxiZones.");

        // ============================================================
        // STEP 2 — Broadcast Hash Join (automatic)
        // Large (3.6M rows) × Small (265 rows)
        // ============================================================
        /*
         * HOW BROADCAST JOIN WORKS:
         *
         * Step 1: Spark reads TaxiZones (265 rows — tiny).
         * Step 2: Driver collects all 265 rows.
         * Step 3: Driver broadcasts a copy to EVERY executor.
         * Step 4: Each executor now has a local copy of TaxiZones.
         * Step 5: Each YellowTaxi partition joins locally against
         *         its own copy of TaxiZones — no data movement.
         *
         * Result: 0 shuffles. Very fast.
         *
         * In Spark UI SQL tab you will see:
         *   BroadcastExchange → the small table being sent to all executors
         *   BroadcastHashJoin → the local join happening per partition
         *   NO Exchange on the large side → YellowTaxi did not shuffle
         */

        printSection("STEP 2 — Broadcast Hash Join (automatic): Large x Small");
        System.out.println("Joining 3.6M YellowTaxi rows with 265 TaxiZones rows.");
        System.out.println("Spark will auto-broadcast TaxiZones (under 10MB threshold).");
        System.out.println("Prediction: BroadcastHashJoin in SQL tab, 1 Stage, no shuffle.");
        System.out.println("Running...");

        Dataset<Row> broadcastJoinDF = yellowTaxiDF.join(
                taxiZonesDF,
                yellowTaxiDF.col("PULocationID").equalTo(taxiZonesDF.col("LocationID")),
                "inner"
        );

        broadcastJoinDF.show(5, false);
        System.out.println("Joined row count: " + broadcastJoinDF.count());

        pause("STEP 2 complete. Go to Spark UI → SQL / DataFrame tab.\n" +
                "\n" +
                "  Find the join query and click it.\n" +
                "  Read the physical plan BOTTOM UP:\n" +
                "\n" +
                "    Scan csv (YellowTaxi)     → reads the large table\n" +
                "    BroadcastExchange          → TaxiZones sent to all executors\n" +
                "    BroadcastHashJoin          → local join on each partition\n" +
                "\n" +
                "  KEY OBSERVATION:\n" +
                "    There is NO Exchange node on the YellowTaxi side.\n" +
                "    The large table did NOT shuffle. Only the small table moved.\n" +
                "    That is why broadcast joins are so fast.\n" +
                "\n" +
                "  Stages tab:\n" +
                "    This join produced only 1 Stage.\n" +
                "    Compare this to the Sort Merge Join you will see next.");

        // ============================================================
        // STEP 3 — Disable Broadcast, Force Sort Merge Join
        // ============================================================
        /*
         * Setting autoBroadcastJoinThreshold to -1 tells Spark:
         * "never broadcast automatically — always shuffle both sides."
         *
         * This forces a SORT MERGE JOIN even though TaxiZones is tiny.
         *
         * HOW SORT MERGE JOIN WORKS:
         *
         * Step 1: Both sides are shuffled by join key (PULocationID / LocationID).
         *         All rows with key=4 from YellowTaxi → same partition.
         *         All rows with key=4 from TaxiZones  → same partition.
         * Step 2: Each partition sorts its rows by join key.
         * Step 3: Sorted partitions are merged — matching keys joined.
         *
         * Result: 2 full shuffles + sort. Expensive.
         *
         * In Spark UI SQL tab you will see:
         *   Exchange hashpartitioning on YellowTaxi side
         *   Exchange hashpartitioning on TaxiZones side
         *   Sort on both sides
         *   SortMergeJoin
         */

        printSection("STEP 3 — Force Sort Merge Join (disable broadcast)");
        System.out.println("Setting autoBroadcastJoinThreshold = -1");
        System.out.println("This disables automatic broadcasting completely.");
        System.out.println("Spark will now shuffle BOTH sides — even the tiny TaxiZones.");
        System.out.println("Prediction: SortMergeJoin in SQL tab, 2+ Stages, two shuffles.");
        System.out.println("Running...");

        // Disable broadcast
        spark.conf().set("spark.sql.autoBroadcastJoinThreshold", "-1");

        Dataset<Row> sortMergeJoinDF = yellowTaxiDF.join(
                taxiZonesDF,
                yellowTaxiDF.col("PULocationID").equalTo(taxiZonesDF.col("LocationID")),
                "inner"
        );

        sortMergeJoinDF.show(5, false);

        pause("STEP 3 complete. Go to Spark UI → SQL / DataFrame tab.\n" +
                "\n" +
                "  Find the most recent join query and click it.\n" +
                "  Read the physical plan BOTTOM UP:\n" +
                "\n" +
                "    Scan csv (YellowTaxi)     → reads large table\n" +
                "    Exchange hashpartitioning  → YellowTaxi shuffled by PULocationID\n" +
                "    Sort                       → sorted after shuffle\n" +
                "    Scan csv (TaxiZones)       → reads small table\n" +
                "    Exchange hashpartitioning  → TaxiZones ALSO shuffled (wasteful!)\n" +
                "    Sort                       → sorted after shuffle\n" +
                "    SortMergeJoin              → merge matching keys\n" +
                "\n" +
                "  KEY OBSERVATION:\n" +
                "    Two Exchange nodes — both sides shuffled.\n" +
                "    Even the 265-row TaxiZones was shuffled across the network.\n" +
                "    This is what happens without broadcast.\n" +
                "\n" +
                "  Stages tab:\n" +
                "    Multiple stages — one per shuffle boundary.\n" +
                "    Shuffle Write and Shuffle Read are both non-zero.\n" +
                "    Compare the duration to STEP 2's broadcast join.");

        // Re-enable broadcast for subsequent steps
        spark.conf().set("spark.sql.autoBroadcastJoinThreshold", "10485760");
        System.out.println("Broadcast threshold restored to 10MB.");

        // ============================================================
        // STEP 4 — Force Broadcast with Hint
        // ============================================================
        /*
         * Sometimes Spark cannot automatically detect that a table
         * is small enough to broadcast — for example, if the table
         * comes from a subquery and statistics are not available.
         *
         * You can FORCE a broadcast with the broadcast() hint:
         *   broadcast(dataFrame)  → in DataFrame API
         *   SQL: SELECT /*+ BROADCAST(alias) *\/ ...
         *
         * Spark will honour the hint as long as the broadcasted
         * data physically fits in executor memory.
         */

        printSection("STEP 4 — Force Broadcast with hint");
        System.out.println("Using broadcast() hint to explicitly force broadcast.");
        System.out.println("Useful when Spark cannot auto-detect the table is small.");
        System.out.println("Running...");

        // First disable auto-broadcast to prove the hint works
        spark.conf().set("spark.sql.autoBroadcastJoinThreshold", "-1");

        Dataset<Row> hintBroadcastDF = yellowTaxiDF.join(
                broadcast(taxiZonesDF),   // force TaxiZones to be broadcast
                yellowTaxiDF.col("PULocationID").equalTo(taxiZonesDF.col("LocationID")),
                "inner"
        );

        hintBroadcastDF.show(5, false);

        pause("STEP 4 complete. Go to Spark UI → SQL / DataFrame tab.\n" +
                "\n" +
                "  Even though autoBroadcastJoinThreshold = -1 (disabled),\n" +
                "  the plan should still show BroadcastHashJoin.\n" +
                "  The hint overrode the threshold setting.\n" +
                "\n" +
                "  This is useful in production when:\n" +
                "    - Table statistics are stale or unavailable\n" +
                "    - You know a table is small but Spark does not\n" +
                "    - AQE switches to sort-merge but you want broadcast\n" +
                "\n" +
                "  Re-enable threshold before the next step.");

        // Re-enable for final experiment
        spark.conf().set("spark.sql.autoBroadcastJoinThreshold", "10485760");

        // ============================================================
        // STEP 5 — Large × Large Join (Sort Merge Join — unavoidable)
        // ============================================================
        /*
         * When BOTH sides of the join are large, broadcast is not
         * possible — you cannot fit 3.6M rows in every executor's memory.
         *
         * Spark must use Sort Merge Join — shuffle both sides.
         * This is the most expensive join and cannot be avoided
         * when both tables are genuinely large.
         *
         * MITIGATION STRATEGIES:
         *   1. Pre-partition both DataFrames on the join key before joining
         *      → repartition(n, col) → avoids one shuffle
         *   2. Cache one side if it will be joined multiple times
         *   3. Filter before joining to reduce row count entering the shuffle
         *
         * Here we join YellowTaxi with itself on a different key
         * to demonstrate the Large × Large scenario.
         */

        printSection("STEP 5 — Large x Large Join (Sort Merge Join)");
        System.out.println("Joining YellowTaxi with itself: PULocationID = DOLocationID.");
        System.out.println("Both sides are 3.6M rows — too large to broadcast either side.");
        System.out.println("Prediction: SortMergeJoin in SQL tab, multiple stages, both sides shuffle.");
        System.out.println("Running...");

        yellowTaxiDF.createOrReplaceTempView("YellowTaxi1");
        yellowTaxiDF.createOrReplaceTempView("YellowTaxi2");

        Dataset<Row> largeJoinDF = spark.sql(
                "SELECT yt1.PULocationID, yt1.total_amount, yt2.DOLocationID " +
                        "FROM YellowTaxi1 yt1 " +
                        "JOIN YellowTaxi2 yt2 ON yt1.PULocationID = yt2.DOLocationID " +
                        "LIMIT 10"
        );

        largeJoinDF.show(false);

        pause("STEP 5 complete. Go to Spark UI → SQL / DataFrame tab.\n" +
                "\n" +
                "  Find the self-join query and click it.\n" +
                "  You will see:\n" +
                "    Two Scan csv nodes (reading the file twice)\n" +
                "    Two Exchange nodes (shuffling both sides)\n" +
                "    Two Sort nodes\n" +
                "    SortMergeJoin\n" +
                "\n" +
                "  This is the most expensive join pattern.\n" +
                "  Both sides of the file are shuffled across the network.\n" +
                "\n" +
                "  Stages tab:\n" +
                "    Multiple stages with large Shuffle Write AND Shuffle Read.\n" +
                "    The duration will be noticeably longer than the broadcast join.");

        // ============================================================
        // SUMMARY
        // ============================================================

        printSection("SUMMARY — Join Strategies");
        System.out.println(
                "  JOIN STRATEGY COMPARISON:\n" +
                        "\n" +
                        "                    Broadcast Hash Join   Sort Merge Join\n" +
                        "                    -------------------   ---------------\n" +
                        "  When used:        One side < threshold  Both sides large\n" +
                        "  Shuffles:         0 (large side)        2 (both sides)\n" +
                        "  Exchange nodes:   1 (BroadcastExchange) 2\n" +
                        "  Sort required:    No                    Yes (both sides)\n" +
                        "  Stages:           1                     2+\n" +
                        "  Speed:            Fast                  Slow\n" +
                        "\n" +
                        "  HOW TO CONTROL:\n" +
                        "    Auto-broadcast threshold:\n" +
                        "      spark.sql.autoBroadcastJoinThreshold = 10485760  (10MB default)\n" +
                        "      Set higher → more auto-broadcasting\n" +
                        "      Set to -1  → disable all auto-broadcasting\n" +
                        "\n" +
                        "    Force broadcast with hint:\n" +
                        "      DataFrame API: yellowTaxiDF.join(broadcast(taxiZonesDF), ...)\n" +
                        "      SQL: SELECT /*+ BROADCAST(alias) */ ...\n" +
                        "\n" +
                        "  SPARK UI — what to look for:\n" +
                        "    BroadcastExchange → small table being copied to all executors\n" +
                        "    BroadcastHashJoin → fast local join, no shuffle on large side\n" +
                        "    Exchange          → a shuffle happened on that side\n" +
                        "    SortMergeJoin     → both sides shuffled and sorted\n" +
                        "\n" +
                        "  RULE: if you can broadcast the smaller side → always do it.\n" +
                        "        One Exchange node is always better than two."
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