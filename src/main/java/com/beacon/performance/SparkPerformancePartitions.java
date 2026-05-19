package com.beacon.performance;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Scanner;

import static org.apache.spark.sql.functions.*;

/**
 * ============================================================
 *  SPARK PERFORMANCE — PARTITIONS AND REPARTITIONING
 *  Beacon Solutions, Inc.
 * ============================================================
 *
 *  THE CORE IDEA:
 *  --------------
 *  Parallelism in Spark is controlled by PARTITIONS.
 *  More partitions = more tasks = more parallelism.
 *  Wrong partition count = wasted cores or overloaded cores.
 *
 *  WHAT WE COVER:
 *  --------------
 *  1. Default partition behaviour for small and large files
 *  2. How spark.sql.shuffle.partitions affects groupBy output
 *  3. Round Robin repartition  — even distribution
 *  4. Hash repartition         — same key to same partition
 *  5. Range repartition        — ordered ranges across partitions
 *  6. getDataFrameStats()      — inspect partition distribution
 *
 *  DATASETS:
 *    TaxiZones.csv           → 265 rows (small file)
 *    YellowTaxis_202210.csv  → 3.6M rows (large file)
 * ============================================================
 */
public class SparkPerformancePartitions {

    // One Scanner shared for the whole program — never close System.in mid-run
    private static final Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Spark Performance - Partitions")
//                .config("spark.sql.adaptive.enabled", "false")
                .master("local[4]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");
        System.out.println("\n>>> Spark UI: http://localhost:4040 <<<\n");

        JavaSparkContext context = new JavaSparkContext(spark.sparkContext());

        // ============================================================
        // STEP 1 — Default parallelism settings
        // ============================================================
        /*
         * defaultParallelism  → number of cores available = max parallel tasks
         * defaultMinPartitions → minimum partitions Spark creates when reading
         *
         * With local[4]:
         *   defaultParallelism  = 4
         *   defaultMinPartitions = 2
         */

        printSection("STEP 1 — Default parallelism settings");

        System.out.println("Default Parallelism:    " + context.defaultParallelism());
        System.out.println("Default Min Partitions: " + context.defaultMinPartitions());

        // ============================================================
        // STEP 2 — Partitions for a small file (TaxiZones — 265 rows)
        // ============================================================
        /*
         * Spark determines partition count based on file size.
         * Default: spark.sql.files.maxPartitionBytes = 128 MB per partition.
         *
         * TaxiZones.csv is tiny (a few KB).
         * Spark creates 1 partition because the file fits easily in one block.
         * With 1 partition, only 1 task runs — 3 cores sit idle.
         * This is fine for small files — the overhead of splitting isn't worth it.
         */

        printSection("STEP 2 — Partitions for a small file: TaxiZones.csv");

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

        System.out.println("TaxiZones partitions: " + taxiZonesDF.rdd().getNumPartitions());
        // Output: 1 — tiny file, one partition
        System.out.println("TaxiZones row count:  " + taxiZonesDF.count());
        // Output: 265

        // ============================================================
        // STEP 3 — Partitions for a large file (YellowTaxis — 3.6M rows)
        // ============================================================
        /*
         * Formula:  Number of partitions = File size / maxPartitionBytes
         *
         * YellowTaxis_202210.csv ≈ 300 MB
         * 300 MB / 128 MB = ~3 partitions
         *
         * Each partition gets processed by one core.
         * With local[4] and 3 partitions, 3 cores are busy and 1 is idle.
         *
         * You can control this with:
         *   spark.conf().set("spark.sql.files.maxPartitionBytes", "64m")
         * Smaller maxPartitionBytes → more partitions → more parallelism
         * (uncomment the line below and re-run to see the difference)
         */

        printSection("STEP 3 — Partitions for a large file: YellowTaxis_202210.csv");

        // Uncomment to create more partitions at read time:
         spark.conf().set("spark.sql.files.maxPartitionBytes", "64m");

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

        System.out.println("YellowTaxis partitions (at read): " + yellowTaxiDF.rdd().getNumPartitions());

        long count = yellowTaxiDF.count();
        System.out.println("YellowTaxis row count: " + count);

        // ============================================================
        // STEP 4 — spark.sql.shuffle.partitions
        // ============================================================
        /*
         * When a shuffle happens (groupBy, join, distinct),
         * Spark creates output partitions based on this setting.
         *
         * Default: spark.sql.shuffle.partitions = 200
         *
         * For 265 unique PULocationIDs, 200 partitions is reasonable.
         * But for a tiny dataset, 200 partitions means most are empty.
         * Here we set it to 3 to keep the output manageable.
         *
         * AQE (enabled by default in Spark 3.5) will coalesce
         * empty shuffle partitions automatically — but it's still
         * good practice to set this thoughtfully.
         */

        printSection("STEP 4 — spark.sql.shuffle.partitions controls groupBy output");

        spark.conf().set("spark.sql.shuffle.partitions", 3);

        Dataset<Row> yellowTaxiGroupedDF = yellowTaxiDF
                .groupBy("PULocationID")
                .agg(sum("total_amount").alias("TotalAmount"));

        System.out.println("Partitions after groupBy (shuffle): "
                + yellowTaxiGroupedDF.rdd().getNumPartitions());
        // Output: 3 — because we set shuffle.partitions = 3

        System.out.println("Grouped row count: " + yellowTaxiGroupedDF.count());

        System.out.println("\n--- Partition distribution of grouped data ---");
        getDataFrameStats(yellowTaxiGroupedDF, "PULocationID").show();
        /*
         * Shows how many PULocationIDs landed in each of the 3 partitions.
         * Uneven distribution here is normal — hash partitioning assigns
         * keys to partitions based on hash(key) % numPartitions.
         */

        System.out.println("\n--- Partition distribution of raw YellowTaxi data ---");
        getDataFrameStats(yellowTaxiDF, "PULocationID").show();
        /*
         * Shows the original partition distribution before any shuffle.
         * With round-robin (default read), PULocationIDs are spread
         * roughly evenly across partitions.
         */

        // ============================================================
        // STEP 5 — Round Robin Repartitioning
        // ============================================================
        /*
         * repartition(n) — splits data into n partitions evenly
         * using a round-robin algorithm.
         * Records are distributed by cycling through partitions: 1,2,3,4,1,2,3,4...
         *
         * USE WHEN:
         *   You want maximum parallelism regardless of key values.
         *   The dataset is skewed and you want even distribution.
         *
         * COST: Always causes a full shuffle.
         * In Spark UI: you will see an Exchange node.
         */

        printSection("STEP 5 — Round Robin repartition(14)");

        Dataset<Row> roundRobinDF = yellowTaxiDF.repartition(14);

        System.out.println("Partitions after round-robin: "
                + roundRobinDF.rdd().getNumPartitions());
        // Output: 14 — exactly as requested

        System.out.println("\n--- Partition distribution after round-robin ---");
        getDataFrameStats(roundRobinDF, "PULocationID").show();
        /*
         * Notice: all 14 partitions have similar Record Counts.
         * But PULocationID ranges OVERLAP between partitions.
         * Round-robin doesn't group by key — it just distributes evenly.
         */

        // ============================================================
        // STEP 6 — Hash Repartitioning (by column)
        // ============================================================
        /*
         * repartition(n, col) — routes rows to a partition
         * based on hash(colValue) % n.
         *
         * KEY PROPERTY: ALL rows with the same PULocationID value
         * will ALWAYS land in the SAME partition.
         *
         * USE WHEN:
         *   You are about to join on this column — co-locating
         *   join keys avoids a re-shuffle during the join.
         *   You want to group related data for processing.
         *
         * COST: Full shuffle.
         */

        printSection("STEP 6 — Hash repartition(14, PULocationID)");

        Dataset<Row> hashRepartitionedDF = yellowTaxiDF.repartition(14, col("PULocationID"));

        System.out.println("Partitions after hash repartition: "
                + hashRepartitionedDF.rdd().getNumPartitions());

        System.out.println("\n--- Partition distribution after hash repartition ---");
        getDataFrameStats(hashRepartitionedDF, "PULocationID").show();
        /*
         * Notice two things:
         *   1. Min and Max PULocationID per partition do NOT overlap.
         *      (Some partitions may have gaps — that is the hash function at work)
         *   2. Record counts may be uneven — depends on how many trips
         *      each PULocationID has. High-volume locations get more rows.
         *
         * This is good for joins — if you then join on PULocationID,
         * Spark knows matching keys are already on the same partition.
         */

        // ============================================================
        // STEP 7 — Range Repartitioning (ordered ranges)
        // ============================================================
        /*
         * repartitionByRange(n, col) — splits data into n ordered
         * ranges of the column value.
         *
         * Partition 1 gets low PULocationID values (e.g. 1–18)
         * Partition 2 gets the next range (e.g. 19–37)
         * ...
         * Partition 14 gets high values (e.g. 248–265)
         *
         * USE WHEN:
         *   You need sorted output within each partition.
         *   You are writing range-partitioned data to disk.
         *   You want to inspect data for a specific key range.
         *
         * COST: Full shuffle (samples the data first to determine ranges).
         */

        printSection("STEP 7 — Range repartition by PULocationID");

        Dataset<Row> rangeRepartitionedDF = yellowTaxiDF
                .repartitionByRange(14, col("PULocationID"));

        // Add a column showing which partition each row is in
        rangeRepartitionedDF = rangeRepartitionedDF
                .withColumn("PID", spark_partition_id());

        rangeRepartitionedDF.createOrReplaceTempView("repartitioned");

        // Inspect what is in partition 2
        System.out.println("--- Contents of partition 2 (sample) ---");
        spark.sql("SELECT * FROM repartitioned WHERE PID = 2 LIMIT 10").show(false);
        /*
         * All rows in partition 2 will have PULocationID values
         * in a specific contiguous range — that is range partitioning.
         */

        System.out.println("Partitions after range repartition: "
                + rangeRepartitionedDF.rdd().getNumPartitions());

        System.out.println("\n--- Partition distribution after range repartition ---");
        getDataFrameStats(rangeRepartitionedDF, "PULocationID").show();
        /*
         * Notice:
         *   Min and Max PULocationID per partition are CONTIGUOUS ranges.
         *   No overlap between partitions.
         *   Partition 1 has the lowest IDs, Partition 14 has the highest.
         */

        // ============================================================
        // SUMMARY
        // ============================================================

        printSection("SUMMARY — Which repartition strategy to use?");
        System.out.println(
                "  Configuration:\n" +
                        "    spark.sql.files.maxPartitionBytes  → controls partitions at READ time\n" +
                        "    spark.sql.shuffle.partitions       → controls partitions after SHUFFLE\n" +
                        "\n" +
                        "  Repartition strategies:\n" +
                        "\n" +
                        "    repartition(n)\n" +
                        "      Round-robin. Even distribution. No key grouping.\n" +
                        "      Use: fix skewed data, increase parallelism generally.\n" +
                        "\n" +
                        "    repartition(n, col)\n" +
                        "      Hash by column. Same key always in same partition.\n" +
                        "      Use: before a join on that column (avoids re-shuffle).\n" +
                        "\n" +
                        "    repartitionByRange(n, col)\n" +
                        "      Ordered ranges. Min-max per partition is contiguous.\n" +
                        "      Use: sorted output, range-based processing.\n" +
                        "\n" +
                        "    coalesce(n)\n" +
                        "      Reduces partitions WITHOUT a full shuffle.\n" +
                        "      Use: reduce partition count cheaply after filtering.\n" +
                        "      Cannot increase partition count — use repartition for that.\n" +
                        "\n" +
                        "  Spark UI — what to look for:\n" +
                        "    Stages tab → getDataFrameStats() creates a groupBy job\n" +
                        "                 showing 2 stages (the stats groupBy shuffles too)\n" +
                        "    SQL tab   → repartition shows Exchange node\n" +
                        "                repartitionByRange shows Exchange + Sort nodes"
        );

        System.out.println("\n>>> PAUSED — Check http://localhost:4040 — Press ENTER to exit <<<");
        scanner.nextLine();
        scanner.close();
    }

    // ── Utility: show partition distribution for any DataFrame ──
    /*
     * Adds a partition ID column, then groups by it to show:
     *   - How many records are in each partition
     *   - The min and max value of a chosen column per partition
     *
     * Use this to detect skew (one partition much larger than others)
     * and to verify that repartitioning worked as expected.
     */
    public static Dataset<Row> getDataFrameStats(Dataset<Row> dataFrame, String columnName) {
        return dataFrame
                .withColumn("Partition Number", functions.spark_partition_id())
                .groupBy("Partition Number")
                .agg(
                        count("*").alias("Record Count"),
                        min(columnName).alias("Min Column Value"),
                        max(columnName).alias("Max Column Value")
                )
                .orderBy("Partition Number");
    }

    private static void printSection(String title) {
        System.out.println("\n");
        System.out.println("=".repeat(60));
        System.out.println("  " + title);
        System.out.println("=".repeat(60));
    }
}