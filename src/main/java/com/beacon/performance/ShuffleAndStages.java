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
 *  SHUFFLE, STAGES AND PARTITIONS — SEEING IT IN SPARK UI
 *  Beacon Solutions, Inc.
 * ============================================================
 *
 *  THE CORE CONCEPT:
 *  -----------------
 *  Not all Spark operations are equal.
 *  Some operations work entirely within a partition — fast, no movement.
 *  Some operations need data from OTHER partitions — expensive, causes shuffle.
 *
 *  NARROW TRANSFORMATIONS — no shuffle:
 *    Data stays in its partition. Each task works independently.
 *    map(), filter(), select(), withColumn(), where()
 *    Result in Spark UI: 1 Stage
 *
 *  WIDE TRANSFORMATIONS — shuffle:
 *    Data must cross partition boundaries.
 *    Records with the same key must land on the same partition.
 *    groupBy(), join(), distinct(), orderBy(), repartition()
 *    Result in Spark UI: 2+ Stages with an Exchange node between them
 *
 *  HOW TO SEE THIS:
 *  ----------------
 *  After EACH step in this file, you will be told exactly what
 *  to look for in the Spark UI before moving on.
 *
 *  Open http://localhost:4040 now and keep it open the entire time.
 *
 *  DATASET: sf-fire-calls.csv — 175,296 rows
 * ============================================================
 */
public class ShuffleAndStages {

    // One Scanner shared across the entire program.
    // Never close System.in mid-run — it cannot be reopened.
    private static final Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Shuffle and Stages - Beacon")
                .master("local[4]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        System.out.println("\n>>> Spark UI: http://localhost:4040 <<<");
        System.out.println(">>> Keep this open for the ENTIRE exercise <<<\n");

        // ============================================================
        // BEFORE WE START — Read This First
        // What Actually Happens Inside Spark, Step by Step
        // ============================================================

        printExplainer();

        pause("Read the explainer above carefully.\n" +
                "Then open http://localhost:4040 in your browser.\n" +
                "The Jobs tab should be empty right now — no jobs yet.\n" +
                "We will fill it up together, one experiment at a time.");

        // ============================================================
        // SETUP — Load the SF Fire Calls dataset
        // ============================================================

        StructType fireSchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("CallNumber",                 DataTypes.IntegerType, true),
                DataTypes.createStructField("UnitID",                     DataTypes.StringType,  true),
                DataTypes.createStructField("IncidentNumber",             DataTypes.IntegerType, true),
                DataTypes.createStructField("CallType",                   DataTypes.StringType,  true),
                DataTypes.createStructField("CallDate",                   DataTypes.StringType,  true),
                DataTypes.createStructField("WatchDate",                  DataTypes.StringType,  true),
                DataTypes.createStructField("CallFinalDisposition",       DataTypes.StringType,  true),
                DataTypes.createStructField("AvailableDtTm",              DataTypes.StringType,  true),
                DataTypes.createStructField("Address",                    DataTypes.StringType,  true),
                DataTypes.createStructField("City",                       DataTypes.StringType,  true),
                DataTypes.createStructField("Zipcode",                    DataTypes.IntegerType, true),
                DataTypes.createStructField("Battalion",                  DataTypes.StringType,  true),
                DataTypes.createStructField("StationArea",                DataTypes.StringType,  true),
                DataTypes.createStructField("Box",                        DataTypes.StringType,  true),
                DataTypes.createStructField("OriginalPriority",           DataTypes.IntegerType, true),
                DataTypes.createStructField("Priority",                   DataTypes.IntegerType, true),
                DataTypes.createStructField("FinalPriority",              DataTypes.IntegerType, true),
                DataTypes.createStructField("ALSUnit",                    DataTypes.BooleanType, true),
                DataTypes.createStructField("CallTypeGroup",              DataTypes.StringType,  true),
                DataTypes.createStructField("NumAlarms",                  DataTypes.IntegerType, true),
                DataTypes.createStructField("UnitType",                   DataTypes.StringType,  true),
                DataTypes.createStructField("UnitSequenceInCallDispatch", DataTypes.IntegerType, true),
                DataTypes.createStructField("FirePreventionDistrict",     DataTypes.StringType,  true),
                DataTypes.createStructField("SupervisorDistrict",         DataTypes.StringType,  true),
                DataTypes.createStructField("Neighborhood",               DataTypes.StringType,  true),
                DataTypes.createStructField("Location",                   DataTypes.StringType,  true),
                DataTypes.createStructField("RowID",                      DataTypes.StringType,  true),
                DataTypes.createStructField("Delay",                      DataTypes.DoubleType,  true)
        });

        Dataset<Row> fireDF = spark
                .read()
                .option("header", "true")
                .schema(fireSchema)
                .csv("C:\\Datasets\\sf-fire\\sf-fire-calls.csv");

        System.out.println("Dataset loaded. 175,296 rows across 4 partitions (local[4]).");
        System.out.println("Total rows: " + fireDF.count());

        pause("SETUP complete. Go to Spark UI → Jobs tab.\n" +
                "  You will see Job 0 from the count() above.\n" +
                "  Click Job 0 → Stages tab.\n" +
                "  Notice: 1 Stage, 4 Tasks (one per partition).\n" +
                "  No shuffle — reading a file is a narrow operation.");

        // ============================================================
        // EXPERIMENT 1 — NARROW TRANSFORMATIONS
        // filter() + select() + withColumn()
        // ============================================================
        /*
         * WHAT WE EXPECT:
         *   Each of these operations works entirely within one partition.
         *   Task 1 handles Partition 1. Task 2 handles Partition 2.
         *   No task needs to see data from another partition.
         *   No data moves across the network.
         *   Spark pipelines all three operations into ONE Stage.
         *
         *  Partition 1 → [filter] → [select] → [withColumn] → result
         *  Partition 2 → [filter] → [select] → [withColumn] → result
         *  Partition 3 → [filter] → [select] → [withColumn] → result
         *  Partition 4 → [filter] → [select] → [withColumn] → result
         *
         *  All run IN PARALLEL. No waiting. No network transfer.
         */

        printSection("EXPERIMENT 1 — NARROW: filter + select + withColumn");
        System.out.println("Operations: filter() → select() → withColumn()");
        System.out.println("Prediction: 1 Stage, 4 Tasks, NO shuffle");
        System.out.println("Running...");

        Dataset<Row> narrowDF = fireDF
                .filter(col("CallType").equalTo("Medical Incident"))
                .select(
                        col("CallNumber"),
                        col("CallType"),
                        col("Neighborhood"),
                        col("Battalion"),
                        col("Delay")
                )
                .withColumn("ResponseSpeed",
                        when(col("Delay").leq(3),  "Fast")
                                .when(col("Delay").leq(10), "Normal")
                                .otherwise("Slow"));

        long narrowCount = narrowDF.count();
        System.out.println("Medical Incidents with speed label: " + narrowCount);
        narrowDF.show(5, false);

        pause("EXPERIMENT 1 complete. Go to Spark UI.\n" +
                "\n" +
                "  JOBS TAB:\n" +
                "    You will see a new job for the count() and show().\n" +
                "    These are narrow — count and show each = 1 Stage.\n" +
                "\n" +
                "  SQL / DATAFRAME TAB:\n" +
                "    Click the most recent query.\n" +
                "    Read the plan BOTTOM UP:\n" +
                "      FileScan csv    → reads the file\n" +
                "      Filter          → your filter(Medical Incident)\n" +
                "      Project         → your select()\n" +
                "      Project         → your withColumn()\n" +
                "    KEY OBSERVATION: NO Exchange node anywhere.\n" +
                "    Exchange = shuffle. No Exchange = no shuffle.\n" +
                "\n" +
                "  STAGES TAB:\n" +
                "    Click the job. You will see exactly 1 Stage.\n" +
                "    4 Tasks inside that stage (one per partition).\n" +
                "    All 4 tasks ran in PARALLEL with no coordination.");

        // ============================================================
        // EXPERIMENT 2 — WIDE TRANSFORMATION: groupBy()
        // ============================================================
        /*
         * WHAT WE EXPECT:
         *   groupBy("Battalion") groups all rows by Battalion value.
         *   The problem: rows for Battalion B02 are spread across
         *   ALL four partitions. To count them, Spark must bring
         *   ALL B02 rows to the SAME partition.
         *
         *   This requires a SHUFFLE — data moves across partitions.
         *
         *   Stage 1: Each partition does a PARTIAL count (fast, local)
         *            Then writes shuffle files to disk
         *   SHUFFLE: Data moves across the network by Battalion key
         *   Stage 2: Final aggregation after shuffle completes
         */

        printSection("EXPERIMENT 2 — WIDE: groupBy() triggers a shuffle");
        System.out.println("Operation: groupBy(Battalion) + count()");
        System.out.println("Prediction: 2 Stages, Exchange node in SQL tab");
        System.out.println("Running...");

        Dataset<Row> groupByDF = fireDF
                .groupBy("Battalion")
                .agg(
                        count("*").alias("TotalCalls"),
                        round(avg("Delay"), 2).alias("AvgDelay")
                )
                .orderBy(col("TotalCalls").desc());

        groupByDF.show(false);

        pause("EXPERIMENT 2 complete. Go to Spark UI.\n" +
                "\n" +
                "  SQL / DATAFRAME TAB:\n" +
                "    Click the most recent query.\n" +
                "    Read the plan BOTTOM UP:\n" +
                "      FileScan csv         → reads the file\n" +
                "      HashAggregate        → partial count per partition (Stage 1)\n" +
                "      Exchange             → THE SHUFFLE HAPPENS HERE\n" +
                "                             hashpartitioning(Battalion)\n" +
                "                             means: all same Battalion values\n" +
                "                             are routed to the same partition\n" +
                "      HashAggregate        → final count after shuffle (Stage 2)\n" +
                "      Sort                 → your orderBy()\n" +
                "\n" +
                "  STAGES TAB:\n" +
                "    Click the groupBy job.\n" +
                "    You will see 2 Stages this time.\n" +
                "    Stage 1: Shuffle Write column is NON-ZERO.\n" +
                "             This is data being written to disk for the shuffle.\n" +
                "    Stage 2: Shuffle Read column is NON-ZERO.\n" +
                "             This is data being read after moving across network.\n" +
                "    The gap between Stage 1 finishing and Stage 2 starting\n" +
                "    IS the shuffle — that is where time is lost.");

        // ============================================================
        // EXPERIMENT 3 — WIDE TRANSFORMATION: distinct()
        // ============================================================
        /*
         * WHAT WE EXPECT:
         *   distinct() removes duplicates across the entire dataset.
         *   To know if a value is truly unique, Spark must compare
         *   it against values in ALL other partitions.
         *   This requires a shuffle — same key must go to same partition.
         */

        printSection("EXPERIMENT 3 — WIDE: distinct() also shuffles");
        System.out.println("Operation: select(CallType).distinct()");
        System.out.println("Prediction: 2 Stages, Exchange in SQL tab");
        System.out.println("Running...");

        Dataset<Row> distinctDF = fireDF
                .select("CallType")
                .distinct()
                .orderBy("CallType");

        distinctDF.show(false);
        System.out.println("Distinct call types: " + distinctDF.count());

        pause("EXPERIMENT 3 complete. Go to Spark UI.\n" +
                "\n" +
                "  SQL / DATAFRAME TAB:\n" +
                "    Find the distinct() query.\n" +
                "    You will see Exchange node again.\n" +
                "    distinct() internally does a groupBy on all columns\n" +
                "    and keeps one row per group — that is why it shuffles.\n" +
                "\n" +
                "  STAGES TAB:\n" +
                "    2 Stages again for the same reason as groupBy.\n" +
                "    Compare the Shuffle Write size between Experiment 2\n" +
                "    and Experiment 3 — which shuffled more data and why?");

        // ============================================================
        // EXPERIMENT 4 — filter BEFORE groupBy (smarter shuffle)
        // ============================================================
        /*
         * QUESTION: Count Medical Incidents per Battalion.
         *
         * filter() THEN groupBy():
         *   filter() is narrow → reduces rows BEFORE the shuffle
         *   groupBy() shuffles only the filtered rows
         *   LESS data goes through the shuffle
         *
         * Catalyst optimizer will push filters down automatically
         * (predicate pushdown). But seeing the pattern helps you
         * write better queries instinctively.
         */

        printSection("EXPERIMENT 4 — NARROW before WIDE: filter then groupBy");
        System.out.println("Approach: filter(Medical) FIRST, then groupBy(Battalion)");
        System.out.println("Only Medical rows enter the shuffle — cheaper.");
        System.out.println("Running...");

        Dataset<Row> smartDF = fireDF
                .filter(col("CallType").equalTo("Medical Incident"))
                .groupBy("Battalion")
                .agg(
                        count("*").alias("MedicalCalls"),
                        round(avg("Delay"), 2).alias("AvgDelay")
                )
                .orderBy(col("MedicalCalls").desc());

        smartDF.show(false);

        pause("EXPERIMENT 4 complete. Go to Spark UI.\n" +
                "\n" +
                "  SQL / DATAFRAME TAB:\n" +
                "    Find this query's physical plan.\n" +
                "    Read bottom up:\n" +
                "      FileScan csv    → reads the file\n" +
                "      Filter          → Medical Incident only (runs BEFORE shuffle)\n" +
                "      HashAggregate   → partial aggregation within each partition\n" +
                "      Exchange        → shuffle happens here\n" +
                "      HashAggregate   → final aggregation\n" +
                "\n" +
                "    Look at PushedFilters inside the FileScan node.\n" +
                "    Catalyst pushed the filter INTO the file read itself.\n" +
                "    This means fewer rows were ever loaded into memory.\n" +
                "\n" +
                "  STAGES TAB:\n" +
                "    Compare the Shuffle Write size of this job\n" +
                "    vs the Experiment 2 groupBy job.\n" +
                "    This one should show LESS shuffle data because\n" +
                "    only Medical Incident rows entered the shuffle.");

        // ============================================================
        // EXPERIMENT 5 — TWO WIDE TRANSFORMATIONS
        // groupBy + orderBy = how many stages?
        // ============================================================

        printSection("EXPERIMENT 5 — TWO WIDE OPS: groupBy + orderBy");
        System.out.println("Operation: groupBy(Neighborhood) → count → orderBy");
        System.out.println("Two wide transformations chained.");
        System.out.println("Running...");

        Dataset<Row> twoWideDF = fireDF
                .groupBy("Neighborhood")
                .agg(
                        count("*").alias("TotalCalls"),
                        round(avg("Delay"), 2).alias("AvgDelay"),
                        round(max("Delay"), 2).alias("MaxDelay")
                )
                .orderBy(col("TotalCalls").desc());

        twoWideDF.show(15, false);

        pause("EXPERIMENT 5 complete. Go to Spark UI.\n" +
                "\n" +
                "  SQL / DATAFRAME TAB:\n" +
                "    Find this query's physical plan.\n" +
                "    Count the number of Exchange nodes.\n" +
                "    You may see 1 or 2 depending on how Catalyst\n" +
                "    optimised the groupBy + orderBy combination.\n" +
                "    AQE (on by default in Spark 3.5) may have\n" +
                "    coalesced some stages automatically.\n" +
                "    Look for AQEShuffleRead node — that is AQE in action.\n" +
                "\n" +
                "  STAGES TAB:\n" +
                "    Count the stages for this job.\n" +
                "    The more unique Neighborhoods, the more data shuffled.\n" +
                "    SF Fire data has 41 unique neighborhoods.");

        // ============================================================
        // EXPERIMENT 6 — CACHING
        // ============================================================
        /*
         * Without cache: file read twice, filter runs twice.
         * With cache: file read once, result stored in memory.
         * Second action reads from cache — no disk read.
         */

        printSection("EXPERIMENT 6 — CACHING: reuse without recomputing");
        System.out.println("Without cache: file read twice, filter runs twice.");
        System.out.println("With cache: file read once, result stored in memory.");
        System.out.println("Running...");

        Dataset<Row> medicalDF = fireDF
                .filter(col("CallType").equalTo("Medical Incident"))
                .select("CallNumber", "Neighborhood", "Battalion", "Delay", "CallDate");

        medicalDF.cache();

        // First action — reads from disk, computes, stores in cache
        System.out.println("Medical calls total: " + medicalDF.count());

        pause("First action complete. Go to Spark UI.\n" +
                "\n" +
                "  STORAGE TAB:\n" +
                "    You will see the cached DataFrame listed here.\n" +
                "    It shows: size in memory, fraction cached,\n" +
                "    storage level (MEMORY_AND_DISK by default).\n" +
                "    Confirm it says 100% cached before moving on.\n" +
                "\n" +
                "  STAGES TAB:\n" +
                "    This count() read from disk — 1 Stage, file scan visible.");

        // Second action — reads from cache, no disk read
        Dataset<Row> byNeighbourhood = medicalDF
                .groupBy("Neighborhood")
                .agg(count("*").alias("MedicalCalls"))
                .orderBy(col("MedicalCalls").desc());

        byNeighbourhood.show(10, false);

        pause("Second action complete. Go to Spark UI.\n" +
                "\n" +
                "  STAGES TAB:\n" +
                "    Click the most recent job.\n" +
                "    Look at Stage 1 — the INPUT column.\n" +
                "    It should say 'In-memory table' not 'csv file'.\n" +
                "    The filter() did NOT re-run. The file was NOT re-read.\n" +
                "    Spark read from the cache — that is the whole point.\n" +
                "\n" +
                "  SQL / DATAFRAME TAB:\n" +
                "    The physical plan will show InMemoryTableScan\n" +
                "    instead of FileScan csv at the bottom.\n" +
                "    That confirms data came from cache, not from disk.");

        medicalDF.unpersist();
        System.out.println("Cache released with unpersist().");

        // ============================================================
        // FINAL SUMMARY
        // ============================================================

        printSection("FINAL SUMMARY — What you observed in Spark UI");
        System.out.println(
                "  Operation                    | Stages | Exchange | Shuffle\n" +
                        "  -----------------------------|--------|----------|--------\n" +
                        "  textFile() (read)            |   1    |    No    |   No  \n" +
                        "  filter()                     |   1    |    No    |   No  \n" +
                        "  select()                     |   1    |    No    |   No  \n" +
                        "  withColumn()                 |   1    |    No    |   No  \n" +
                        "  filter() + select() chained  |   1    |    No    |   No  \n" +
                        "  groupBy() + agg()            |   2    |    YES   |  YES  \n" +
                        "  distinct()                   |   2    |    YES   |  YES  \n" +
                        "  orderBy()                    |   2    |    YES   |  YES  \n" +
                        "  groupBy() + orderBy()        |  2-3   |    YES   |  YES  \n" +
                        "  filter() THEN groupBy()      |   2    |    YES   | LESS  \n" +
                        "\n" +
                        "  THE RULE:\n" +
                        "    Narrow → data stays in partition → 1 Stage\n" +
                        "    Wide   → data crosses partitions → 2+ Stages\n" +
                        "    Every Exchange node in the SQL tab = one shuffle\n" +
                        "    Every shuffle = one Stage boundary\n" +
                        "\n" +
                        "  HOW TO REDUCE SHUFFLE COST:\n" +
                        "    1. Filter early — reduce rows before the shuffle\n" +
                        "    2. Select early — reduce columns before the shuffle\n" +
                        "    3. Cache DataFrames used more than once\n" +
                        "    4. Use broadcast join for small tables (no shuffle at all)\n" +
                        "    5. Trust AQE — it coalesces small shuffle partitions automatically"
        );

        System.out.println("\n>>> PAUSED — Final Spark UI exploration <<<");
        System.out.println("  Find the job with the most Shuffle Write bytes.");
        System.out.println("  Find the job with the fewest stages.");
        System.out.println("  Open the Storage tab — it should be empty (cache released).");
        System.out.println("\nPress ENTER to exit...");
        scanner.nextLine();

        scanner.close();
    }

    // ── Pause helper ──────────────────────────────────────────
    // Uses the shared static Scanner — System.in stays open
    // across all pause() calls throughout the program.
    private static void pause(String uiInstructions) {
        System.out.println("\n");
        System.out.println("*".repeat(60));
        System.out.println("  >>> STOP — GO TO SPARK UI NOW <<<");
        System.out.println("  http://localhost:4040");
        System.out.println("*".repeat(60));
        System.out.println();
        System.out.println(uiInstructions);
        System.out.println();
        System.out.println("Press ENTER when you are done exploring the UI...");
        scanner.nextLine();
    }

    // ── Deep explainer ────────────────────────────────────────
    private static void printExplainer() {

        System.out.println("\n");
        System.out.println("#".repeat(65));
        System.out.println("  HOW SPARK ACTUALLY PROCESSES YOUR DATA — STEP BY STEP");
        System.out.println("#".repeat(65));

        System.out.println(
                "\n" +
                        "  FIRST — UNDERSTAND THE CLUSTER SETUP\n" +
                        "  ---------------------------------------------------------------\n" +
                        "  We are running on local[4]. This means:\n" +
                        "\n" +
                        "    1 JVM process on your laptop\n" +
                        "    1 Driver (your main() function, the brain)\n" +
                        "    4 Cores acting as 4 virtual Executor slots\n" +
                        "\n" +
                        "  When the CSV file is loaded, Spark splits it into\n" +
                        "  PARTITIONS. With local[4], you get 4 partitions.\n" +
                        "  Think of each partition as one chunk of the 175K rows.\n" +
                        "\n" +
                        "    Partition 1 → ~44,000 rows\n" +
                        "    Partition 2 → ~44,000 rows\n" +
                        "    Partition 3 → ~44,000 rows\n" +
                        "    Partition 4 → ~43,296 rows\n" +
                        "\n" +
                        "  Each core processes exactly ONE partition at a time.\n" +
                        "  So all 4 partitions process IN PARALLEL.\n"
        );

        System.out.println(
                "\n" +
                        "  ---------------------------------------------------------------\n" +
                        "  NARROW TRANSFORMATION — WHAT HAPPENS INSIDE SPARK\n" +
                        "  Example: fireDF.filter(col('CallType').equalTo('Medical Incident'))\n" +
                        "  ---------------------------------------------------------------\n" +
                        "\n" +
                        "  Step 1 — You call filter(). NOTHING RUNS.\n" +
                        "           Spark just records your intent in the DAG.\n" +
                        "           No data is read. No computation happens.\n" +
                        "\n" +
                        "  Step 2 — You call count() or show(). THIS triggers execution.\n" +
                        "           Spark looks at the full DAG you have built.\n" +
                        "           It creates a JOB for this action.\n" +
                        "\n" +
                        "  Step 3 — Spark asks: does any operation need data from\n" +
                        "           another partition? For filter(), the answer is NO.\n" +
                        "           Partition 1 can filter itself. Partition 2 can filter\n" +
                        "           itself. They do not need to talk to each other.\n" +
                        "           So Spark puts everything into ONE STAGE.\n" +
                        "\n" +
                        "  Step 4 — Spark launches 4 TASKS simultaneously.\n" +
                        "           Task 1 → Partition 1 → reads 44K rows → keeps matches\n" +
                        "           Task 2 → Partition 2 → reads 44K rows → keeps matches\n" +
                        "           Task 3 → Partition 3 → reads 44K rows → keeps matches\n" +
                        "           Task 4 → Partition 4 → reads 43K rows → keeps matches\n" +
                        "           All 4 run SIMULTANEOUSLY. No waiting.\n" +
                        "\n" +
                        "  Step 5 — Each task finishes independently.\n" +
                        "           Results are collected. No data moved between partitions.\n" +
                        "           No disk write. No network transfer.\n" +
                        "\n" +
                        "  WHAT YOU SEE IN SPARK UI:\n" +
                        "    Jobs tab      → 1 Job\n" +
                        "    Stages tab    → 1 Stage inside that job\n" +
                        "    Tasks         → 4 Tasks, all similar duration\n" +
                        "    SQL tab       → NO Exchange node in the physical plan\n" +
                        "    Shuffle Write → 0 bytes\n" +
                        "    Shuffle Read  → 0 bytes\n"
        );

        System.out.println(
                "\n" +
                        "  ---------------------------------------------------------------\n" +
                        "  WIDE TRANSFORMATION — WHAT HAPPENS INSIDE SPARK\n" +
                        "  Example: fireDF.groupBy('Battalion').agg(count('*'))\n" +
                        "  ---------------------------------------------------------------\n" +
                        "\n" +
                        "  The problem:\n" +
                        "    Battalion B02 rows are spread across ALL 4 partitions.\n" +
                        "    To count ALL B02 rows, one task must see ALL of them.\n" +
                        "    But they are on different partitions — different cores.\n" +
                        "    Spark must MOVE data so all B02 rows land in ONE place.\n" +
                        "    This movement is the SHUFFLE.\n" +
                        "\n" +
                        "  Step 1 — You call groupBy(). NOTHING RUNS. DAG is updated.\n" +
                        "\n" +
                        "  Step 2 — You call show(). Spark creates a JOB.\n" +
                        "           Spark finds a groupBy in the DAG.\n" +
                        "           It creates 2 STAGES with a shuffle in between.\n" +
                        "\n" +
                        "  ---- STAGE 1 BEGINS ----\n" +
                        "\n" +
                        "  Step 3 — Spark launches 4 Tasks (one per partition).\n" +
                        "           Each task reads its partition from the CSV file.\n" +
                        "           All 4 running in parallel.\n" +
                        "\n" +
                        "  Step 4 — Each task does a PARTIAL AGGREGATION locally.\n" +
                        "           Task 1: B01=1200, B02=900, B03=1100 (Partition 1 only)\n" +
                        "           Task 2: B01=1000, B02=1050, B04=800 (Partition 2 only)\n" +
                        "           These are PARTIAL counts — not the final answer yet.\n" +
                        "           This is called the map-side or pre-shuffle aggregation.\n" +
                        "\n" +
                        "  Step 5 — Each task HASHES keys to decide where to send them.\n" +
                        "           hash('B01') % 200 = Reducer slot 45\n" +
                        "           hash('B02') % 200 = Reducer slot 12\n" +
                        "           hash('B03') % 200 = Reducer slot 78\n" +
                        "           Every task that has B01 rows sends them to slot 45.\n" +
                        "           This guarantees: same key → same reducer → correct count.\n" +
                        "\n" +
                        "  Step 6 — Each task WRITES its partial results to local disk.\n" +
                        "           These are called SHUFFLE FILES (or spill files).\n" +
                        "           This disk write is what you see in SHUFFLE WRITE column.\n" +
                        "\n" +
                        "  ---- STAGE 1 ENDS — THE SHUFFLE BEGINS ----\n" +
                        "\n" +
                        "  Step 7 — Data moves across the network.\n" +
                        "           All B01 partial counts travel to Reducer slot 45.\n" +
                        "           All B02 partial counts travel to Reducer slot 12.\n" +
                        "           This is pure network transfer — the most expensive step.\n" +
                        "\n" +
                        "  ---- STAGE 2 BEGINS ----\n" +
                        "\n" +
                        "  Step 8 — New tasks read their shuffle input from disk.\n" +
                        "           This read is what you see in SHUFFLE READ column.\n" +
                        "           Reducer 45 now has ALL partial B01 counts from all 4 partitions.\n" +
                        "\n" +
                        "  Step 9 — Final aggregation.\n" +
                        "           B01: 1200 + 1000 + 900 + ... = final correct total.\n" +
                        "           Now the answer is correct and complete.\n" +
                        "\n" +
                        "  Step 10 — Results returned. Job complete.\n" +
                        "\n" +
                        "  WHAT YOU SEE IN SPARK UI:\n" +
                        "    Jobs tab      → 1 Job\n" +
                        "    Stages tab    → 2 Stages inside that job\n" +
                        "    Stage 1       → Shuffle Write is non-zero (wrote to disk)\n" +
                        "    Stage 2       → Shuffle Read is non-zero (read from disk)\n" +
                        "    SQL tab       → Exchange node between the two HashAggregate nodes\n" +
                        "    Stage 1 tasks → 4 (one per input partition)\n" +
                        "    Stage 2 tasks → up to 200 (default shuffle.partitions)\n" +
                        "                    AQE coalesces the empty ones automatically\n"
        );

        System.out.println(
                "\n" +
                        "  ---------------------------------------------------------------\n" +
                        "  KEY DIFFERENCES AT A GLANCE\n" +
                        "  ---------------------------------------------------------------\n" +
                        "\n" +
                        "                     NARROW              WIDE\n" +
                        "                     ------              ----\n" +
                        "  Data movement:     None                Across partitions\n" +
                        "  Disk write:        No                  Yes (shuffle files)\n" +
                        "  Network transfer:  No                  Yes\n" +
                        "  Stages created:    1                   2 or more\n" +
                        "  Exchange node:     Absent              Present\n" +
                        "  Shuffle Write:     0 bytes             Non-zero\n" +
                        "  Shuffle Read:      0 bytes             Non-zero\n" +
                        "  Examples:          filter, select,     groupBy, join,\n" +
                        "                     withColumn, map     distinct, orderBy\n" +
                        "\n" +
                        "  THE SHUFFLE IS THE MOST EXPENSIVE THING SPARK DOES.\n" +
                        "  Every performance optimisation you will ever do in Spark\n" +
                        "  is about reducing how much data crosses the shuffle boundary.\n" +
                        "  ---------------------------------------------------------------\n"
        );
    }

    private static void printSection(String title) {
        System.out.println("\n");
        System.out.println("=".repeat(60));
        System.out.println("  " + title);
        System.out.println("=".repeat(60));
    }
}