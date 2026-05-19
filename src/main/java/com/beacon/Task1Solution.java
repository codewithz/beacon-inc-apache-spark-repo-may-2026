

package com.beacon;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Scanner;

/**
 * ============================================================
 *  SOLUTION SHEET — SF Fire Department Calls SQL Lab
 *  Beacon Solutions, Inc.

 * ============================================================
 */
public class Task1Solution {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("SF Fire Calls SQL Lab - Solutions")
                .master("local[4]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");
        System.out.println("\n>>> Spark UI: http://localhost:4040 <<<\n");

        // ============================================================
        // SETUP
        // ============================================================

        String filePath = "C:\\Datasets\\sf-fire-calls.csv";

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
                .csv(filePath);

        fireDF.createOrReplaceTempView("FireCalls");

        System.out.println("FireCalls view registered. Total rows: " + fireDF.count());

        // ============================================================
        // TASK 1 — How many distinct call types are there?
        // ============================================================
        /*
         * SOLUTION NOTES:
         *   COUNT(DISTINCT column) counts unique values.
         *   Alternative: SELECT DISTINCT CallType then count the rows.
         *   Both are valid — COUNT(DISTINCT ...) is the cleaner SQL.
         *
         * EXPECTED OUTPUT: 33
         *
         * SPARK UI: 1 Stage — no shuffle needed for COUNT DISTINCT
         *           on a single column with no GROUP BY.
         */

        System.out.println("\n--- TASK 1 SOLUTION: How many distinct call types? ---");

        spark.sql(
                "SELECT COUNT(DISTINCT CallType) AS DistinctCallTypes FROM FireCalls"
        ).show();
        /*
         * +-----------------+
         * |DistinctCallTypes|
         * +-----------------+
         * |               33|
         * +-----------------+
         */

        // ============================================================
        // TASK 2 — Show all calls where CallType is 'Structure Fire'
        // ============================================================
        /*
         * SOLUTION NOTES:
         *   WHERE with string equality — single quotes required.
         *   Selecting specific columns keeps output readable.
         *   LIMIT prevents flooding the console.
         *
         * EXPECTED OUTPUT: 10 rows of Structure Fire calls.
         *
         * SPARK UI: 1 Stage — simple filter + column selection.
         *           SQL tab: FileScan -> Filter -> Project
         *           Look for PushedFilters in the FileScan node —
         *           Catalyst pushed the WHERE into the file read itself.
         */

        System.out.println("\n--- TASK 2 SOLUTION: Structure Fire calls ---");

        spark.sql(
                "SELECT CallNumber, CallDate, Neighborhood, Address, Delay " +
                        "FROM FireCalls " +
                        "WHERE CallType = 'Structure Fire' " +
                        "LIMIT 10"
        ).show(10, false);

        // ============================================================
        // TASK 3 — How many calls came from each Battalion?
        // ============================================================
        /*
         * SOLUTION NOTES:
         *   GROUP BY on Battalion + COUNT(*) for the count.
         *   ORDER BY alias DESC puts highest-volume battalion first.
         *
         * EXPECTED OUTPUT: 10 rows, B02 or B03 typically at the top.
         *
         * SPARK UI: 2 Stages!
         *   Stage 1: FileScan -> partial HashAggregate -> Exchange (shuffle)
         *   Stage 2: final HashAggregate -> Sort
         *   This is the first time candidates see 2 stages.
         *   Point them to the Exchange node in the SQL tab.
         */

        System.out.println("\n--- TASK 3 SOLUTION: Total calls per Battalion ---");

        spark.sql(
                "SELECT Battalion, COUNT(*) AS TotalCalls " +
                        "FROM FireCalls " +
                        "GROUP BY Battalion " +
                        "ORDER BY TotalCalls DESC"
        ).show(false);
        /*
         * +---------+----------+
         * |Battalion| TotalCalls|
         * +---------+----------+
         * |      B02|     22857|
         * |      B03|     22192|
         * |      B01|     21443|
         * ...
         */

        // ============================================================
        // TASK 4 — Which neighborhood had the most calls?
        // ============================================================
        /*
         * SOLUTION NOTES:
         *   Same pattern as Task 3 but on Neighborhood.
         *   LIMIT 10 keeps output clean.
         *
         * EXPECTED OUTPUT: Tenderloin, South of Market, Mission near top.
         *
         * SPARK UI: 2 Stages again (GROUP BY always shuffles).
         *   Compare this stage's shuffle size vs Task 3 —
         *   more unique keys (41 neighborhoods vs 10 battalions)
         *   means more shuffle partitions used.
         */

        System.out.println("\n--- TASK 4 SOLUTION: Top 10 neighborhoods by call volume ---");

        spark.sql(
                "SELECT Neighborhood, COUNT(*) AS TotalCalls " +
                        "FROM FireCalls " +
                        "GROUP BY Neighborhood " +
                        "ORDER BY TotalCalls DESC " +
                        "LIMIT 10"
        ).show(false);
        /*
         * +-------------------+----------+
         * |       Neighborhood|TotalCalls|
         * +-------------------+----------+
         * |         Tenderloin|     16220|
         * |South of Market    |     13762|
         * |            Mission|     12872|
         * ...
         */

        // ============================================================
        // TASK 5 — Average response delay per CallType
        // ============================================================
        /*
         * SOLUTION NOTES:
         *   AVG() computes the mean. ROUND(..., 2) limits decimal places.
         *   Alias the result so the column name is readable.
         *   ORDER BY the alias DESC to see slowest types first.
         *
         * EXPECTED OUTPUT: Some niche call types have high delays
         *                  because they are rare and far from stations.
         *
         * SPARK UI: 2 Stages (GROUP BY + AVG is an aggregation).
         *   SQL tab: you'll see HashAggregate twice —
         *   once for the partial avg before the shuffle,
         *   once for the final avg after.
         */

        System.out.println("\n--- TASK 5 SOLUTION: Average delay per CallType ---");

        spark.sql(
                "SELECT CallType, ROUND(AVG(Delay), 2) AS AvgDelayMinutes " +
                        "FROM FireCalls " +
                        "GROUP BY CallType " +
                        "ORDER BY AvgDelayMinutes DESC " +
                        "LIMIT 10"
        ).show(false);
        /*
         * +--------------------+---------------+
         * |            CallType|AvgDelayMinutes|
         * +--------------------+---------------+
         * |        Odor (Gas...)|           6.8 |
         * |        Train / Rail |           6.1 |
         * |        Structure ...|           4.9 |
         * ...
         */

        // ============================================================
        // TASK 6 — Calls where Delay > 10 minutes
        // ============================================================
        /*
         * SOLUTION NOTES:
         *   Numeric comparison with > operator.
         *   ORDER BY Delay DESC puts the worst offenders at the top.
         *   The max delay in the dataset is 1844 minutes — 30+ hours!
         *   Worth highlighting to candidates as a data quality discussion.
         *
         * EXPECTED OUTPUT: ~18,000 rows match. LIMIT 15 shows the worst.
         *
         * SPARK UI: 1 Stage — filter + sort, no GROUP BY.
         *   But sort still creates 2 stages because ORDER BY needs
         *   a shuffle to guarantee global ordering.
         *   Interesting contrast with Task 3's 2 stages.
         */

        System.out.println("\n--- TASK 6 SOLUTION: Calls with Delay > 10 minutes ---");

        spark.sql(
                "SELECT CallType, Neighborhood, Battalion, Delay " +
                        "FROM FireCalls " +
                        "WHERE Delay > 10 " +
                        "ORDER BY Delay DESC " +
                        "LIMIT 15"
        ).show(15, false);
        /*
         * Max Delay will be ~1844.55 minutes.
         * Ask candidates: is that realistic? Data error or real event?
         */

        // ============================================================
        // TASK 7 — Medical Incidents from Tenderloin only
        // ============================================================
        /*
         * SOLUTION NOTES:
         *   AND combines two WHERE conditions.
         *   Both conditions must be true for the row to be included.
         *   String values always in single quotes in SQL.
         *
         * EXPECTED OUTPUT: ~8,000+ rows. LIMIT 10 shows a sample.
         *
         * SPARK UI: 1 Stage — two filters, no shuffle.
         *   SQL tab: look for two Filter nodes or one combined Filter.
         *   Catalyst may combine them into a single predicate.
         *   PushedFilters in the FileScan will show both conditions.
         */

        System.out.println("\n--- TASK 7 SOLUTION: Medical Incidents in Tenderloin ---");

        spark.sql(
                "SELECT CallDate, Address, CallFinalDisposition, Delay " +
                        "FROM FireCalls " +
                        "WHERE CallType = 'Medical Incident' " +
                        "AND Neighborhood = 'Tenderloin' " +
                        "LIMIT 10"
        ).show(10, false);

        // ============================================================
        // TASK 8 — Label calls as Fast / Normal / Slow
        // ============================================================
        /*
         * SOLUTION NOTES:
         *   CASE WHEN creates a new column based on conditions.
         *   Conditions evaluated top to bottom — first match wins.
         *   Order matters: check <= 3 before <= 10, otherwise
         *   all delays <= 3 would also match <= 10.
         *   AS gives the new column a name.
         *
         * EXPECTED OUTPUT: Mix of Fast, Normal and Slow labels.
         *
         * SPARK UI: 1 Stage — CASE WHEN is a projection, no shuffle.
         *   SQL tab: look for Project node containing the CASE expression.
         *   No Exchange node — this confirms no shuffle happened.
         */

        System.out.println("\n--- TASK 8 SOLUTION: Fast / Normal / Slow labels ---");

        spark.sql(
                "SELECT CallType, Neighborhood, Delay, " +
                        "CASE " +
                        "    WHEN Delay <= 3  THEN 'Fast' " +
                        "    WHEN Delay <= 10 THEN 'Normal' " +
                        "    ELSE 'Slow' " +
                        "END AS ResponseSpeed " +
                        "FROM FireCalls " +
                        "LIMIT 20"
        ).show(20, false);

        // ============================================================
        // TASK 9 — Structure Fire and Medical Incident per Battalion
        // ============================================================
        /*
         * SOLUTION NOTES:
         *   IN ('a', 'b') is cleaner than two OR conditions.
         *   GROUP BY on TWO columns creates one row per combination.
         *   ORDER BY Battalion first, then by count descending within
         *   each battalion, so you can easily compare.
         *
         * EXPECTED OUTPUT: ~20 rows (10 battalions x 2 call types).
         *
         * SPARK UI: 2 Stages (GROUP BY on 2 columns still shuffles).
         *   SQL tab: Exchange node groups by BOTH columns together.
         *   The shuffle key is (Battalion, CallType) as a pair.
         */

        System.out.println("\n--- TASK 9 SOLUTION: Structure Fire and Medical per Battalion ---");

        spark.sql(
                "SELECT Battalion, CallType, COUNT(*) AS TotalCalls " +
                        "FROM FireCalls " +
                        "WHERE CallType IN ('Structure Fire', 'Medical Incident') " +
                        "GROUP BY Battalion, CallType " +
                        "ORDER BY Battalion ASC, TotalCalls DESC"
        ).show(false);

        // ============================================================
        // TASK 10 BONUS — Worst delay Battalion for Structure Fires
        // ============================================================
        /*
         * SOLUTION NOTES:
         *   WHERE filters BEFORE the GROUP BY — only Structure Fires included.
         *   COUNT(*) shows volume so you can spot small-sample outliers.
         *   AVG(Delay) gives the average per battalion.
         *   Ordering by avg delay descending shows worst response first.
         *
         * EXPECTED OUTPUT: 10 rows. The battalion at the top has the
         *                  worst average response time to structure fires.
         *
         * SPARK UI: 2 Stages.
         *   This is a good moment to show that WHERE executes BEFORE
         *   GROUP BY in the physical plan — FileScan includes the
         *   Structure Fire filter (PushedFilters), so fewer rows
         *   enter the shuffle than the raw 175K total.
         *   That is Catalyst's predicate pushdown in action.
         */

        System.out.println("\n--- TASK 10 BONUS SOLUTION: Worst delay per Battalion for Structure Fires ---");

        spark.sql(
                "SELECT Battalion, " +
                        "       COUNT(*) AS TotalFires, " +
                        "       ROUND(AVG(Delay), 2) AS AvgDelayMinutes, " +
                        "       ROUND(MAX(Delay), 2) AS MaxDelayMinutes " +
                        "FROM FireCalls " +
                        "WHERE CallType = 'Structure Fire' " +
                        "GROUP BY Battalion " +
                        "ORDER BY AvgDelayMinutes DESC"
        ).show(false);
        /*
         * +---------+----------+---------------+---------------+
         * |Battalion|TotalFires|AvgDelayMinutes|MaxDelayMinutes|
         * +---------+----------+---------------+---------------+
         * |      B09|      1842|           5.23|         124.72|
         * |      B10|      2104|           4.98|         ...   |
         * ...
         * Teaching moment: B09 has highest avg delay but is it
         * because of geography? Station distance? Worth exploring.
         */

        // ============================================================
        // DISCUSSION QUESTIONS — for debrief with candidates
        // ============================================================

        System.out.println("\n");
        System.out.println("=".repeat(65));
        System.out.println("  DEBRIEF DISCUSSION QUESTIONS");
        System.out.println("=".repeat(65));
        System.out.println(
                "\n  1. Which tasks created 2 stages in Spark UI? Why?" +
                        "\n     ANSWER: Tasks 3, 4, 5, 9, 10 — all used GROUP BY." +
                        "\n             Task 6 also gets 2 stages because of ORDER BY." +
                        "\n             GROUP BY and ORDER BY both require a shuffle." +
                        "\n" +
                        "\n  2. Which tasks ran in just 1 stage?" +
                        "\n     ANSWER: Tasks 1, 2, 7, 8 — filter/select/CASE WHEN" +
                        "\n             only. No GROUP BY, no ORDER BY = no shuffle." +
                        "\n" +
                        "\n  3. In Task 6, the max delay was ~1844 minutes." +
                        "\n     What does that tell us about data quality?" +
                        "\n     ANSWER: Real data always has outliers and errors." +
                        "\n             In production you'd filter these out or" +
                        "\n             investigate before including in analytics." +
                        "\n" +
                        "\n  4. Look at Task 2 in the SQL tab — find PushedFilters." +
                        "\n     What does that mean?" +
                        "\n     ANSWER: Catalyst pushed the WHERE clause INTO the" +
                        "\n             file scan. Spark didn't read all 175K rows" +
                        "\n             and then filter — it filtered while reading." +
                        "\n             This is predicate pushdown — free optimization." +
                        "\n" +
                        "\n  5. Tasks 3 and 9 both use GROUP BY." +
                        "\n     Does Task 9 shuffle more data than Task 3? Why?" +
                        "\n     ANSWER: Task 9 shuffles LESS because WHERE IN filters" +
                        "\n             first — only 2 call types enter the shuffle," +
                        "\n             not all 175K rows. WHERE always runs before" +
                        "\n             GROUP BY in the physical plan."
        );

        System.out.println("\n>>> PAUSED — Press ENTER to exit <<<");
        try (final var scanner = new Scanner(System.in)) {
            scanner.nextLine();
        }
    }
}
