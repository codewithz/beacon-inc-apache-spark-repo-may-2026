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
 *  PRACTICE LAB — SF Fire Department Calls
 *  Beacon Solutions, Inc. | Code With Z
 * ============================================================
 *
 *  DATASET: San Francisco Fire Department Service Calls
 *  FILE:    sf-fire-calls.csv
 *  ROWS:    175,296 real emergency calls
 *
 *  COLUMNS AVAILABLE:
 *  -----------------------------------------------------------
 *  CallNumber           Integer  Unique ID for the call
 *  CallType             String   Type: 'Medical Incident', 'Structure Fire', 'Alarms'...
 *  CallDate             String   Date call was made (MM/dd/yyyy)
 *  CallFinalDisposition String   How the call ended: 'Fire', 'Code 2 Transport'...
 *  Address              String   Street address of the incident
 *  Neighborhood         String   SF neighborhood name (41 unique values)
 *  Battalion            String   Fire battalion: B01 through B10
 *  UnitType             String   TRUCK, MEDIC, ENGINE, CHIEF...
 *  NumAlarms            Integer  Number of alarms raised (1-5)
 *  Delay                Double   Response delay in minutes
 *  ALSUnit              Boolean  Was this an Advanced Life Support unit?
 *  Priority             Integer  Call priority level
 *  City                 String   City name
 *  Zipcode              Integer  Zip code
 *  -----------------------------------------------------------
 *
 *  YOUR TASKS: 10 SQL questions to answer using spark.sql()
 *
 *  HINTS FOR EACH TASK ARE IN THE COMMENTS.
 *  Write your SQL inside spark.sql("...").show();
 *
 *  Spark UI: http://localhost:4040
 * ============================================================
 */
public class Task1 {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("SF Fire Calls SQL Lab - Beacon")
                .master("local[4]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        System.out.println("\n>>> Spark UI: http://localhost:4040 <<<\n");

        // ============================================================
        // SETUP — File is loaded and registered as a SQL view for you
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

        // Register as SQL view — all your spark.sql() queries run against this
        fireDF.createOrReplaceTempView("FireCalls");

        System.out.println("FireCalls view registered. Total rows: " + fireDF.count());
        System.out.println("Schema preview:");
        fireDF.printSchema();

        // ============================================================
        // TASK 1 — How many distinct call types are there?
        // ============================================================
        /*
         * HINT: Use SELECT DISTINCT on the CallType column.
         *       Wrap it in a COUNT to get the total number.
         *
         * EXPECTED: You should see a single number (around 30 types)
         *
         * SQL PATTERN:
         *   SELECT COUNT(DISTINCT column_name) AS alias FROM table
         */

        System.out.println("\n--- TASK 1: How many distinct call types are there? ---");

        // Write your SQL here:
        spark.sql(
                "-- YOUR SQL HERE"
        ).show();

        // ============================================================
        // TASK 2 — Show all calls where CallType is 'Structure Fire'
        // ============================================================
        /*
         * HINT: Use WHERE to filter on CallType.
         *       Select only: CallNumber, CallDate, Neighborhood, Address, Delay
         *       Limit to 10 rows so the output is readable.
         *
         * SQL PATTERN:
         *   SELECT col1, col2 FROM table WHERE column = 'value' LIMIT n
         */

        System.out.println("\n--- TASK 2: Show Structure Fire calls (10 rows) ---");

        // Write your SQL here:
        spark.sql(
                "-- YOUR SQL HERE"
        ).show(10, false);

        // ============================================================
        // TASK 3 — How many calls came from each Battalion?
        // ============================================================
        /*
         * HINT: Use GROUP BY on the Battalion column.
         *       Count all rows with COUNT(*).
         *       Order by the count, highest first.
         *
         * EXPECTED: 10 battalions (B01 through B10), each with a count.
         *
         * SQL PATTERN:
         *   SELECT column, COUNT(*) AS alias
         *   FROM table
         *   GROUP BY column
         *   ORDER BY alias DESC
         */

        System.out.println("\n--- TASK 3: Total calls per Battalion ---");

        // Write your SQL here:
        spark.sql(
                "-- YOUR SQL HERE"
        ).show(false);

        // ============================================================
        // TASK 4 — Which neighborhood had the most calls?
        // ============================================================
        /*
         * HINT: Similar to Task 3 but group by Neighborhood.
         *       Show top 10 only using LIMIT.
         *
         * EXPECTED: Neighborhoods like Tenderloin, Mission, South of Market
         *           will be near the top.
         *
         * SQL PATTERN:
         *   SELECT column, COUNT(*) AS alias
         *   FROM table
         *   GROUP BY column
         *   ORDER BY alias DESC
         *   LIMIT 10
         */

        System.out.println("\n--- TASK 4: Top 10 neighborhoods by call volume ---");

        // Write your SQL here:
        spark.sql(
                "-- YOUR SQL HERE"
        ).show(false);

        // ============================================================
        // TASK 5 — What is the average response delay per CallType?
        // ============================================================
        /*
         * HINT: Use AVG() on the Delay column.
         *       Use ROUND(AVG(Delay), 2) to round to 2 decimal places.
         *       Group by CallType, order by average delay descending.
         *       Show top 10.
         *
         * EXPECTED: Some call types will have higher average delays than others.
         *
         * SQL PATTERN:
         *   SELECT column, ROUND(AVG(numeric_column), 2) AS alias
         *   FROM table
         *   GROUP BY column
         *   ORDER BY alias DESC
         *   LIMIT 10
         */

        System.out.println("\n--- TASK 5: Average response delay per CallType (top 10) ---");

        // Write your SQL here:
        spark.sql(
                "-- YOUR SQL HERE"
        ).show(false);

        // ============================================================
        // TASK 6 — Find all calls where Delay was more than 10 minutes
        // ============================================================
        /*
         * HINT: Use WHERE with a greater than condition on Delay.
         *       Select: CallType, Neighborhood, Battalion, Delay
         *       Order by Delay descending so worst cases appear first.
         *       Limit to 15 rows.
         *
         * SQL PATTERN:
         *   SELECT col1, col2 FROM table
         *   WHERE numeric_column > value
         *   ORDER BY numeric_column DESC
         *   LIMIT n
         */

        System.out.println("\n--- TASK 6: Calls with Delay > 10 minutes (worst first) ---");

        // Write your SQL here:
        spark.sql(
                "-- YOUR SQL HERE"
        ).show(15, false);

        // ============================================================
        // TASK 7 — Show only Medical Incident calls from the Tenderloin
        // ============================================================
        /*
         * HINT: Filter on TWO conditions using AND.
         *       CallType must be 'Medical Incident'
         *       AND Neighborhood must be 'Tenderloin'
         *       Select: CallDate, Address, CallFinalDisposition, Delay
         *       Limit to 10 rows.
         *
         * SQL PATTERN:
         *   SELECT col1, col2 FROM table
         *   WHERE column1 = 'value1' AND column2 = 'value2'
         *   LIMIT n
         */

        System.out.println("\n--- TASK 7: Medical Incidents in the Tenderloin ---");

        // Write your SQL here:
        spark.sql(
                "-- YOUR SQL HERE"
        ).show(10, false);

        // ============================================================
        // TASK 8 — Label each call as 'Fast', 'Normal' or 'Slow'
        // ============================================================
        /*
         * HINT: Use CASE WHEN to create a new column called ResponseSpeed.
         *         WHEN Delay <= 3  THEN 'Fast'
         *         WHEN Delay <= 10 THEN 'Normal'
         *         ELSE 'Slow'
         *       Select: CallType, Neighborhood, Delay, ResponseSpeed
         *       Limit to 20 rows.
         *
         * SQL PATTERN:
         *   SELECT col1,
         *     CASE
         *       WHEN condition THEN 'value'
         *       WHEN condition THEN 'value'
         *       ELSE 'value'
         *     END AS new_column_name
         *   FROM table
         *   LIMIT n
         */

        System.out.println("\n--- TASK 8: Label calls as Fast / Normal / Slow ---");

        // Write your SQL here:
        spark.sql(
                "-- YOUR SQL HERE"
        ).show(20, false);

        // ============================================================
        // TASK 9 — How many calls of each type came from each Battalion?
        //           Show only Structure Fire and Medical Incident types.
        // ============================================================
        /*
         * HINT: Filter on CallType using IN ('value1', 'value2').
         *       Then GROUP BY two columns: Battalion AND CallType.
         *       Count rows and order by Battalion, then by count descending.
         *
         * SQL PATTERN:
         *   SELECT col1, col2, COUNT(*) AS alias
         *   FROM table
         *   WHERE column IN ('value1', 'value2')
         *   GROUP BY col1, col2
         *   ORDER BY col1, alias DESC
         */

        System.out.println("\n--- TASK 9: Structure Fire and Medical Incident count per Battalion ---");

        // Write your SQL here:
        spark.sql(
                "-- YOUR SQL HERE"
        ).show(false);

        // ============================================================
        // TASK 10 — BONUS: Which Battalion has the worst average delay
        //            for Structure Fire calls specifically?
        // ============================================================
        /*
         * HINT: Combine WHERE to filter for 'Structure Fire' only,
         *       then GROUP BY Battalion,
         *       then use ROUND(AVG(Delay), 2) and ORDER BY that.
         *       Also show COUNT(*) so you know how many fires each had.
         *
         * EXPECTED: You'll see which battalion has the slowest response
         *           to structure fires — this is a real operations insight.
         *
         * SQL PATTERN:
         *   SELECT column,
         *          COUNT(*) AS alias1,
         *          ROUND(AVG(numeric_column), 2) AS alias2
         *   FROM table
         *   WHERE column = 'value'
         *   GROUP BY column
         *   ORDER BY alias2 DESC
         */

        System.out.println("\n--- TASK 10 BONUS: Worst average delay per Battalion for Structure Fires ---");

        // Write your SQL here:
        spark.sql(
                "-- YOUR SQL HERE"
        ).show(false);

        // ============================================================
        // SPARK UI HOLDER — Program stays alive so you can explore
        // ============================================================
        /*
         * While the program is paused here, open http://localhost:4040
         *
         * WHAT TO EXPLORE:
         *
         *   SQL / DataFrame Tab:
         *     - Every spark.sql() you ran appears here as a query
         *     - Click any query to see its physical execution plan
         *     - Read the plan BOTTOM UP — that's the execution order
         *     - Look for:
         *         FileScan   → reading the CSV file
         *         Filter     → your WHERE clause
         *         Project    → your SELECT columns
         *         Exchange   → a shuffle happened (from GROUP BY)
         *         HashAggregate → your COUNT / AVG
         *         Sort       → your ORDER BY
         *
         *   Stages Tab:
         *     - Tasks without GROUP BY: 1 Stage
         *     - Tasks WITH GROUP BY:    2 Stages (spot the Exchange!)
         *     - Click into a stage to see individual task durations
         *
         *   Jobs Tab:
         *     - Each .show() or .count() = one Job
         *     - Notice how many jobs your 10 tasks created
         */

        System.out.println("\n");
        System.out.println("=".repeat(60));
        System.out.println("  LAB COMPLETE — Explore Spark UI before exiting");
        System.out.println("=".repeat(60));
        System.out.println("  Open: http://localhost:4040");
        System.out.println("  Go to: SQL / DataFrame tab");
        System.out.println("  Click each query and read the physical plan.");
        System.out.println("  Which of your 10 tasks created 2 stages? Why?");
        System.out.println("=".repeat(60));
        System.out.println("\nPress ENTER to exit...");

        try (final var scanner = new Scanner(System.in)) {
            scanner.nextLine();
        }
    }
}