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
 *  MULTI-DATASET OPERATIONS — JOINS AND SET OPERATIONS
 *  Beacon Solutions, Inc.
 * ============================================================
 *
 *  WHAT WE COVER:
 *  --------------
 *  Part 1 — JOINS: Combining two DataFrames on a common key
 *    - DataFrame API join
 *    - SQL JOIN (INNER, LEFT, RIGHT, FULL)
 *    - Real question: find zones with no pickups (LEFT JOIN trick)
 *
 *  Part 2 — SET OPERATIONS: Combining results of two queries
 *    - UNION     → all rows from both, removes duplicates
 *    - UNION ALL → all rows from both, keeps duplicates
 *    - INTERSECT → only rows that appear in BOTH
 *    - EXCEPT    → rows in first query that are NOT in second
 *
 *  DATASETS USED:
 *  --------------
 *  YellowTaxis_202210.csv  → 3.6M taxi trips (PULocationID, DOLocationID...)
 *  TaxiZones.csv           → 265 zone definitions (LocationID, Borough, Zone)
 *  Cabs.csv                → Licensed cab registrations (Name, LicenseType...)
 *  Drivers.csv             → Registered drivers (Name...)
 *
 *  JOIN KEY:
 *  YellowTaxis.PULocationID = TaxiZones.LocationID
 * ============================================================
 */
public class MultiDatasetOperations {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Multi-Dataset Operations - Beacon")
                .master("local[4]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");
        System.out.println("\n>>> Spark UI: http://localhost:4040 <<<\n");

        // ============================================================
        // SETUP — Load both datasets
        // ============================================================

        printSection("SETUP — Loading YellowTaxis and TaxiZones");

        // ── Yellow Taxi (large dataset — 3.6 million rows) ──────────
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

        // ── Taxi Zones (small lookup table — 265 rows) ───────────────
        /*
         * Notice the ALTERNATIVE way to define a schema using .add() chaining.
         * new StructType().add("col", type, nullable)
         * This is cleaner for short schemas — identical result to createStructType.
         */
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

        System.out.println("YellowTaxi rows: " + yellowTaxiDF.count());
        System.out.println("TaxiZones rows:  " + taxiZonesDF.count());

        // ============================================================
        // PART 1A — JOIN using the DataFrame API
        // ============================================================
        /*
         * .join(otherDF, joinCondition, joinType)
         *
         * joinCondition: how the two tables relate
         *   yellowTaxiDF.col("PULocationID").equalTo(taxiZonesDF.col("LocationID"))
         *
         * joinType:
         *   "inner"      → only rows with a match in BOTH tables
         *   "left"       → all rows from left, nulls where right has no match
         *   "right"      → all rows from right, nulls where left has no match
         *   "outer"      → all rows from both, nulls on whichever side has no match
         *   "left_semi"  → left rows that HAVE a match (no right columns returned)
         *   "left_anti"  → left rows that DO NOT have a match
         *
         * SPARK UI — SQL tab after this:
         *   TaxiZones is 265 rows → Spark will BROADCAST it automatically.
         *   Look for BroadcastHashJoin in the physical plan — no shuffle!
         *   This is AQE + autoBroadcastJoinThreshold working automatically.
         */

        printSection("PART 1A — JOIN using DataFrame API");

        Dataset<Row> joinedDF = yellowTaxiDF.join(
                taxiZonesDF,
                yellowTaxiDF.col("PULocationID").equalTo(taxiZonesDF.col("LocationID")),
                "inner"
        );

        joinedDF.printSchema();
        /*
         * Schema now contains columns from BOTH DataFrames combined.
         * Notice both have a LocationID column — this can cause ambiguity.
         * Always alias or drop duplicate columns after a join.
         */

        System.out.println("Joined row count: " + joinedDF.count());

        // Select specific columns to avoid ambiguity
        System.out.println("\n--- Joined result: trip + zone info ---");
        joinedDF.select(
                yellowTaxiDF.col("PULocationID"),
                col("Borough"),
                col("Zone"),
                col("total_amount"),
                col("trip_distance")
        ).show(10, false);

        // ============================================================
        // PART 1B — JOIN using SQL
        // ============================================================
        /*
         * Register both DataFrames as views first.
         * Then write a standard SQL JOIN.
         *
         * SQL joins are often more readable for complex conditions.
         * The physical plan produced is identical to the DataFrame join.
         */

        printSection("PART 1B — JOIN using SQL");

        yellowTaxiDF.createOrReplaceTempView("YellowTaxis");
        taxiZonesDF.createOrReplaceTempView("TaxiZones");

        System.out.println("--- SQL INNER JOIN: taxi trips with zone names ---");
        spark.sql(
                "SELECT yt.PULocationID, tz.Borough, tz.Zone, " +
                        "yt.total_amount, yt.trip_distance " +
                        "FROM YellowTaxis yt " +
                        "INNER JOIN TaxiZones tz ON yt.PULocationID = tz.LocationID " +
                        "LIMIT 10"
        ).show(false);

        // ============================================================
        // PART 1C — LEFT JOIN to find zones with NO pickups
        // ============================================================
        /*
         * QUESTION: Which zones in TaxiZones had NO taxi pickups?
         *
         * TECHNIQUE: LEFT JOIN + WHERE right side IS NULL
         *
         * This is a classic SQL pattern called "anti-join using LEFT JOIN":
         *   1. LEFT JOIN keeps ALL TaxiZones rows
         *   2. For zones with no matching pickup, YellowTaxis columns = null
         *   3. WHERE yt.PULocationID IS NULL → keeps only the unmatched zones
         *
         * TaxiZones is the LEFT table (we want all its rows).
         * YellowTaxis is the RIGHT table (we check if a match exists).
         *
         * SPARK UI:
         *   Look at this job in the SQL tab.
         *   Catalyst may convert this LEFT JOIN + IS NULL into a
         *   more efficient anti-join internally.
         */

        printSection("PART 1C — LEFT JOIN: Zones with zero pickups");

        Dataset<Row> noPickupZones = spark.sql(
                "SELECT DISTINCT tz.* " +
                        "FROM TaxiZones tz " +
                        "LEFT JOIN YellowTaxis yt ON yt.PULocationID = tz.LocationID " +
                        "WHERE yt.PULocationID IS NULL"
        );

        System.out.println("Zones with NO pickups in October 2022:");
        noPickupZones.show(false);
        System.out.println("Count: " + noPickupZones.count());

        // ============================================================
        // PART 2 — SET OPERATIONS: UNION, INTERSECT, EXCEPT
        // ============================================================
        /*
         * SET OPERATIONS work on two queries that return the SAME columns.
         * Think of them as combining result sets, not joining row by row.
         *
         * UNION     → rows from Query A + rows from Query B, no duplicates
         * UNION ALL → rows from Query A + rows from Query B, WITH duplicates
         * INTERSECT → only rows that appear in BOTH Query A and Query B
         * EXCEPT    → rows in Query A that do NOT appear in Query B
         *
         * DATASETS:
         *   Cabs.csv    → licensed cab registrations with owner names
         *   Drivers.csv → registered active drivers
         *
         * BUSINESS QUESTION:
         *   Find cab owners (from Cabs with LicenseType = 'OWNER MUST DRIVE')
         *   who are NOT registered in the Drivers table.
         *   These are people who should be driving their own cab
         *   but haven't registered as a driver — a compliance issue.
         */

        printSection("PART 2 — SET OPERATIONS: Cabs and Drivers datasets");

        Dataset<Row> cabsDF = spark
                .read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("C:\\Spark\\DataFiles\\Cabs.csv");

        Dataset<Row> driversDF = spark
                .read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("C:\\Spark\\DataFiles\\Drivers.csv");

        cabsDF.createOrReplaceTempView("Cabs");
        driversDF.createOrReplaceTempView("Drivers");

        System.out.println("--- Cabs sample ---");
        cabsDF.show(5, false);

        System.out.println("--- Drivers sample ---");
        driversDF.show(5, false);

        // ── UNION — combine names from both tables, no duplicates ────
        /*
         * Both queries must return the same number of columns
         * with compatible types. Here both return a single Name column.
         *
         * UNION removes duplicate names that appear in both tables.
         * Use UNION ALL if you want to keep all rows including duplicates.
         */

        printSection("UNION — All names from Cabs and Drivers (no duplicates)");

        spark.sql(
                "SELECT Name FROM Cabs " +
                        "UNION " +
                        "SELECT Name FROM Drivers " +
                        "ORDER BY Name"
        ).show(10, false);

        // ── UNION ALL — keeps duplicates ─────────────────────────────

        printSection("UNION ALL — All names including duplicates");

        spark.sql(
                "SELECT Name, 'Cabs' AS Source FROM Cabs " +
                        "UNION ALL " +
                        "SELECT Name, 'Drivers' AS Source FROM Drivers " +
                        "ORDER BY Name"
        ).show(10, false);
        /*
         * If the same person appears in both tables, you'll see two rows.
         * The Source column shows where each row came from.
         */

        // ── INTERSECT — names that appear in BOTH tables ─────────────
        /*
         * Only returns names that exist in BOTH Cabs AND Drivers.
         * These are cab owners who are also registered as drivers — compliant.
         */

        printSection("INTERSECT — Names in BOTH Cabs and Drivers");

        Dataset<Row> bothRegistered = spark.sql(
                "SELECT Name FROM Cabs " +
                        "INTERSECT " +
                        "SELECT Name FROM Drivers"
        );
        bothRegistered.show(10, false);
        System.out.println("People in both: " + bothRegistered.count());

        // ── EXCEPT — the compliance check ────────────────────────────
        /*
         * EXCEPT returns rows from the FIRST query that do NOT appear
         * in the SECOND query.
         *
         * Query 1: Names from Cabs where LicenseType = 'OWNER MUST DRIVE'
         * Query 2: Names from Drivers
         *
         * Result: Cab owners who MUST drive their own cab
         *         but are NOT registered as a driver.
         *         These are compliance violations.
         */

        printSection("EXCEPT — Unregistered cab owners (compliance check)");

        Dataset<Row> unregisteredDrivers = spark.sql(
                "SELECT Name FROM Cabs WHERE LicenseType = 'OWNER MUST DRIVE' " +
                        "EXCEPT " +
                        "SELECT Name FROM Drivers"
        );

        unregisteredDrivers.show(false);
        System.out.println("Total unregistered cab owners: " + unregisteredDrivers.count());

        // ============================================================
        // SUMMARY
        // ============================================================

        printSection("SUMMARY");
        System.out.println(
                "  JOINS — combine columns from two tables:\n" +
                        "    INNER JOIN  → only matching rows from both sides\n" +
                        "    LEFT JOIN   → all from left, nulls where right has no match\n" +
                        "    RIGHT JOIN  → all from right, nulls where left has no match\n" +
                        "    FULL JOIN   → all rows from both, nulls on non-matching side\n" +
                        "\n" +
                        "  LEFT JOIN + IS NULL trick:\n" +
                        "    Find rows in table A that have NO match in table B\n" +
                        "    LEFT JOIN B ON key, WHERE B.key IS NULL\n" +
                        "\n" +
                        "  SET OPERATIONS — combine rows from two queries:\n" +
                        "    UNION     → combine, remove duplicates\n" +
                        "    UNION ALL → combine, keep duplicates\n" +
                        "    INTERSECT → only rows in BOTH queries\n" +
                        "    EXCEPT    → rows in first query NOT in second\n" +
                        "\n" +
                        "  SPARK UI — SQL tab:\n" +
                        "    INNER JOIN on small table → BroadcastHashJoin (no shuffle!)\n" +
                        "    INNER JOIN on large table → SortMergeJoin (shuffle both sides)\n" +
                        "    LEFT JOIN + IS NULL       → Catalyst may optimize to anti-join\n" +
                        "    SET operations            → always involve a shuffle (Exchange node)"
        );

        System.out.println("\n>>> PAUSED — Check http://localhost:4040 — Press ENTER to exit <<<");
        try (final var scanner = new Scanner(System.in)) {
            scanner.nextLine();
        }
    }

    private static void printSection(String title) {
        System.out.println("\n");
        System.out.println("=".repeat(60));
        System.out.println("  " + title);
        System.out.println("=".repeat(60));
    }
}