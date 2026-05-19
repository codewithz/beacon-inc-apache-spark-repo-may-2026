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
 *  HANDLING CORRUPT / MALFORMED DATA IN SPARK
 *  Beacon Solutions, Inc.
 * ============================================================
 *
 *  THE PROBLEM:
 *  ------------
 *  Real-world data is never perfectly clean.
 *  JSON files can contain records that are:
 *    - Malformed  (broken syntax, missing brackets)
 *    - Mismatched (a field has the wrong type — string where int expected)
 *    - Incomplete (missing required fields)
 *
 *  THE WRONG APPROACH:
 *  -------------------
 *  Crash the entire job when one bad record is found.
 *  In production, a 10-million row file with 3 bad records
 *  should not fail the entire pipeline.
 *
 *  THE RIGHT APPROACH:
 *  -------------------
 *  Use PERMISSIVE mode + columnNameOfCorruptRecord.
 *  Spark puts good records in normal columns.
 *  Spark puts bad records in a special _corrupt_record column.
 *  Then you split them: process good records, log bad ones.
 *
 *  READ MODES RECAP:
 *  ------------------
 *  PERMISSIVE   → keep all rows, bad values become null  (default)
 *  DROPMALFORMED → silently drop bad rows — dangerous, you lose data
 *  FAILFAST     → crash on first bad row — good for dev, bad for prod
 *
 *  FILE: records_corrupt_10k.json
 *  Contains a mix of valid and intentionally malformed JSON records.
 * ============================================================
 */
public class HandlingCorruptData {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Handling Corrupt Data - Beacon")
                .master("local[4]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");
        System.out.println("\n>>> Spark UI: http://localhost:4040 <<<\n");

        // ============================================================
        // STEP 1 — Define the schema explicitly
        // ============================================================
        /*
         * Always define schema when using PERMISSIVE mode.
         * Without a schema, Spark infers from the data — but malformed
         * records confuse inference and you get unpredictable results.
         *
         * Our JSON records look like:
         *
         * {
         *   "id": 1,
         *   "name": "Alice",
         *   "details": {
         *     "age": 30,
         *     "address": {
         *       "street": "123 Main St",
         *       "city": "New York"
         *     }
         *   }
         * }
         *
         * IMPORTANT: Add "_corrupt_record" as a StringType column.
         * Spark will use this column to store the raw text of any
         * record it could not parse. Valid records get null here.
         */

        printSection("STEP 1 — Define schema with _corrupt_record column");

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("id",   DataTypes.IntegerType, true),
                DataTypes.createStructField("name", DataTypes.StringType,  true),

                // Nested struct: details → age + address
                DataTypes.createStructField("details",
                        DataTypes.createStructType(new StructField[]{
                                DataTypes.createStructField("age", DataTypes.IntegerType, true),

                                // Nested struct inside details: address → street + city
                                DataTypes.createStructField("address",
                                        DataTypes.createStructType(new StructField[]{
                                                DataTypes.createStructField("street", DataTypes.StringType, true),
                                                DataTypes.createStructField("city",   DataTypes.StringType, true)
                                        }), true)
                        }), true),

                // This column captures the raw text of any bad record
                // Must match the name you pass to columnNameOfCorruptRecord below
                DataTypes.createStructField("_corrupt_record", DataTypes.StringType, true)
        });

        System.out.println("Schema defined including _corrupt_record column.");

        // ============================================================
        // STEP 2 — Read the file in PERMISSIVE mode
        // ============================================================
        /*
         * Three things working together:
         *
         * mode = PERMISSIVE
         *   → Don't crash on bad records.
         *   → Try to parse every row. If it fails, keep going.
         *
         * columnNameOfCorruptRecord = "_corrupt_record"
         *   → When a row can't be parsed, put its raw text here.
         *   → Valid rows get null in this column.
         *   → This is how we separate good from bad later.
         *
         * schema(schema)
         *   → Required. Without this, PERMISSIVE + corrupt column
         *     doesn't work reliably.
         */

        printSection("STEP 2 — Read with PERMISSIVE mode");

        String filePath = "C:\\Datasets\\records_corrupt_10k.json";

        Dataset<Row> rawDF = spark
                .read()
                .option("mode", "PERMISSIVE")
                .option("columnNameOfCorruptRecord", "_corrupt_record")
                .schema(schema)
                .json(filePath)
                        .cache();

        System.out.println("File loaded. Schema:");
        rawDF.printSchema();
        /*
         * root
         *  |-- id: integer (nullable = true)
         *  |-- name: string (nullable = true)
         *  |-- details: struct (nullable = true)
         *  |    |-- age: integer (nullable = true)
         *  |    |-- address: struct (nullable = true)
         *  |    |    |-- street: string (nullable = true)
         *  |    |    |-- city: string (nullable = true)
         *  |-- _corrupt_record: string (nullable = true)
         *
         * _corrupt_record appears as a normal column.
         * It will be null for good rows, non-null for bad rows.
         */

        System.out.println("\nTotal rows loaded (good + bad): " + rawDF.count());

        // ============================================================
        // STEP 3 — Split into valid and malformed records
        // ============================================================
        /*
         * The split is simple:
         *
         * VALID records   → _corrupt_record IS NULL
         *   These parsed correctly. All fields have proper values.
         *
         * MALFORMED records → _corrupt_record IS NOT NULL
         *   These failed to parse. The raw broken text is in _corrupt_record.
         *   All other columns will be null for these rows.
         *
         * TRANSFORMATION — lazy, no job fires yet.
         */

        printSection("STEP 3 — Split good records from bad records");

        // Valid records — _corrupt_record is null
        Dataset<Row> validDF = rawDF
                .filter(col("_corrupt_record").isNull())
                .drop("_corrupt_record");   // clean up — no need for this column anymore
        /*
         * .drop("_corrupt_record") removes the column from valid records.
         * No point keeping a null column in your clean dataset.
         */

        // Malformed records — _corrupt_record has content
        Dataset<Row> malformedDF = rawDF
                .filter(col("_corrupt_record").isNotNull())
                .select(col("_corrupt_record"));   // only the raw broken text matters
        /*
         * For bad records, all parsed columns are null anyway.
         * We only care about the raw text so we can log or fix it.
         */

        System.out.println("Split defined. Actions below will trigger execution.");

        // ============================================================
        // STEP 4 — Inspect both sets
        // ============================================================

        printSection("STEP 4 — Valid records");

        long validCount = validDF.count();
        System.out.println("Valid record count: " + validCount);
        validDF.show(10, false);
        /*
         * +---+-----+---------------------------+
         * | id| name|                    details|
         * +---+-----+---------------------------+
         * |  1|Alice|{30, {123 Main St, New York}}|
         * |  2|  Bob|{25, {456 Oak Ave, Boston}} |
         * ...
         *
         * Clean data — all fields parsed correctly.
         * _corrupt_record column is gone (we dropped it).
         */

        printSection("STEP 4 — Malformed records");

        long malformedCount = malformedDF.count();
        System.out.println("Malformed record count: " + malformedCount);
        malformedDF.show(false);
        /*
         * +--------------------------------------------+
         * |                          _corrupt_record   |
         * +--------------------------------------------+
         * |{"id": "not_an_int", "name": "BadRecord"}   |
         * |{broken json without closing brace           |
         * |{"id": 999, "name": null, "details": "wrong"}|
         * +--------------------------------------------+
         *
         * Raw text of every record Spark couldn't parse.
         * You can now:
         *   - Write these to a separate error log file
         *   - Alert your data team
         *   - Attempt manual correction and reprocess
         */

        // ============================================================
        // STEP 5 — Work with valid records normally
        // ============================================================
        /*
         * Once separated, the valid DataFrame behaves exactly
         * like any other DataFrame. No special handling needed.
         */

        printSection("STEP 5 — Process valid records");

        // Access nested fields using dot notation
        System.out.println("--- Valid records: id, name, age, city ---");
        validDF.select(
                col("id"),
                col("name"),
                col("details.age").alias("age"),
                col("details.address.city").alias("city")
        ).show(10, false);

        // Basic aggregation on valid data
        System.out.println("--- Count of valid records per city ---");
        validDF.select(
                        col("details.address.city").alias("city")
                )
                .groupBy("city")
                .count()
                .orderBy(col("count").desc())
                .show(false);

        // ============================================================
        // STEP 6 — Write bad records to a log file
        // ============================================================
        /*
         * In production you would write the malformed records to
         * a separate location so the data team can investigate.
         *
         * Common pattern:
         *   - Good records  → write to Parquet / Delta table
         *   - Bad records   → write to a quarantine CSV or JSON log
         */

        printSection("STEP 6 — Write malformed records to error log");

        String errorLogPath = "C:\\Spark\\output\\corrupt_records_log";

        malformedDF
                .write()
                .mode("overwrite")
                .text(errorLogPath);

        System.out.println("Malformed records written to: " + errorLogPath);
        System.out.println("Data team can now review and fix these records.");

        // ============================================================
        // SUMMARY
        // ============================================================

        printSection("SUMMARY");
        System.out.println(
                "  READ MODES:\n" +
                        "    PERMISSIVE    → keep all rows, bad values = null  (use this)\n" +
                        "    DROPMALFORMED → silently drop bad rows            (dangerous)\n" +
                        "    FAILFAST      → crash on first bad row            (dev only)\n" +
                        "\n" +
                        "  THE PATTERN:\n" +
                        "    1. Add _corrupt_record: StringType to your schema\n" +
                        "    2. Set mode = PERMISSIVE\n" +
                        "    3. Set columnNameOfCorruptRecord = '_corrupt_record'\n" +
                        "    4. Filter isNull()    → valid records\n" +
                        "    5. Filter isNotNull() → malformed records\n" +
                        "    6. Process valid, log malformed\n" +
                        "\n" +
                        "  Valid: " + validCount + " records processed cleanly.\n" +
                        "  Bad:   " + malformedCount + " records captured for review."
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