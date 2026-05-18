package com.beacon;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

/**
 * ============================================================
 *  TRANSFORMATIONS vs ACTIONS — Using TaxiZones Dataset

 * ============================================================
 *
 *  TaxiZones.csv structure (4 columns):
 *  LocationID | Borough   | Zone                    | ServiceZone
 *  -----------|-----------|-------------------------|------------
 *  1          | EWR       | Newark Airport          | EWR
 *  2          | Queens    | Jamaica Bay             | Boro Zone
 *  4          | Manhattan | Alphabet City           | Yellow Zone
 *  7          | Queens    | Astoria                 | Boro Zone
 *  ...265 rows total
 *
 *  TRANSFORMATIONS WE WILL USE:
 *    map()     → transform every row
 *    filter()  → keep rows matching a condition
 *
 *  ACTIONS WE WILL USE:
 *    count()   → how many rows
 *    take(n)   → first n rows
 *    collect() → all rows
 *    first()   → very first row
 *
 *  Open http://localhost:4040 and watch the Jobs tab.
 *  Every action you see print to console = one new Job in Spark UI.
 * ============================================================
 */
public class TransformationVsAction {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Transformations vs Actions - Beacon")
                .master("local[4]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        // ============================================================
        // STEP 1 — Read the file
        // ============================================================
        // textFile() is a TRANSFORMATION — lazy, nothing runs yet
        // Each line of the CSV becomes one String element in the RDD

        String filePath="C:\\Datasets\\TaxiZones.csv";

        JavaRDD<String> rawRDD = sc.textFile(filePath);
//        rawRDD.collect();
        printSection("STEP 1 — textFile() called — TRANSFORMATION (lazy)");
        System.out.println("rawRDD is defined. File has NOT been read yet.");
        System.out.println("Check Spark UI → Jobs tab: ZERO jobs so far.");

        // ============================================================
        // STEP 2 — First ACTION: count()
        // ============================================================
        // count() is an ACTION — this is the moment Spark actually reads the file
        // Spark creates JOB 0 right here

        printSection("STEP 2 — count() — ACTION — JOB 0 fires NOW");
        long totalRows = rawRDD.count();
        System.out.println("Total rows in TaxiZones.csv: " + totalRows);
        // Output: 266 (265 zones + 1 header line)

        System.out.println("\n→ Spark UI now shows Job 0 completed.");
        System.out.println("  1 Stage, 4 Tasks (one per partition).");

        // ============================================================
        // STEP 3 — ACTION: first()
        // ============================================================
        // Look at what the raw data actually looks like
        // first() is an ACTION — JOB 1

        printSection("STEP 3 — first() — ACTION — JOB 1 fires");
        String headerLine = rawRDD.first();
        System.out.println("First line (header): " + headerLine);
        // Output: LocationID,Borough,Zone,service_zone

        System.out.println("\n→ Spark UI: Job 1 completed. 2 jobs total now.");

        // ============================================================
        // STEP 4 — TRANSFORMATION: map() — split each line into columns
        // ============================================================
        // map() takes one element and returns one element
        // Here: one String → one String array (the columns)
        // LAZY — no execution, just adds to the DAG

        JavaRDD<String[]> colsRDD = rawRDD.map(
                line -> line.split(",")
                // "4,Manhattan,Alphabet City,Yellow Zone"
                //  → ["4", "Manhattan", "Alphabet City", "Yellow Zone"]
                //       [0]       [1]                    [2]                               [3]
        );
//        colsRDD.collect();
        printSection("STEP 4 — map(split) — TRANSFORMATION (lazy)");
        System.out.println("colsRDD defined. Each row is now a String[] of 4 columns.");
        System.out.println("Still no new job in Spark UI — map() is lazy.");

        // ============================================================
        // STEP 5 — ACTION: take(5) — see what our map() produced
        // ============================================================
        // take(5) is an ACTION — JOB 2
        // Triggers both: textFile + map(split)

        printSection("STEP 5 — take(5) — ACTION — JOB 2 fires");
        System.out.println("First 5 rows after splitting into columns:");
        System.out.println();

        List<String[]> first5 = colsRDD.take(5);
        for (String[] row : first5) {
            System.out.printf("  LocationID=%-4s  Borough=%-15s  Zone=%-30s  ServiceZone=%s%n",
                    row[0], row[1], row[2], row[3]);
        }

        System.out.println("\n→ Spark UI: Job 2 completed.");
        System.out.println("  Spark ran textFile() + map() together in ONE stage.");
        System.out.println("  No shuffle between them — they pipeline as one pass.");
//
        // ============================================================
        // STEP 6 — Remove the header row
        // ============================================================
        // The CSV has a header line: "LocationID,Borough,Zone,service_zone"
        // We need to filter it out before doing real analysis
        // filter() is a TRANSFORMATION — lazy

        JavaRDD<String[]> dataRDD = colsRDD.filter(
                row -> !row[0].equals("LocationID")   // skip the header
        );
//        dataRDD.collect();
        printSection("STEP 6 — filter(skip header) — TRANSFORMATION (lazy)");
        System.out.println("dataRDD removes the header line. No job yet.");

        // ============================================================
        // STEP 7 — TRANSFORMATION: filter() — Manhattan only
        // ============================================================
        // Keep rows where column index 1 (Borough) equals "Manhattan"
        // LAZY

        JavaRDD<String[]> manhattanRDD = dataRDD.filter(
                row -> row[1].equals("Manhattan")
        );
//        manhattanRDD.collect();
        printSection("STEP 7 — filter(Manhattan) — TRANSFORMATION (lazy)");
        System.out.println("manhattanRDD keeps only Borough = Manhattan.");
        System.out.println("DAG so far:");
        System.out.println("  textFile → map(split) → filter(skip header) → filter(Manhattan)");
        System.out.println("No job yet.");
//
        // ============================================================
        // STEP 8 — ACTION: count() on Manhattan
        // ============================================================
        // JOB 3 — runs the full chain above

        printSection("STEP 8 — count() on Manhattan — ACTION — JOB 3 fires");
        long manhattanCount = manhattanRDD.count();
        System.out.println("Total Manhattan zones: " + manhattanCount);
        // Output: 69

        System.out.println("\n→ Spark UI: Job 3 ran the FULL pipeline:");
        System.out.println("  textFile → map(split) → filter(header) → filter(Manhattan) → count");
        System.out.println("  All in ONE Stage (no shuffle needed for these operations).");

        // ============================================================
        // STEP 9 — TRANSFORMATION: filter() — Yellow Zone only
        // ============================================================
        // Chain another filter on top of manhattanRDD
        // Keep rows where column index 3 (ServiceZone) equals "Yellow Zone"
        // LAZY

        JavaRDD<String[]> yellowZoneRDD = manhattanRDD.filter(
                row -> row[3].equals("Yellow Zone")
        );
//    yellowZoneRDD.collect();
        printSection("STEP 9 — filter(Yellow Zone) — TRANSFORMATION (lazy)");
        System.out.println("yellowZoneRDD = Manhattan zones that are also Yellow Zones.");

        // ============================================================
        // STEP 10 — TRANSFORMATION: map() — extract just the Zone name
        // ============================================================
        // We only want the Zone name (column index 2), not the full row
        // map() one String[] → one String
        // LAZY

        JavaRDD<String> zoneNamesRDD = yellowZoneRDD.map(
                row -> row[2]   // just the Zone name
                // ["4","Manhattan","Alphabet City","Yellow Zone"]  →  "Alphabet City"
        );
//    zoneNamesRDD.collect();
        printSection("STEP 10 — map(zone name) — TRANSFORMATION (lazy)");
        System.out.println("zoneNamesRDD extracts just column[2] — the Zone name.");
        System.out.println("Full DAG now:");
        System.out.println("  textFile");
        System.out.println("    → map(split into columns)");
        System.out.println("      → filter(skip header)");
        System.out.println("        → filter(Manhattan only)");
        System.out.println("          → filter(Yellow Zone only)");
        System.out.println("            → map(extract zone name)");
        System.out.println("  *** NOTHING HAS RUN YET ***");

        // ============================================================
        // STEP 11 — ACTION: collect() — get all Yellow Zone names
        // ============================================================
        // JOB 4 — triggers the entire 6-step DAG above

        printSection("STEP 11 — collect() — ACTION — JOB 4 fires");
        System.out.println("All Manhattan Yellow Zone names:");
        System.out.println();

        List<String> yellowZoneNames = zoneNamesRDD.collect();
        for (String name : yellowZoneNames) {
            System.out.println("  → " + name);
        }

        System.out.println("\nTotal: " + yellowZoneNames.size() + " Yellow Zones in Manhattan");
        System.out.println("\n→ Spark UI: Job 4 ran the full 6-step pipeline.");
        System.out.println("  Still ONE Stage — no shuffles, all narrow transformations.");
        System.out.println("  4 Tasks ran in parallel (one per partition).");

        // ============================================================
        // STEP 12 — TRANSFORMATION: map() to build a readable report line
        // ============================================================
        // Go back to manhattanRDD and format each row into a readable string
        // LAZY

        JavaRDD<String> manhattanReportRDD = manhattanRDD.map(
                row -> String.format("Zone: %-35s | Type: %s", row[2], row[3])
        );

        // ============================================================
        // STEP 13 — ACTION: take(10) — preview the report
        // ============================================================
        // JOB 5

        printSection("STEP 12+13 — map(format) + take(10) — JOB 5 fires");
        System.out.println("Manhattan zones (first 10):");
        System.out.println();

        List<String> reportPreview = manhattanReportRDD.take(10);
        reportPreview.forEach(line -> System.out.println("  " + line));

        System.out.println("\n→ Spark UI: Job 5. Notice take(10) stopped early —");
        System.out.println("  Spark did not process all 265 rows, only enough to find 10.");

        // ============================================================
        // FINAL SUMMARY
        // ============================================================

        printSection("FINAL SUMMARY — What happened in Spark UI");
        System.out.println(
                "  Job 0  → rawRDD.count()              reads file, counts all rows\n" +
                        "  Job 1  → rawRDD.first()               reads file, gets header line\n" +
                        "  Job 2  → colsRDD.take(5)              reads + splits, returns 5 rows\n" +
                        "  Job 3  → manhattanRDD.count()         reads + splits + 2 filters\n" +
                        "  Job 4  → zoneNamesRDD.collect()       reads + splits + 3 filters + map\n" +
                        "  Job 5  → manhattanReportRDD.take(10)  reads + splits + filter + map\n" +
                        "\n" +
                        "  Total: 6 Actions = 6 Jobs\n" +
                        "\n" +
                        "  TRANSFORMATIONS used (all lazy, no job created):\n" +
                        "    textFile()   — read file as RDD of Strings\n" +
                        "    map()        — split line / extract column / format output\n" +
                        "    filter()     — skip header / keep Manhattan / keep Yellow Zone\n" +
                        "\n" +
                        "  ACTIONS used (each one triggers a Job):\n" +
                        "    count()      — how many rows match\n" +
                        "    first()      — look at the first row\n" +
                        "    take(n)      — peek at first n rows (stops early!)\n" +
                        "    collect()    — bring all results to the driver\n" +
                        "\n" +
                        "  KEY OBSERVATION in Spark UI:\n" +
                        "    Every single job ran in just 1 Stage.\n" +
                        "    Why? map() and filter() are NARROW transformations.\n" +
                        "    Data stays in its partition, no shuffling needed.\n" +
                        "    Spark pipelines all steps in one pass per partition.\n" +
                        "\n" +
                        "    When we add groupBy() or join() in the next module —\n" +
                        "    THAT is when you will see 2 Stages with a Shuffle."
        );

        printSection("PAUSED — Explore http://localhost:4040 — Press ENTER to exit");
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

