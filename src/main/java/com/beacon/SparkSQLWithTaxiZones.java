package com.beacon;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Scanner;

import static org.apache.spark.sql.functions.*;

public class SparkSQLWithTaxiZones {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Spark SQL - TaxiZones")
                .master("local[4]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");
        System.out.println("\n>>> Spark UI: http://localhost:4040 <<<\n");

        // ============================================================
        // SETUP — Read the same TaxiZones file we used before
        // ============================================================
        /*
         * Same file, same schema, same data as CreateDataframes.java.
         * The only new thing today is HOW we query it — using SQL.
         */

        String filePath = "C:\\Datasets\\TaxiZones.csv";

        StructType taxiZoneSchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("LocationID",  DataTypes.IntegerType, true),
                DataTypes.createStructField("Borough",     DataTypes.StringType,  true),
                DataTypes.createStructField("Zone",        DataTypes.StringType,  true),
                DataTypes.createStructField("ServiceZone", DataTypes.StringType,  true)
        });

        Dataset<Row> taxiZonesDF = spark
                .read()
                .option("header", "true")
                .schema(taxiZoneSchema)
                .csv(filePath);

        // ============================================================
        // THE BRIDGE — createOrReplaceTempView()
        // ============================================================
        /*
         * This is the ONE new concept today.
         *
         * createOrReplaceTempView("tableName")
         *
         * Registers the DataFrame as a TEMPORARY SQL VIEW.
         * After this, you can write plain SQL against it using spark.sql().
         *
         * The view:
         *   - Lives only for the duration of this SparkSession
         *   - Is NOT stored on disk — it's just a named reference
         *   - Can be replaced if you call the same name again
         *   - Is visible to spark.sql() calls anywhere in this program
         *
         * Think of it as: "Give this DataFrame a SQL table name"
         */

        taxiZonesDF.createOrReplaceTempView("TaxiZones");

        System.out.println("=== SETUP: TaxiZones registered as a SQL view ===");
        System.out.println("You can now write: spark.sql(\"SELECT ... FROM TaxiZones\")");
        System.out.println("Total rows: " + taxiZonesDF.count());

        // ============================================================
        // STEP 1 — SELECT * — see everything
        // ============================================================
        /*
         * spark.sql() takes a plain SQL string and returns a Dataset<Row>.
         * That Dataset<Row> is a DataFrame — same type as always.
         * It follows the same lazy + action model.
         *
         * spark.sql("SELECT ...") → TRANSFORMATION (lazy)
         * .show()                 → ACTION (triggers the job)
         */

        System.out.println("\n=== STEP 1: SELECT * FROM TaxiZones ===");

        Dataset<Row> allZones = spark.sql("SELECT * FROM TaxiZones");
        allZones.show(5, false);
        /*
         * +----------+---------+------------------------+-----------+
         * |LocationID|  Borough|                    Zone|ServiceZone|
         * +----------+---------+------------------------+-----------+
         * |         1|      EWR|         Newark Airport |        EWR|
         * |         2|   Queens|             Jamaica Bay|  Boro Zone|
         * |         3|    Bronx|Allerton/Pelham Gardens |  Boro Zone|
         * +----------+---------+------------------------+-----------+
         *
         * Same data you saw in CreateDataframes.java.
         * SQL is just another way to express the same operations.
         */

        // ============================================================
        // STEP 2 — SELECT specific columns
        // ============================================================
        /*
         * DataFrame way:  taxiZonesDF.select("Borough", "Zone")
         * SQL way:        spark.sql("SELECT Borough, Zone FROM TaxiZones")
         *
         * Exact same result. Exact same job in Spark UI.
         * Catalyst compiles both down to the same physical plan.
         */

        System.out.println("\n=== STEP 2: SELECT specific columns ===");

        spark.sql("SELECT Borough, Zone FROM TaxiZones")
                .show(5, false);

        // Alias a column in SQL using AS
        System.out.println("--- SELECT with alias (AS) ---");
        spark.sql("SELECT LocationID AS ID, Borough, Zone AS ZoneName, ServiceZone AS Type FROM TaxiZones")
                .show(5, false);

        // ============================================================
        // STEP 3 — WHERE — filter rows
        // ============================================================
        /*
         * DataFrame way:  taxiZonesDF.filter(col("Borough").equalTo("Manhattan"))
         * SQL way:        spark.sql("SELECT * FROM TaxiZones WHERE Borough = 'Manhattan'")
         *
         * Note single quotes around string values in SQL.
         */

        System.out.println("\n=== STEP 3: WHERE — filter rows ===");

        // Simple WHERE
        System.out.println("--- Manhattan zones ---");
        Dataset<Row> manhattanSQL = spark.sql(
                "SELECT * FROM TaxiZones WHERE Borough = 'Manhattan'"
        );
        System.out.println("Count: " + manhattanSQL.count());
        manhattanSQL.show(5, false);

        // WHERE with AND
        System.out.println("--- Manhattan Yellow Zones (WHERE with AND) ---");
        spark.sql("SELECT * FROM TaxiZones  WHERE Borough = 'Manhattan' AND ServiceZone = 'Yellow Zone'")
                .show(10, false);

        // WHERE with LIKE
        System.out.println("--- Zones starting with 'Central' (LIKE) ---");
        spark.sql("SELECT * FROM TaxiZones WHERE Zone LIKE 'Central%' ")
                .show(false);

        // WHERE with IN
        System.out.println("--- Queens, Bronx and Brooklyn (IN) ---");
        spark.sql("SELECT * FROM TaxiZones WHERE Borough IN ('Queens', 'Bronx', 'Brooklyn')")
                .show(10, false);

        // ============================================================
        // STEP 4 — CASE WHEN — computed columns in SQL
        // ============================================================
        /*
         * DataFrame way:  .withColumn("IsOuterBorough",
         *                     when(col("Borough").equalTo("Manhattan"), "No").otherwise("Yes"))
         *
         * SQL way:        CASE WHEN Borough = 'Manhattan' THEN 'No' ELSE 'Yes' END
         *
         * Use AS to name the result column.
         */

        System.out.println("\n=== STEP 4: CASE WHEN — computed columns ===");

        spark.sql("""
                SELECT
                    LocationID,
                    Borough,
                    Zone,
                    ServiceZone,
                    CASE
                        WHEN Borough = 'Manhattan' THEN 'No'
                        ELSE 'Yes'
                    END AS IsOuterBorough,
                    CASE
                        WHEN ServiceZone = 'Yellow Zone' THEN 'YZ'
                        WHEN ServiceZone = 'Boro Zone'   THEN 'BZ'
                        WHEN ServiceZone = 'Airports'    THEN 'AP'
                        ELSE 'OT'
                    END AS ZoneCode
                FROM TaxiZones
                """)
                .show(10, false);
        /*
         * Same output as the withColumn() step in CreateDataframes.
         * Just written in SQL syntax.
         */

        // ============================================================
        // STEP 5 — GROUP BY + aggregate functions
        // ============================================================
        /*
         * DataFrame way:  taxiZonesDF.groupBy("Borough").agg(count("*").alias("TotalZones"))
         * SQL way:        SELECT Borough, COUNT(*) AS TotalZones FROM TaxiZones GROUP BY Borough
         *
         * Remember: GROUP BY causes a SHUFFLE.
         * → Spark UI: 2 stages, Exchange node in SQL tab.
         */

        System.out.println("\n=== STEP 5: GROUP BY + COUNT — causes a shuffle! ===");

        spark.sql("""
                SELECT
                    Borough,
                    COUNT(*) AS TotalZones
                FROM TaxiZones
                GROUP BY Borough
                ORDER BY TotalZones DESC
                """)
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
         * Identical to what groupBy().agg() produced.
         * → Go check Spark UI SQL tab — same Exchange node, same 2 stages.
         */

        // GROUP BY multiple columns
        System.out.println("--- GROUP BY Borough AND ServiceZone ---");
        spark.sql("""
                SELECT
                    Borough,
                    ServiceZone,
                    COUNT(*) AS ZoneCount
                FROM TaxiZones
                GROUP BY Borough, ServiceZone
                ORDER BY Borough ASC, ZoneCount DESC
                """)
                .show(20, false);

        // ============================================================
        // STEP 6 — ORDER BY
        // ============================================================
        /*
         * DataFrame way:  .orderBy(col("Borough").asc(), col("Zone").asc())
         * SQL way:        ORDER BY Borough ASC, Zone ASC
         *
         * ASC  = ascending  (A→Z, 0→9) — default
         * DESC = descending (Z→A, 9→0)
         */

        System.out.println("\n=== STEP 6: ORDER BY ===");

        spark.sql("""
                SELECT Borough, Zone, ServiceZone
                FROM TaxiZones
                ORDER BY Borough ASC, Zone ASC
                """)
                .show(10, false);

        // ============================================================
        // STEP 7 — DISTINCT
        // ============================================================
        /*
         * DataFrame way:  taxiZonesDF.select("Borough").distinct()
         * SQL way:        SELECT DISTINCT Borough FROM TaxiZones
         */

        System.out.println("\n=== STEP 7: SELECT DISTINCT ===");

        System.out.println("--- Unique Boroughs ---");
        spark.sql("""
                SELECT DISTINCT Borough
                FROM TaxiZones
                ORDER BY Borough
                """)
                .show(false);

        System.out.println("--- Unique ServiceZone types ---");
        spark.sql("SELECT DISTINCT ServiceZone FROM TaxiZones")
                .show(false);

        // ============================================================
        // STEP 8 — LIMIT
        // ============================================================
        /*
         * DataFrame way:  taxiZonesDF.show(5) or .take(5)
         * SQL way:        SELECT * FROM TaxiZones LIMIT 5
         *
         * LIMIT in SQL = take(n) in DataFrame API
         */

        System.out.println("\n=== STEP 8: LIMIT ===");

        spark.sql("SELECT * FROM TaxiZones LIMIT 5")
                .show(false);

        // ============================================================
        // STEP 9 — Full SQL query — same question as Step 10 in CreateDataframes
        // ============================================================
        /*
         * In CreateDataframes.java Step 10, the DataFrame pipeline was:
         *
         *   taxiZonesDF
         *       .filter(col("Borough").notEqual("EWR"))
         *       .filter(col("ServiceZone").isin("Yellow Zone", "Boro Zone"))
         *       .groupBy("Borough", "ServiceZone")
         *       .agg(count("*").alias("ZoneCount"))
         *       .orderBy(col("Borough").asc(), col("ZoneCount").desc())
         *
         * Here is the EXACT same query written in SQL.
         * Same result. Same physical plan. Same job in Spark UI.
         */

        System.out.println("\n=== STEP 9: Same question as Step 10 — SQL version ===");
        System.out.println("DataFrame: filter → filter → groupBy → agg → orderBy");
        System.out.println("SQL:       WHERE  → IN     → GROUP BY → COUNT → ORDER BY");
        System.out.println();

        spark.sql("""
                SELECT
                    Borough,
                    ServiceZone,
                    COUNT(*) AS ZoneCount
                FROM TaxiZones
                WHERE Borough != 'EWR'
                  AND ServiceZone IN ('Yellow Zone', 'Boro Zone')
                GROUP BY Borough, ServiceZone
                ORDER BY Borough ASC, ZoneCount DESC
                """)
                .show(false);

        // ============================================================
        // STEP 10 — Mix SQL and DataFrame API (they work together)
        // ============================================================
        /*
         * You can go back and forth between SQL and DataFrame API freely.
         * spark.sql() returns a Dataset<Row> — just another DataFrame.
         * You can call .filter(), .withColumn(), .show() on it afterwards.
         *
         * This is useful when part of your logic is easier in SQL
         * and part is easier in the DataFrame API.
         */

        System.out.println("\n=== STEP 10: SQL result → back to DataFrame API ===");

        // Step A: Run SQL to get Manhattan zones
        Dataset<Row> manhattanFromSQL = spark.sql(
                "SELECT * FROM TaxiZones WHERE Borough = 'Manhattan'"
        );

        // Step B: Continue with DataFrame API on the SQL result
        manhattanFromSQL
                .withColumn("ZoneCode",
                        when(col("ServiceZone").equalTo("Yellow Zone"), "YZ")
                                .otherwise("OT"))
                .select("Zone", "ServiceZone", "ZoneCode")
                .orderBy("Zone")
                .show(10, false);

        // ============================================================
        // SIDE BY SIDE — DataFrame API vs SQL (same result)
        // ============================================================

        System.out.println("\n=== SIDE BY SIDE — DataFrame API vs SQL ===");

        System.out.println("--- DataFrame API ---");
        taxiZonesDF
                .filter(col("Borough").equalTo("Queens"))
                .groupBy("ServiceZone")
                .agg(count("*").alias("Count"))
                .orderBy(col("Count").desc())
                .show(false);

        System.out.println("--- SQL (same result) ---");
        spark.sql("""
                SELECT ServiceZone, COUNT(*) AS Count
                FROM TaxiZones
                WHERE Borough = 'Queens'
                GROUP BY ServiceZone
                ORDER BY Count DESC
                """)
                .show(false);

        /*
         * OUTPUT IS IDENTICAL.
         * Catalyst compiles both to the same physical plan.
         * Use whichever is more readable for your team.
         * Most teams use SQL for analytics, DataFrame API for transformations.
         */

        // ============================================================
        // SUMMARY
        // ============================================================

        System.out.println("\n=== SUMMARY — SQL vs DataFrame API ===\n");
        System.out.println(
                "  createOrReplaceTempView('name')   → register DF as a SQL view\n" +
                        "  spark.sql('SELECT ...')            → run SQL, returns Dataset<Row>\n" +
                        "\n" +
                        "  SQL              ←→  DataFrame API\n" +
                        "  ─────────────────────────────────────────────────────\n" +
                        "  SELECT col        ←→  .select(col('col'))\n" +
                        "  col AS alias      ←→  col('col').alias('alias')\n" +
                        "  WHERE x = 'val'   ←→  .filter(col('x').equalTo('val'))\n" +
                        "  AND / OR          ←→  .and() / .or()\n" +
                        "  LIKE 'val%'       ←→  .startsWith('val')\n" +
                        "  IN ('a','b')      ←→  .isin('a','b')\n" +
                        "  CASE WHEN...END   ←→  when(cond,val).otherwise(val)\n" +
                        "  GROUP BY          ←→  .groupBy()\n" +
                        "  COUNT(*)          ←→  count('*')\n" +
                        "  ORDER BY col DESC ←→  .orderBy(col('col').desc())\n" +
                        "  SELECT DISTINCT   ←→  .distinct()\n" +
                        "  LIMIT n           ←→  .show(n) or .take(n)\n" +
                        "\n" +
                        "  KEY POINT:\n" +
                        "    Both compile to the same physical plan via Catalyst.\n" +
                        "    Same performance. Same Spark UI output.\n" +
                        "    Use SQL when your team knows SQL.\n" +
                        "    Use DataFrame API when you need programmatic logic."
        );

        System.out.println("\n>>> PAUSED — Check http://localhost:4040 → SQL tab <<<");
        System.out.println("    Each spark.sql() call appears as a separate query.");
        System.out.println("    Compare the plan of a SQL query vs the DataFrame API query.");
        System.out.println("    They should look identical.\n");
        System.out.println("Press ENTER to exit...");

        try (final var scanner = new Scanner(System.in)) {
            scanner.nextLine();
        }
    }
}