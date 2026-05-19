// ============================================================================
// E — EXTRACT
// ============================================================================
// We connected Apache Spark directly to our PostgreSQL database and pulled
// three tables into Spark's memory:
//
//   • Orders — every purchase transaction, who bought what, how much they
//     paid and whether it was delivered
//   • Products — the product catalogue with names, categories and prices
//   • Customers — the people who placed those orders, including which city
//     they are from
//
// At this point we had raw, disconnected data. Orders didn't know product
// names. Products didn't know who bought them. Customers didn't know what
// they spent.
//
// ============================================================================
// T — TRANSFORM
// ============================================================================
// Inside Spark we connected all three tables together and asked real
// business questions:
//
//   • Joined orders with products to find out which category each sale
//     belonged to
//   • Joined with customers to find out who was spending the most
//   • Grouped and aggregated to calculate total revenue, number of orders
//     and average order value per category
//   • Ranked customers by their total spend across all purchases
//   • Labelled every order as High, Medium or Low value based on the amount
//
// At this point we had answers — not raw data.
//
// ============================================================================
// L — LOAD
// ============================================================================
// We wrote those answers back into PostgreSQL as two clean, ready-to-use
// tables:
//
//   • sales_summary — one row per category telling the business exactly how
//     much revenue each product category generated
//   • customer_report — one row per customer showing their total spend,
//     number of orders and average basket size
//
// Any reporting tool, dashboard or manager can now open these two tables and
// get instant business insight — without ever touching the raw transactional
// data or slowing down the live database.
//
// ============================================================================
// RESULT
// ============================================================================
// We turned 50 raw order records spread across three disconnected tables
// into two clean reports that directly answer the questions a business
// actually needs to run — all in a single Spark job.


package com.beacon;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Scanner;

import static org.apache.spark.sql.functions.*;

/**
 * ============================================================
 *  SPARK WITH POSTGRESQL — READ AND WRITE VIA JDBC
 *  Beacon Solutions, Inc.
 * ============================================================
 *
 *  PROBLEM STATEMENT:
 *  ------------------
 *  Beacon Solutions manages a retail operation with sales data
 *  stored in a PostgreSQL database (beacon_retail).
 *
 *  The data team needs a Spark job that:
 *
 *  READS from PostgreSQL:
 *    - retail.orders    → all sales transactions
 *    - retail.products  → product catalogue with categories
 *    - retail.customers → customer details
 *
 *  TRANSFORMS using Spark:
 *    - Join orders with products to get category information
 *    - Join with customers to get location information
 *    - Calculate total revenue, order count, and average order
 *      value per product category
 *    - Identify top customers by total spend
 *    - Flag orders as High / Medium / Low value
 *
 *  WRITES back to PostgreSQL:
 *    - retail.sales_summary   → aggregated category revenue report
 *    - retail.customer_report → top customer spend analysis
 *
 *  WHY SPARK FOR THIS?
 *  --------------------
 *  PostgreSQL is great for transactional data (OLTP).
 *  When your analytics queries become too complex or slow,
 *  you pull the data into Spark (OLAP), process in parallel,
 *  and write aggregated results back. This is the classic
 *  ETL pattern: Extract from source, Transform in Spark,
 *  Load back to a reporting database or data warehouse.
 *
 *  PRE-REQUISITES:
 *  ---------------
 *  1. PostgreSQL installed and running on localhost:5432
 *  2. Run setup_postgres.sql to create the database and tables
 *  3. PostgreSQL JDBC driver in pom.xml (see comment at bottom)
 *
 *  CONNECTION DETAILS:
 *  -------------------
 *  Host:     localhost
 *  Port:     5432
 *  Database: beacon_retail
 *  Schema:   retail
 *  User:     postgres
 *  Password: (update to your local password below)
 * ============================================================
 */
public class SparkWithPostgreSQL {

    // ── Database connection config ────────────────────────────
    // Update these to match your local PostgreSQL installation
    private static final String JDBC_URL  = "jdbc:postgresql://localhost:5432/beacon_retail";
    private static final String DB_USER   = "postgres";
    private static final String DB_PASS   = "admin";   // ← change to your password
    private static final String DRIVER    = "org.postgresql.Driver";

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Spark with PostgreSQL - Beacon")
                .master("local[4]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");
        System.out.println("\n>>> Spark UI: http://localhost:4040 <<<\n");

        // ============================================================
        // PART 1 — READ FROM POSTGRESQL
        // ============================================================
        /*
         * Spark reads from any JDBC-compatible database using the
         * "jdbc" format. You need to provide:
         *
         *   url      → JDBC connection string
         *   dbtable  → the table or schema.table to read
         *              Can also be a subquery: "(SELECT ... ) alias"
         *   user     → database username
         *   password → database password
         *   driver   → fully qualified JDBC driver class name
         *
         * HOW IT WORKS INTERNALLY:
         *   Spark sends a SELECT * to PostgreSQL.
         *   PostgreSQL executes and streams rows back to Spark.
         *   Spark distributes rows across partitions.
         *   By default: 1 partition (single JDBC connection).
         *   For large tables, use numPartitions + partitionColumn
         *   to parallelise reads (covered in the config section).
         *
         * SPARK UI — SQL tab:
         *   Look for Scan JDBCRelation in the physical plan.
         *   That is Spark reading from PostgreSQL.
         */

        printSection("PART 1 — READ: Loading tables from PostgreSQL");

        // ── Read orders table ─────────────────────────────────────
        Dataset<Row> ordersDF = spark.read()
                .format("jdbc")
                .option("url",      JDBC_URL)
                .option("dbtable",  "retail.orders")
                .option("user",     DB_USER)
                .option("password", DB_PASS)
                .option("driver",   DRIVER)
                .load();

        System.out.println("--- Orders table ---");
        ordersDF.printSchema();
        System.out.println("Total orders: " + ordersDF.count());
        ordersDF.show(5, false);

        // ── Read products table ───────────────────────────────────
        Dataset<Row> productsDF = spark.read()
                .format("jdbc")
                .option("url",      JDBC_URL)
                .option("dbtable",  "retail.products")
                .option("user",     DB_USER)
                .option("password", DB_PASS)
                .option("driver",   DRIVER)
                .load();

        System.out.println("--- Products table ---");
        productsDF.printSchema();
        System.out.println("Total products: " + productsDF.count());
        productsDF.show(5, false);

        // ── Read customers table ──────────────────────────────────
        Dataset<Row> customersDF = spark.read()
                .format("jdbc")
                .option("url",      JDBC_URL)
                .option("dbtable",  "retail.customers")
                .option("user",     DB_USER)
                .option("password", DB_PASS)
                .option("driver",   DRIVER)
                .load();

        System.out.println("--- Customers table ---");
        System.out.println("Total customers: " + customersDF.count());
        customersDF.show(false);

        // ── Alternative: Read using a custom SQL query ────────────
        /*
         * Instead of reading a whole table, you can push a query
         * down to PostgreSQL. Useful for filtering at the source
         * and reducing data transferred to Spark.
         *
         * Wrap the query in parentheses and give it an alias.
         * This is called a "push-down query" or "subquery read".
         */

        System.out.println("\n--- Read using custom SQL query (Delivered orders only) ---");
        Dataset<Row> deliveredOrdersDF = spark.read()
                .format("jdbc")
                .option("url",      JDBC_URL)
                .option("dbtable",
                        "(SELECT * FROM retail.orders WHERE status = 'Delivered') AS delivered")
                .option("user",     DB_USER)
                .option("password", DB_PASS)
                .option("driver",   DRIVER)
                .load();

        System.out.println("Delivered orders only: " + deliveredOrdersDF.count());
        deliveredOrdersDF.show(5, false);

        // ============================================================
        // PART 2 — TRANSFORM: Process the data in Spark
        // ============================================================
        /*
         * Once loaded into DataFrames, work with them exactly as
         * with any other Spark DataFrame. No special JDBC handling.
         * The data is now in Spark memory, fully distributed.
         */

        printSection("PART 2 — TRANSFORM: Process data in Spark");

        // ── Register views for SQL ────────────────────────────────
        ordersDF.createOrReplaceTempView("Orders");
        productsDF.createOrReplaceTempView("Products");
        customersDF.createOrReplaceTempView("Customers");

        // ── Transform 1: Full order details (join all three tables) ──
        System.out.println("--- Full order details: orders + products + customers ---");

        Dataset<Row> fullOrdersDF = spark.sql(
                "SELECT " +
                        "    o.order_id, " +
                        "    o.order_date, " +
                        "    c.first_name || ' ' || c.last_name AS customer_name, " +
                        "    c.city, " +
                        "    p.product_name, " +
                        "    p.category, " +
                        "    o.quantity, " +
                        "    o.total_amount, " +
                        "    o.status " +
                        "FROM Orders o " +
                        "JOIN Products  p ON o.product_id  = p.product_id " +
                        "JOIN Customers c ON o.customer_id = c.customer_id " +
                        "ORDER BY o.order_date"
        );

        fullOrdersDF.show(10, false);

        // ── Transform 2: Add order value classification ───────────
        Dataset<Row> classifiedOrdersDF = fullOrdersDF.withColumn(
                "ValueTier",
                when(col("total_amount").geq(500),  "High")
                        .when(col("total_amount").geq(100),  "Medium")
                        .otherwise("Low")
        );

        System.out.println("--- Orders with value classification ---");
        classifiedOrdersDF.select(
                "order_id", "customer_name", "product_name",
                "total_amount", "ValueTier"
        ).show(10, false);

        // ── Transform 3: Sales summary by category ────────────────
        /*
         * This is the main aggregation we will write back to PostgreSQL.
         * GROUP BY triggers a shuffle — 2 stages in Spark UI.
         */
        System.out.println("--- Sales summary by category ---");

        Dataset<Row> salesSummaryDF = spark.sql(
                "SELECT " +
                        "    p.category, " +
                        "    COUNT(*)                          AS total_orders, " +
                        "    ROUND(SUM(o.total_amount), 2)     AS total_revenue, " +
                        "    ROUND(AVG(o.total_amount), 2)     AS avg_order_value, " +
                        "    SUM(o.quantity)                   AS total_units_sold " +
                        "FROM Orders o " +
                        "JOIN Products p ON o.product_id = p.product_id " +
                        "GROUP BY p.category " +
                        "ORDER BY total_revenue DESC"
        );

        salesSummaryDF.show(false);

        // ── Transform 4: Top customers by total spend ────────────
        System.out.println("--- Top customers by total spend ---");

        Dataset<Row> customerReportDF = spark.sql(
                "SELECT " +
                        "    c.customer_id, " +
                        "    c.first_name || ' ' || c.last_name AS customer_name, " +
                        "    c.city, " +
                        "    COUNT(*)                           AS total_orders, " +
                        "    ROUND(SUM(o.total_amount), 2)      AS total_spend, " +
                        "    ROUND(AVG(o.total_amount), 2)      AS avg_order_value " +
                        "FROM Orders o " +
                        "JOIN Customers c ON o.customer_id = c.customer_id " +
                        "GROUP BY c.customer_id, c.first_name, c.last_name, c.city " +
                        "ORDER BY total_spend DESC"
        );

        customerReportDF.show(false);

        // ============================================================
        // PART 3 — WRITE BACK TO POSTGRESQL
        // ============================================================
        /*
         * Writing to PostgreSQL uses the same JDBC format.
         * You also need to specify a SaveMode:
         *
         *   SaveMode.Overwrite  → drops and recreates the table
         *   SaveMode.Append     → adds rows to existing table
         *   SaveMode.ErrorIfExists → fails if table already exists
         *   SaveMode.Ignore     → does nothing if table already exists
         *
         * IMPORTANT:
         *   SaveMode.Overwrite drops the ENTIRE TABLE and recreates it.
         *   For production, use Append or handle the table lifecycle
         *   separately in your pipeline.
         *
         * SPARK UI:
         *   Writing to JDBC does not appear in the SQL tab
         *   the same way reads do. Check the Jobs tab instead —
         *   you'll see a job triggered by the write action.
         */

        printSection("PART 3 — WRITE: Results back to PostgreSQL");

        // ── Write sales summary ───────────────────────────────────
        System.out.println("Writing sales_summary to PostgreSQL...");

        salesSummaryDF.write()
                .format("jdbc")
                .option("url",      JDBC_URL)
                .option("dbtable",  "retail.sales_summary")
                .option("user",     DB_USER)
                .option("password", DB_PASS)
                .option("driver",   DRIVER)
                .mode(SaveMode.Overwrite)
                .save();

        System.out.println("sales_summary written successfully.");

        // ── Write customer report ─────────────────────────────────
        System.out.println("Writing customer_report to PostgreSQL...");

        customerReportDF.write()
                .format("jdbc")
                .option("url",      JDBC_URL)
                .option("dbtable",  "retail.customer_report")
                .option("user",     DB_USER)
                .option("password", DB_PASS)
                .option("driver",   DRIVER)
                .mode(SaveMode.Overwrite)
                .save();

        System.out.println("customer_report written successfully.");

        // ── Verify: Read back what was written ────────────────────
        /*
         * Always verify writes in a pipeline.
         * Read the table back and confirm the row count matches.
         */

        printSection("PART 4 — VERIFY: Read back what was written");

        Dataset<Row> verifyDF = spark.read()
                .format("jdbc")
                .option("url",      JDBC_URL)
                .option("dbtable",  "retail.sales_summary")
                .option("user",     DB_USER)
                .option("password", DB_PASS)
                .option("driver",   DRIVER)
                .load();

        System.out.println("--- sales_summary table (read back from PostgreSQL) ---");
        verifyDF.show(false);

        Dataset<Row> verifyCustDF = spark.read()
                .format("jdbc")
                .option("url",      JDBC_URL)
                .option("dbtable",  "retail.customer_report")
                .option("user",     DB_USER)
                .option("password", DB_PASS)
                .option("driver",   DRIVER)
                .load();

        System.out.println("--- customer_report table (read back from PostgreSQL) ---");
        verifyCustDF.show(false);

        // ============================================================
        // SUMMARY
        // ============================================================

        printSection("SUMMARY — Spark + JDBC pattern");
        System.out.println(
                "  READ from PostgreSQL:\n" +
                        "    spark.read()\n" +
                        "         .format('jdbc')\n" +
                        "         .option('url',      jdbcUrl)\n" +
                        "         .option('dbtable',  'schema.table')\n" +
                        "         .option('user',     username)\n" +
                        "         .option('password', password)\n" +
                        "         .option('driver',   'org.postgresql.Driver')\n" +
                        "         .load()\n" +
                        "\n" +
                        "  Push-down query (read only what you need):\n" +
                        "    .option('dbtable', '(SELECT * FROM t WHERE x=1) AS alias')\n" +
                        "\n" +
                        "  WRITE to PostgreSQL:\n" +
                        "    df.write()\n" +
                        "       .format('jdbc')\n" +
                        "       .option(...)            same options as read\n" +
                        "       .option('dbtable',  'schema.new_table')\n" +
                        "       .mode(SaveMode.Overwrite)\n" +
                        "       .save()\n" +
                        "\n" +
                        "  SaveMode options:\n" +
                        "    Overwrite      -> drops table and recreates\n" +
                        "    Append         -> adds rows to existing table\n" +
                        "    ErrorIfExists  -> fails if table exists (default)\n" +
                        "    Ignore         -> does nothing if table exists\n" +
                        "\n" +
                        "  pom.xml dependency needed:\n" +
                        "    <groupId>org.postgresql</groupId>\n" +
                        "    <artifactId>postgresql</artifactId>\n" +
                        "    <version>42.7.4</version>"
        );

        System.out.println("\n>>> PAUSED — Check http://localhost:4040 — Press ENTER to exit <<<");
        System.out.println("    Then verify in pgAdmin:");
        System.out.println("    SELECT * FROM retail.sales_summary;");
        System.out.println("    SELECT * FROM retail.customer_report;");

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

/*
 * ============================================================
 *  POM.XML — Add this dependency to read/write PostgreSQL
 * ============================================================
 *
 *  <dependency>
 *      <groupId>org.postgresql</groupId>
 *      <artifactId>postgresql</artifactId>
 *      <version>42.7.4</version>
 *  </dependency>
 *
 * ============================================================
 */
