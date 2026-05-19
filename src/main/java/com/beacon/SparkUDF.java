package com.beacon;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.util.Scanner;

import static org.apache.spark.sql.functions.*;

/**
 * ============================================================
 *  USER DEFINED FUNCTIONS (UDFs) IN SPARK
 *  Beacon Solutions, Inc.
 * ============================================================
 *
 *  WHAT IS A UDF?
 *  --------------
 *  A UDF (User Defined Function) is a custom function you write
 *  in Java and register with Spark so you can use it just like
 *  a built-in function — both in the DataFrame API and in SQL.
 *
 *  WHEN DO YOU NEED A UDF?
 *  ------------------------
 *  When Spark's built-in functions (functions.*) cannot express
 *  your transformation. For example:
 *    - Custom string formatting  (our example today)
 *    - Business-specific logic
 *    - Complex parsing or encoding
 *
 *  WHEN SHOULD YOU NOT USE A UDF?
 *  --------------------------------
 *  If a built-in function can do the job — use that instead.
 *  UDFs are opaque to Catalyst. Spark cannot optimize inside them.
 *  Built-ins like upper(), lower(), concat(), regexp_replace()
 *  are always faster because Catalyst understands them.
 *
 *  UDF PERFORMANCE WARNING:
 *  -------------------------
 *  Every row goes through your Java function one at a time.
 *  No vectorization, no Catalyst optimization, no code generation.
 *  Use UDFs as a last resort — not a first choice.
 *
 *  DATASET: Cabs.csv
 *  Column:  Name  →  e.g. "CHAWKI,MICHAEL"
 *  Goal:    Format to  →  "Chawki, Michael"
 * ============================================================
 */
public class SparkUDF {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Spark UDF - Beacon")
                .master("local[4]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");
        System.out.println("\n>>> Spark UI: http://localhost:4040 <<<\n");

        // ============================================================
        // SETUP — Load the Cabs dataset
        // ============================================================

        String filePath = "C:\\Datasets\\Cabs.csv";

        Dataset<Row> cabsDF = spark
                .read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(filePath);

        System.out.println("=== Raw Cabs data ===");
        cabsDF.show();
        /*
         * The Name column contains values like:
         *   CHAWKI,MICHAEL
         *   SMITH,JOHN
         *   DOE,JANE
         *
         * We want to reformat to:
         *   Chawki, Michael
         *   Smith, John
         *   Doe, Jane
         *
         * No single built-in function can do this exact transformation.
         * This is exactly where a UDF is justified.
         */

        // Register as SQL view for later use with spark.sql()
        cabsDF.createOrReplaceTempView("Cabs");

        // ============================================================
        // STEP 1 — Define the UDF
        // ============================================================
        /*
         * UDF1<InputType, OutputType>
         *   InputType  → the Java type of the input column
         *   OutputType → the Java type the function returns
         *
         * Here: UDF1<String, String>
         *   takes one String (the Name column)
         *   returns one String (the formatted name)
         *
         * ALWAYS handle null input.
         * Spark columns can be null — if you don't check, your UDF
         * will throw a NullPointerException at runtime.
         *
         * LOGIC:
         *   "CHAWKI,MICHAEL"
         *    → split on ","  → ["CHAWKI", "MICHAEL"]
         *    → for each part → uppercase first letter + lowercase rest
         *    → join with ", " → "Chawki, Michael"
         */

        printSection("STEP 1 — Define the UDF");

        UDF1<String, String> convertCaseUDF = new UDF1<String, String>() {
            @Override
            public String call(String input) throws Exception {
                // Always handle null — columns can be null in real data
                if (input == null) {
                    return null;
                }

                StringBuilder result = new StringBuilder();
                String[] names = input.split(",");

                for (String name : names) {
                    result
                            .append(name.substring(0, 1).toUpperCase())   // first letter uppercase
                            .append(name.substring(1).toLowerCase())       // rest lowercase
                            .append(", ");                                  // separator
                }

                // trim trailing comma and space: "Chawki, Michael, " → "Chawki, Michael"
                return result.toString().trim().replaceAll(",$", "");
            }
        };

        System.out.println("UDF defined. Not registered yet — can't use it in SQL or DataFrame API.");

        // ============================================================
        // STEP 2 — Register the UDF with Spark
        // ============================================================
        /*
         * spark.udf().register("functionName", udfObject, returnType)
         *
         * "convertCaseUdf"  → the name you use in SQL queries
         * convertCaseUDF    → the Java UDF object defined above
         * DataTypes.StringType → the return type Spark needs to know
         *
         * After registration:
         *   - Available in SQL as: convertCaseUdf(columnName)
         *   - Available in DataFrame API as: callUDF("convertCaseUdf", col("Name"))
         */

        printSection("STEP 2 — Register the UDF with Spark");

        spark.udf().register("convertCaseUdf", convertCaseUDF, DataTypes.StringType);

        System.out.println("UDF registered as 'convertCaseUdf'.");
        System.out.println("Now usable in SQL and DataFrame API.");

        // ============================================================
        // STEP 3 — Use the UDF in the DataFrame API
        // ============================================================
        /*
         * Two ways to call a registered UDF in the DataFrame API:
         *
         * Way A: callUDF("name", col)
         *   → Uses the registered name — cleaner, more readable
         *
         * Way B: udf(udfObject, returnType).apply(col)
         *   → Uses the Java object directly — more verbose
         *
         * Both produce identical results. We'll use callUDF (Way A)
         * as it is consistent with the registered name pattern.
         */

        printSection("STEP 3 — Use UDF in DataFrame API");

        Dataset<Row> dfAPIResult = cabsDF.select(
                col("Name"),
                callUDF("convertCaseUdf", col("Name")).alias("FormattedName")
        );

        System.out.println("--- DataFrame API result ---");
        dfAPIResult.show(false);
        /*
         * +----------------+----------------+
         * |            Name|   FormattedName|
         * +----------------+----------------+
         * |  CHAWKI,MICHAEL| Chawki, Michael|
         * |     SMITH,JOHN |     Smith, John|
         * ...
         */

        // ============================================================
        // STEP 4 — Use the UDF in SQL
        // ============================================================
        /*
         * Once registered, the UDF name works in SQL exactly like
         * any built-in SQL function — UPPER(), LOWER(), etc.
         *
         * SQL style:  convertCaseUdf(ColumnName)
         */

        printSection("STEP 4 — Use UDF in SQL");

        Dataset<Row> sqlResult = spark.sql(
                "SELECT Name, convertCaseUdf(Name) AS FormattedName FROM Cabs"
        );

        System.out.println("--- SQL result (identical output) ---");
        sqlResult.show(false);

        // ============================================================
        // STEP 5 — Side by side: confirm both produce the same result
        // ============================================================

        printSection("STEP 5 — DataFrame API vs SQL: same result");

        System.out.println("DataFrame API:");
        dfAPIResult.show(5, false);

        System.out.println("SQL:");
        sqlResult.show(5, false);

        /*
         * Output is identical.
         *
         * SPARK UI — SQL tab:
         * After running these, open http://localhost:4040 → SQL tab.
         * Click either query and look at the physical plan.
         * You will see a node called "BatchEvalPython" or a user
         * function node — that is where your UDF executes.
         * Notice Catalyst cannot look inside it — no optimization.
         * Compare this to a plain filter() or withColumn() plan
         * where Catalyst can push and optimize freely.
         */

        // ============================================================
        // SUMMARY
        // ============================================================

        printSection("SUMMARY");
        System.out.println(
                "  DEFINING A UDF:\n" +
                        "    UDF1<InputType, OutputType> myUdf = new UDF1<...>() {\n" +
                        "        public OutputType call(InputType input) { ... }\n" +
                        "    };\n" +
                        "\n" +
                        "  REGISTERING A UDF:\n" +
                        "    spark.udf().register('name', udfObject, DataTypes.StringType)\n" +
                        "\n" +
                        "  USING IN DATAFRAME API:\n" +
                        "    callUDF('name', col('column'))           → preferred\n" +
                        "    udf(udfObject, returnType).apply(col)    → also valid\n" +
                        "\n" +
                        "  USING IN SQL:\n" +
                        "    spark.sql('SELECT myUdf(column) FROM table')\n" +
                        "\n" +
                        "  RULES:\n" +
                        "    Always null-check inside the UDF\n" +
                        "    Register before using — in SQL or DataFrame API\n" +
                        "    Prefer built-in functions — UDFs bypass Catalyst\n" +
                        "    Check Spark UI SQL tab — UDF shows as opaque node"
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