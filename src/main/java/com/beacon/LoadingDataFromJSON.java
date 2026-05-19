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
 *  LOADING DATA FROM JSON FILES
 *  Beacon Solutions, Inc.
 * ============================================================
 *
 *  WHY JSON IS DIFFERENT FROM CSV:
 *  --------------------------------
 *  CSV  → flat, every row has the same columns, all strings
 *  JSON → can be nested, a column can contain another object,
 *         a column can contain an array of values
 *
 *  THIS FILE COVERS:
 *  ------------------
 *  Part 1 — Flat JSON (TaxiBases.json)
 *           Simple key-value pairs, some columns nested one level
 *
 *  Part 2 — Nested JSON (employee.json)
 *           Objects inside objects, arrays inside objects
 *           How to define a nested schema
 *           How to access nested columns using dot notation
 *
 *  THE KEY NEW CONCEPT:
 *  ---------------------
 *  When JSON has nested fields, your StructType must mirror
 *  the nesting. You put a StructType INSIDE a StructField.
 *
 *  FILES:
 *    C:\Spark\DataFiles\TaxiBases.json   (flat-ish JSON)
 *    C:\Spark\DataFiles\employee.json    (deeply nested JSON)
 * ============================================================
 */
public class LoadingDataFromJSON {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Loading JSON Files - Beacon")
                .master("local[4]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");
        System.out.println("\n>>> Spark UI: http://localhost:4040 <<<\n");

        // ============================================================
        // PART 1 — Flat JSON: TaxiBases.json
        // ============================================================
        /*
         * TaxiBases.json structure — each record looks like:
         *
         * {
         *   "License Number": "B00001",
         *   "Entity Name": "UBER USA LLC",
         *   "Telephone Number": 3477893801,
         *   "SHL Endorsed": "No",
         *   "Type of Base": "Black Car",
         *   "Address": {
         *     "Building": "636",
         *     "Street": "W 28 ST",
         *     "City": "NEW YORK",
         *     "State": "NY",
         *     "Postcode": "10001"
         *   },
         *   "GeoLocation": {
         *     "Latitude": "40.75007",
         *     "Longitude": "-74.00107",
         *     "Location": "(40.75007, -74.00107)"
         *   }
         * }
         *
         * Notice: Address and GeoLocation are NESTED objects.
         * In the schema, we represent them as StructType inside StructField.
         */

        printSection("PART 1 — Flat JSON with one level of nesting: TaxiBases.json");

        // ── Step 1: Define the schema ─────────────────────────────
        /*
         * For flat fields → DataTypes.createStructField("name", DataTypes.StringType, true)
         *
         * For nested objects → DataTypes.createStructField("Address",
         *                          DataTypes.createStructType(new StructField[]{
         *                              ... inner fields here ...
         *                          }), true)
         *
         * The inner StructType mirrors exactly what's inside the JSON object.
         */

        StructType taxiBasesSchema = DataTypes.createStructType(new StructField[]{

                // Flat fields — simple types
                DataTypes.createStructField("License Number",    DataTypes.StringType,  true),
                DataTypes.createStructField("Entity Name",       DataTypes.StringType,  true),
                DataTypes.createStructField("Telephone Number",  DataTypes.LongType,    true),
                DataTypes.createStructField("SHL Endorsed",      DataTypes.StringType,  true),
                DataTypes.createStructField("Type of Base",      DataTypes.StringType,  true),

                // Nested object: Address → defined as StructType inside StructField
                DataTypes.createStructField("Address",
                        DataTypes.createStructType(new StructField[]{
                                DataTypes.createStructField("Building", DataTypes.StringType, true),
                                DataTypes.createStructField("Street",   DataTypes.StringType, true),
                                DataTypes.createStructField("City",     DataTypes.StringType, true),
                                DataTypes.createStructField("State",    DataTypes.StringType, true),
                                DataTypes.createStructField("Postcode", DataTypes.StringType, true)
                        }), true),

                // Nested object: GeoLocation → another StructType inside StructField
                DataTypes.createStructField("GeoLocation",
                        DataTypes.createStructType(new StructField[]{
                                DataTypes.createStructField("Latitude",  DataTypes.StringType, true),
                                DataTypes.createStructField("Longitude", DataTypes.StringType, true),
                                DataTypes.createStructField("Location",  DataTypes.StringType, true)
                        }), true)
        });

        // ── Step 2: Read the JSON file ────────────────────────────
        /*
         * .json(filePath) → reads JSON files
         *
         * KEY OPTION: "multiline" = "true"
         *   By default Spark expects each line to be one complete JSON record.
         *   If your JSON is formatted across multiple lines (pretty-printed),
         *   you MUST add option("multiline", "true").
         *   Without it — Spark tries to parse each line individually and fails.
         */

        String taxiBasesPath = "C:\\Datasets\\TaxiBases.json";

        Dataset<Row> taxiBasesDF = spark
                .read()
                .option("multiline", "true")   // JSON spans multiple lines
                .schema(taxiBasesSchema)        // explicit schema
                .json(taxiBasesPath);

        // ── Step 3: Inspect ──────────────────────────────────────

        System.out.println("--- printSchema() ---");
        taxiBasesDF.printSchema();
        /*
         * root
         *  |-- License Number: string (nullable = true)
         *  |-- Entity Name: string (nullable = true)
         *  |-- Telephone Number: long (nullable = true)
         *  |-- SHL Endorsed: string (nullable = true)
         *  |-- Type of Base: string (nullable = true)
         *  |-- Address: struct (nullable = true)         ← nested object
         *  |    |-- Building: string (nullable = true)
         *  |    |-- Street: string (nullable = true)
         *  |    |-- City: string (nullable = true)
         *  |    |-- State: string (nullable = true)
         *  |    |-- Postcode: string (nullable = true)
         *  |-- GeoLocation: struct (nullable = true)     ← nested object
         *  |    |-- Latitude: string (nullable = true)
         *  |    |-- Longitude: string (nullable = true)
         *  |    |-- Location: string (nullable = true)
         *
         * The tree structure in printSchema() directly mirrors
         * the nesting in your JSON file. This is how you verify
         * your schema was defined correctly.
         */

        System.out.println("\n--- show() — notice nested columns appear as structs ---");
        taxiBasesDF.show(5, false);
        /*
         * Nested columns show as {value1, value2, ...}
         * Address column: {636, W 28 ST, NEW YORK, NY, 10001}
         */

        // ── Step 4: Access nested fields using dot notation ──────
        /*
         * To select a field INSIDE a nested struct, use dot notation:
         *   col("Address.City")     → the City field inside Address
         *   col("GeoLocation.Latitude") → Latitude inside GeoLocation
         *
         * Or in SQL: Address.City, GeoLocation.Latitude
         */

        System.out.println("\n--- Accessing nested fields with dot notation ---");
        taxiBasesDF.select(
                col("Entity Name"),
                col("Type of Base"),
                col("Address.Street").alias("Street"),
                col("Address.City").alias("City"),
                col("GeoLocation.Latitude").alias("Lat"),
                col("GeoLocation.Longitude").alias("Long")
        ).show(5, false);

        // Same in SQL
        taxiBasesDF.createOrReplaceTempView("TaxiBases");

        System.out.println("\n--- Same thing using SQL dot notation ---");
        spark.sql(
                "SELECT `Entity Name`, `Type of Base`, " +
                        "Address.City AS City, " +
                        "Address.Street AS Street, " +
                        "GeoLocation.Latitude AS Lat " +
                        "FROM TaxiBases " +
                        "LIMIT 5"
        ).show(false);
        /*
         * Note: Column names with spaces (like 'Entity Name') need
         * backticks in SQL: `Entity Name`
         */

        // ============================================================
        // PART 2 — Deeply Nested JSON: employee.json
        // ============================================================
        /*
         * employee.json — this is what one record looks like:
         *
         * {
         *   "EmployeeID": 123,
         *   "Name": {
         *     "First": "John",
         *     "Last": "Doe"
         *   },
         *   "ContactInfo": {
         *     "Email": "john.doe@example.com",
         *     "Phone": "1234567890",
         *     "Address": {
         *       "Street": "123 Main St",
         *       "City": "New York",
         *       "State": "NY",
         *       "ZipCode": "10001",
         *       "PreviousAddresses": [        ← ARRAY of strings
         *         "456 Elm St",
         *         "789 Oak St"
         *       ]
         *     }
         *   },
         *   "Department": "Engineering",
         *   "Salary": 120000
         * }
         *
         * THREE LEVELS of nesting:
         *   Level 1 → ContactInfo
         *   Level 2 → ContactInfo.Address
         *   Level 3 → ContactInfo.Address.PreviousAddresses (an Array!)
         *
         * RULE: For every level of nesting in the JSON,
         *       add one more StructType inside the parent StructField.
         */

        printSection("PART 2 — Deeply Nested JSON: employee.json");

        // ── Step 1: Define the nested schema ─────────────────────
        /*
         * Read this schema from the INSIDE OUT:
         *
         * Innermost: PreviousAddresses is an ARRAY of Strings
         *   → DataTypes.createArrayType(DataTypes.StringType)
         *
         * Next out: Address struct contains Street, City, State, ZipCode, PreviousAddresses
         *   → DataTypes.createStructType(new StructField[]{ ... })
         *
         * Next out: ContactInfo struct contains Email, Phone, and the Address struct
         *   → DataTypes.createStructType(new StructField[]{ ... Address struct inside ... })
         *
         * Outermost: The Employee record contains EmployeeID, Name, ContactInfo, Department, Salary
         */

        StructType employeeSchema = DataTypes.createStructType(new StructField[]{

                // Level 1 flat fields
                DataTypes.createStructField("EmployeeID", DataTypes.IntegerType, true),

                // Level 1 nested: Name object
                DataTypes.createStructField("Name",
                        DataTypes.createStructType(new StructField[]{
                                DataTypes.createStructField("First", DataTypes.StringType, true),
                                DataTypes.createStructField("Last",  DataTypes.StringType, true)
                        }), true),

                // Level 1 nested: ContactInfo object (which itself contains Address)
                DataTypes.createStructField("ContactInfo",
                        DataTypes.createStructType(new StructField[]{
                                DataTypes.createStructField("Email", DataTypes.StringType, true),
                                DataTypes.createStructField("Phone", DataTypes.StringType, true),

                                // Level 2 nested: Address object (inside ContactInfo)
                                DataTypes.createStructField("Address",
                                        DataTypes.createStructType(new StructField[]{
                                                DataTypes.createStructField("Street",  DataTypes.StringType, true),
                                                DataTypes.createStructField("City",    DataTypes.StringType, true),
                                                DataTypes.createStructField("State",   DataTypes.StringType, true),
                                                DataTypes.createStructField("ZipCode", DataTypes.StringType, true),

                                                // Level 3: PreviousAddresses is an ARRAY of Strings
                                                // createArrayType(elementType) — what type is inside the array?
                                                DataTypes.createStructField("PreviousAddresses",
                                                        DataTypes.createArrayType(DataTypes.StringType), true)
                                        }), true)
                        }), true),

                // Level 1 flat fields
                DataTypes.createStructField("Department", DataTypes.StringType, true),
                DataTypes.createStructField("Salary",     DataTypes.DoubleType,  true)
        });

        // ── Step 2: Read the JSON file ────────────────────────────

        String empJsonPath = "C:\\Datasets\\employee.json";

        Dataset<Row> employeeDF = spark
                .read()
                .option("multiline", "true")
                .schema(employeeSchema)
                .json(empJsonPath);

        // ── Step 3: Inspect the schema ───────────────────────────

        System.out.println("--- printSchema() — notice 3 levels of nesting ---");
        employeeDF.printSchema();
        /*
         * root
         *  |-- EmployeeID: integer (nullable = true)
         *  |-- Name: struct (nullable = true)
         *  |    |-- First: string (nullable = true)
         *  |    |-- Last: string (nullable = true)
         *  |-- ContactInfo: struct (nullable = true)
         *  |    |-- Email: string (nullable = true)
         *  |    |-- Phone: string (nullable = true)
         *  |    |-- Address: struct (nullable = true)
         *  |    |    |-- Street: string (nullable = true)
         *  |    |    |-- City: string (nullable = true)
         *  |    |    |-- State: string (nullable = true)
         *  |    |    |-- ZipCode: string (nullable = true)
         *  |    |    |-- PreviousAddresses: array (nullable = true)
         *  |    |    |    |-- element: string (containsNull = true)
         *  |-- Department: string (nullable = true)
         *  |-- Salary: double (nullable = true)
         *
         * The indentation in printSchema() shows the depth of nesting.
         * Three levels deep: ContactInfo → Address → PreviousAddresses
         */

        System.out.println("\n--- show() — nested fields appear as structs and arrays ---");
        employeeDF.show(false);

        // ── Step 4: Access deeply nested fields ──────────────────
        /*
         * Chain dots for each level:
         *   Name.First                          → 1 level deep
         *   ContactInfo.Email                   → 1 level deep
         *   ContactInfo.Address.City            → 2 levels deep
         *   ContactInfo.Address.PreviousAddresses → 2 levels deep (array)
         */

        System.out.println("\n--- Select specific nested fields ---");
        employeeDF.select(
                col("EmployeeID"),
                col("Name.First").alias("FirstName"),
                col("Name.Last").alias("LastName"),
                col("ContactInfo.Email").alias("Email"),
                col("ContactInfo.Address.City").alias("City"),
                col("ContactInfo.Address.State").alias("State"),
                col("Department"),
                col("Salary")
        ).show(false);
        /*
         * +----------+---------+--------+--------------------+--------+-----+-----------+--------+
         * |EmployeeID|FirstName|LastName|               Email|    City|State| Department|  Salary|
         * +----------+---------+--------+--------------------+--------+-----+-----------+--------+
         * |       123|     John|     Doe|john.doe@example.com|New York|   NY|Engineering|120000.0|
         * +----------+---------+--------+--------------------+--------+-----+-----------+--------+
         */

        // ── Step 5: Working with Arrays ──────────────────────────
        /*
         * PreviousAddresses is an ARRAY column.
         * You can't access it with a simple dot — you need to EXPLODE it.
         *
         * explode(arrayCol) → creates one row per element in the array.
         * Use it when you want to process each array element as its own row.
         *
         * Before explode:
         *   EmployeeID | PreviousAddresses
         *   123        | [456 Elm St, 789 Oak St]       ← one row, two values
         *
         * After explode:
         *   EmployeeID | PreviousAddress
         *   123        | 456 Elm St                     ← two rows, one value each
         *   123        | 789 Oak St
         */

        System.out.println("\n--- explode() — turn array elements into individual rows ---");
        employeeDF.select(
                col("EmployeeID"),
                col("Name.First").alias("FirstName"),
                explode(col("ContactInfo.Address.PreviousAddresses")).alias("PreviousAddress")
        ).show(false);
        /*
         * +----------+---------+---------------+
         * |EmployeeID|FirstName|PreviousAddress|
         * +----------+---------+---------------+
         * |       123|     John|    456 Elm St  |
         * |       123|     John|    789 Oak St  |
         * +----------+---------+---------------+
         */

        // ── Step 6: Same operations in SQL ───────────────────────

        employeeDF.createOrReplaceTempView("Employees");

        System.out.println("\n--- Nested fields in SQL using dot notation ---");
        spark.sql(
                "SELECT EmployeeID, " +
                        "Name.First AS FirstName, " +
                        "Name.Last AS LastName, " +
                        "ContactInfo.Email AS Email, " +
                        "ContactInfo.Address.City AS City, " +
                        "Department, Salary " +
                        "FROM Employees"
        ).show(false);

        System.out.println("\n--- explode() in SQL ---");
        spark.sql(
                "SELECT EmployeeID, Name.First AS FirstName, " +
                        "EXPLODE(ContactInfo.Address.PreviousAddresses) AS PreviousAddress " +
                        "FROM Employees"
        ).show(false);

        // ============================================================
        // SUMMARY
        // ============================================================

        printSection("SUMMARY — JSON vs CSV, and handling nesting");
        System.out.println(
                "  CSV vs JSON:\n" +
                        "    CSV    → flat rows, all same columns, all strings\n" +
                        "    JSON   → flexible, nested objects, arrays allowed\n" +
                        "\n" +
                        "  Reading JSON:\n" +
                        "    spark.read()\n" +
                        "         .option(\"multiline\", \"true\")  → if JSON is pretty-printed\n" +
                        "         .schema(yourSchema)\n" +
                        "         .json(filePath)\n" +
                        "\n" +
                        "  Nested objects in schema:\n" +
                        "    DataTypes.createStructField(\"Address\",\n" +
                        "        DataTypes.createStructType(new StructField[]{\n" +
                        "            ... inner fields ...\n" +
                        "        }), true)\n" +
                        "\n" +
                        "  Array columns in schema:\n" +
                        "    DataTypes.createArrayType(DataTypes.StringType)\n" +
                        "\n" +
                        "  Accessing nested fields:\n" +
                        "    col(\"ContactInfo.Address.City\")   → dot notation\n" +
                        "    SQL: ContactInfo.Address.City     → same dot notation\n" +
                        "\n" +
                        "  Working with arrays:\n" +
                        "    explode(col(\"arrayColumn\"))        → one row per element\n" +
                        "    SQL: EXPLODE(arrayColumn)          → same in SQL\n" +
                        "\n" +
                        "  TIP — use printSchema() as your guide:\n" +
                        "    The indentation in printSchema() shows exactly\n" +
                        "    how many levels of nesting you have.\n" +
                        "    One level of indent = one StructType in your schema."
        );
        System.out.println("\n>>> PAUSED — Check http://localhost:4040 — Press ENTER to exit <<<");
        try (final var scanner = new Scanner(System.in)) {
            scanner.nextLine();
        }
    }

    private static void printSection(String title) {
        System.out.println("\n");
        System.out.println("=".repeat(65));
        System.out.println("  " + title);
        System.out.println("=".repeat(65));
    }
}