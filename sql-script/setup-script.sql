-- ============================================================
--  POSTGRESQL SETUP SCRIPT
--  Beacon Solutions, Inc.
--  Run this in pgAdmin or psql BEFORE running the Spark job
-- ============================================================

-- ── STEP 1: Create the database ──────────────────────────────
-- Run this connected to the default 'postgres' database

CREATE DATABASE beacon_retail;

-- ── STEP 2: Connect to the new database ──────────────────────
-- In psql:     \c beacon_retail
-- In pgAdmin:  open a new Query Tool on beacon_retail

-- ── STEP 3: Create the schema and tables ─────────────────────

CREATE SCHEMA IF NOT EXISTS retail;

-- Products table
CREATE TABLE retail.products (
                                 product_id      SERIAL PRIMARY KEY,
                                 product_name    VARCHAR(100) NOT NULL,
                                 category        VARCHAR(50),
                                 unit_price      DECIMAL(10, 2),
                                 stock_quantity  INTEGER
);

-- Customers table
CREATE TABLE retail.customers (
                                  customer_id     SERIAL PRIMARY KEY,
                                  first_name      VARCHAR(50) NOT NULL,
                                  last_name       VARCHAR(50) NOT NULL,
                                  email           VARCHAR(100),
                                  city            VARCHAR(50),
                                  country         VARCHAR(50)
);

-- Orders table
CREATE TABLE retail.orders (
                               order_id        SERIAL PRIMARY KEY,
                               customer_id     INTEGER REFERENCES retail.customers(customer_id),
                               product_id      INTEGER REFERENCES retail.products(product_id),
                               order_date      DATE,
                               quantity        INTEGER,
                               total_amount    DECIMAL(10, 2),
                               status          VARCHAR(20) DEFAULT 'Pending'
);

-- Sales summary table (Spark will CREATE and WRITE this)
-- No need to create this — Spark will create it automatically
-- It is listed here just for reference:
-- retail.sales_summary (category, total_revenue, total_orders, avg_order_value)

-- ── STEP 4: Insert sample data ───────────────────────────────

-- Products (20 products across 4 categories)
INSERT INTO retail.products (product_name, category, unit_price, stock_quantity) VALUES
                                                                                     ('Laptop Pro 15',        'Electronics',  1299.99,  45),
                                                                                     ('Wireless Mouse',       'Electronics',    29.99, 200),
                                                                                     ('USB-C Hub',            'Electronics',    49.99, 150),
                                                                                     ('Mechanical Keyboard',  'Electronics',    89.99,  80),
                                                                                     ('4K Monitor',           'Electronics',   399.99,  30),
                                                                                     ('Office Chair',         'Furniture',     249.99,  25),
                                                                                     ('Standing Desk',        'Furniture',     499.99,  15),
                                                                                     ('Desk Lamp',            'Furniture',      39.99, 100),
                                                                                     ('Bookshelf',            'Furniture',     179.99,  20),
                                                                                     ('Filing Cabinet',       'Furniture',     129.99,  35),
                                                                                     ('Python Programming',   'Books',          49.99, 500),
                                                                                     ('Data Engineering',     'Books',          59.99, 300),
                                                                                     ('SQL Mastery',          'Books',          39.99, 400),
                                                                                     ('Cloud Architecture',   'Books',          54.99, 250),
                                                                                     ('Machine Learning',     'Books',          64.99, 180),
                                                                                     ('Notebook Set',         'Stationery',      9.99, 800),
                                                                                     ('Ballpoint Pens 12pk',  'Stationery',      5.99, 600),
                                                                                     ('Sticky Notes 5pk',     'Stationery',      4.99, 700),
                                                                                     ('Whiteboard Markers',   'Stationery',      7.99, 400),
                                                                                     ('Stapler',              'Stationery',     12.99, 300);

-- Customers (15 customers from different cities)
INSERT INTO retail.customers (first_name, last_name, email, city, country) VALUES
                                                                               ('Maria',    'Santos',    'maria.santos@email.com',    'Manila',        'Philippines'),
                                                                               ('James',    'Reyes',     'james.reyes@email.com',     'Cebu',          'Philippines'),
                                                                               ('Ana',      'Cruz',      'ana.cruz@email.com',        'Davao',         'Philippines'),
                                                                               ('Carlos',   'Mendoza',   'carlos.mendoza@email.com',  'Quezon City',   'Philippines'),
                                                                               ('Sofia',    'Garcia',    'sofia.garcia@email.com',    'Makati',        'Philippines'),
                                                                               ('Miguel',   'Torres',    'miguel.torres@email.com',   'Pasig',         'Philippines'),
                                                                               ('Isabella', 'Lim',       'isabella.lim@email.com',    'Taguig',        'Philippines'),
                                                                               ('Rafael',   'Tan',       'rafael.tan@email.com',      'Mandaluyong',   'Philippines'),
                                                                               ('Camila',   'Villanueva','camila.v@email.com',        'Paranaque',     'Philippines'),
                                                                               ('Diego',    'Ramos',     'diego.ramos@email.com',     'Pasay',         'Philippines'),
                                                                               ('Valentina','Flores',    'valentina.f@email.com',     'Muntinlupa',    'Philippines'),
                                                                               ('Sebastian','Aquino',    'sebastian.a@email.com',     'Las Pinas',     'Philippines'),
                                                                               ('Lucia',    'Bautista',  'lucia.b@email.com',         'Marikina',      'Philippines'),
                                                                               ('Mateo',    'Dela Cruz', 'mateo.dc@email.com',        'Caloocan',      'Philippines'),
                                                                               ('Valeria',  'Castillo',  'valeria.c@email.com',       'Antipolo',      'Philippines');

-- Orders (50 orders spread across customers, products and dates)
INSERT INTO retail.orders (customer_id, product_id, order_date, quantity, total_amount, status) VALUES
                                                                                                    (1,  1,  '2024-01-05',  1,  1299.99, 'Delivered'),
                                                                                                    (1,  2,  '2024-01-05',  2,    59.98, 'Delivered'),
                                                                                                    (2,  5,  '2024-01-08',  1,   399.99, 'Delivered'),
                                                                                                    (2,  11, '2024-01-08',  3,   149.97, 'Delivered'),
                                                                                                    (3,  6,  '2024-01-10',  2,   499.98, 'Delivered'),
                                                                                                    (3,  3,  '2024-01-10',  1,    49.99, 'Delivered'),
                                                                                                    (4,  12, '2024-01-12',  2,   119.98, 'Delivered'),
                                                                                                    (4,  4,  '2024-01-15',  1,    89.99, 'Delivered'),
                                                                                                    (5,  7,  '2024-01-18',  1,   499.99, 'Delivered'),
                                                                                                    (5,  16, '2024-01-18',  5,    49.95, 'Delivered'),
                                                                                                    (6,  13, '2024-01-20',  4,   159.96, 'Shipped'),
                                                                                                    (6,  1,  '2024-01-22',  1,  1299.99, 'Shipped'),
                                                                                                    (7,  8,  '2024-02-01',  3,   119.97, 'Delivered'),
                                                                                                    (7,  17, '2024-02-01',  10,   59.90, 'Delivered'),
                                                                                                    (8,  14, '2024-02-05',  2,   109.98, 'Delivered'),
                                                                                                    (8,  5,  '2024-02-08',  2,   799.98, 'Delivered'),
                                                                                                    (9,  15, '2024-02-10',  1,    64.99, 'Delivered'),
                                                                                                    (9,  9,  '2024-02-12',  1,   179.99, 'Shipped'),
                                                                                                    (10, 2,  '2024-02-14',  3,    89.97, 'Delivered'),
                                                                                                    (10, 3,  '2024-02-14',  2,    99.98, 'Delivered'),
                                                                                                    (11, 10, '2024-02-18',  1,   129.99, 'Pending'),
                                                                                                    (11, 11, '2024-02-20',  5,   249.95, 'Delivered'),
                                                                                                    (12, 4,  '2024-02-22',  2,   179.98, 'Delivered'),
                                                                                                    (12, 6,  '2024-02-25',  1,   249.99, 'Shipped'),
                                                                                                    (13, 12, '2024-03-01',  3,   179.97, 'Delivered'),
                                                                                                    (13, 16, '2024-03-01',  8,    79.92, 'Delivered'),
                                                                                                    (14, 1,  '2024-03-05',  1,  1299.99, 'Delivered'),
                                                                                                    (14, 2,  '2024-03-05',  1,    29.99, 'Delivered'),
                                                                                                    (15, 7,  '2024-03-08',  1,   499.99, 'Pending'),
                                                                                                    (15, 13, '2024-03-10',  2,    79.98, 'Delivered'),
                                                                                                    (1,  5,  '2024-03-12',  1,   399.99, 'Delivered'),
                                                                                                    (2,  8,  '2024-03-15',  2,    79.98, 'Delivered'),
                                                                                                    (3,  15, '2024-03-18',  2,   129.98, 'Shipped'),
                                                                                                    (4,  9,  '2024-03-20',  1,   179.99, 'Delivered'),
                                                                                                    (5,  10, '2024-03-22',  2,   259.98, 'Delivered'),
                                                                                                    (6,  14, '2024-03-25',  1,    54.99, 'Pending'),
                                                                                                    (7,  11, '2024-04-01',  4,   199.96, 'Delivered'),
                                                                                                    (8,  16, '2024-04-03',  6,    59.94, 'Delivered'),
                                                                                                    (9,  1,  '2024-04-05',  1,  1299.99, 'Shipped'),
                                                                                                    (10, 6,  '2024-04-08',  1,   249.99, 'Delivered'),
                                                                                                    (11, 3,  '2024-04-10',  3,   149.97, 'Delivered'),
                                                                                                    (12, 17, '2024-04-12',  5,    29.95, 'Delivered'),
                                                                                                    (13, 4,  '2024-04-15',  1,    89.99, 'Delivered'),
                                                                                                    (14, 12, '2024-04-18',  2,   119.98, 'Delivered'),
                                                                                                    (15, 5,  '2024-04-20',  1,   399.99, 'Pending'),
                                                                                                    (1,  13, '2024-04-22',  3,   119.97, 'Delivered'),
                                                                                                    (2,  7,  '2024-04-25',  1,   499.99, 'Delivered'),
                                                                                                    (3,  14, '2024-04-28',  2,   109.98, 'Shipped'),
                                                                                                    (4,  15, '2024-05-01',  1,    64.99, 'Delivered'),
                                                                                                    (5,  11, '2024-05-03',  6,   299.94, 'Delivered');

-- ── STEP 5: Verify the data ───────────────────────────────────

SELECT 'products'  AS table_name, COUNT(*) AS row_count FROM retail.products
UNION ALL
SELECT 'customers' AS table_name, COUNT(*) AS row_count FROM retail.customers
UNION ALL
SELECT 'orders'    AS table_name, COUNT(*) AS row_count FROM retail.orders;

-- Expected:
-- products  | 20
-- customers | 15
-- orders    | 50

-- ── Quick preview ─────────────────────────────────────────────
SELECT
    o.order_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    p.product_name,
    p.category,
    o.quantity,
    o.total_amount,
    o.status
FROM retail.orders o
         JOIN retail.customers c ON o.customer_id = c.customer_id
         JOIN retail.products  p ON o.product_id  = p.product_id
ORDER BY o.order_date
    LIMIT 10;