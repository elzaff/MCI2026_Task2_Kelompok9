/* =====================================================================
   MCI 2026 - Task 2 Modul 2 & 3
   Dataset  : E-Commerce Orders API
   Source   : http://96.9.212.102:8000/orders
   Purpose  : ClickHouse DDL + Metabase Questions SQL

   Notes:
   - Pipeline Airflow + PySpark membuat dan mengisi tabel ini otomatis.
   - File ini disiapkan untuk dokumentasi tugas dan referensi query Metabase.
   - Jalankan bagian DDL jika ingin membuat tabel secara manual dari ClickHouse.
   ===================================================================== */


/* =====================================================================
   1. DATABASE
   ===================================================================== */

CREATE DATABASE IF NOT EXISTS analytics;


/* =====================================================================
   2. DDL TABLES
   ===================================================================== */

CREATE TABLE IF NOT EXISTS analytics.orders_order_items (
    order_id UInt64,
    user_id UInt64,
    order_number UInt32,
    order_dow UInt8,
    order_hour_of_day UInt8,
    days_since_prior_order Nullable(Float64),
    eval_set String,
    product_id Nullable(UInt64),
    product_name Nullable(String),
    aisle_id Nullable(UInt32),
    aisle Nullable(String),
    department_id Nullable(UInt32),
    department Nullable(String),
    add_to_cart_order Nullable(UInt32),
    reordered Nullable(UInt8),
    ingested_at DateTime
) ENGINE = MergeTree()
ORDER BY order_id;

CREATE TABLE IF NOT EXISTS analytics.orders_product_summary (
    product_id UInt64,
    product_name String,
    aisle String,
    department String,
    total_order_lines UInt64,
    unique_orders UInt64,
    reordered_count UInt64,
    reorder_rate Float64,
    avg_add_to_cart_order Float64
) ENGINE = MergeTree()
ORDER BY total_order_lines;

CREATE TABLE IF NOT EXISTS analytics.orders_department_summary (
    department_id UInt32,
    department String,
    total_order_lines UInt64,
    unique_orders UInt64,
    unique_products UInt64,
    reordered_count UInt64,
    reorder_rate Float64
) ENGINE = MergeTree()
ORDER BY total_order_lines;

CREATE TABLE IF NOT EXISTS analytics.orders_hourly_summary (
    order_dow UInt8,
    order_hour_of_day UInt8,
    total_orders UInt64,
    total_order_lines UInt64,
    unique_users UInt64,
    avg_items_per_order Float64
) ENGINE = MergeTree()
ORDER BY (order_dow, order_hour_of_day);


/* =====================================================================
   3. VALIDATION QUERIES
   ===================================================================== */

-- Cek tabel Orders sudah terbentuk.
SHOW TABLES FROM analytics LIKE 'orders%';

-- Cek jumlah baris per tabel.
SELECT 'orders_order_items' AS table_name, count() AS total_rows
FROM analytics.orders_order_items
UNION ALL
SELECT 'orders_product_summary', count()
FROM analytics.orders_product_summary
UNION ALL
SELECT 'orders_department_summary', count()
FROM analytics.orders_department_summary
UNION ALL
SELECT 'orders_hourly_summary', count()
FROM analytics.orders_hourly_summary;

-- Cek sampel data detail.
SELECT *
FROM analytics.orders_order_items
LIMIT 20;


/* =====================================================================
   4. METABASE QUESTION QUERIES
   ===================================================================== */

/* ---------------------------------------------------------------------
   Q1 - Executive KPI Summary
   Metabase visualization: Table
   Metrics/columns: total_orders, total_users, total_order_lines,
   total_unique_products, avg_items_per_order, reorder_rate_percent.
   Insight: ukuran dataset, total transaksi unik, user unik, dan item line.
   --------------------------------------------------------------------- */

SELECT
    count(DISTINCT order_id) AS total_orders,
    count(DISTINCT user_id) AS total_users,
    count() AS total_order_lines,
    count(DISTINCT product_id) AS total_unique_products,
    round(count() / count(DISTINCT order_id), 2) AS avg_items_per_order,
    round(avg(coalesce(reordered, 0)) * 100, 2) AS reorder_rate_percent
FROM analytics.orders_order_items
WHERE product_id IS NOT NULL;


/* ---------------------------------------------------------------------
   Q1A - KPI Total Orders
   Metabase visualization: Number
   Metric: total_orders.
   --------------------------------------------------------------------- */

SELECT count(DISTINCT order_id) AS total_orders
FROM analytics.orders_order_items
WHERE product_id IS NOT NULL;


/* ---------------------------------------------------------------------
   Q1B - KPI Total Users
   Metabase visualization: Number
   Metric: total_users.
   --------------------------------------------------------------------- */

SELECT count(DISTINCT user_id) AS total_users
FROM analytics.orders_order_items
WHERE product_id IS NOT NULL;


/* ---------------------------------------------------------------------
   Q1C - KPI Total Order Lines
   Metabase visualization: Number
   Metric: total_order_lines.
   --------------------------------------------------------------------- */

SELECT count() AS total_order_lines
FROM analytics.orders_order_items
WHERE product_id IS NOT NULL;


/* ---------------------------------------------------------------------
   Q1D - KPI Unique Products
   Metabase visualization: Number
   Metric: total_unique_products.
   --------------------------------------------------------------------- */

SELECT count(DISTINCT product_id) AS total_unique_products
FROM analytics.orders_order_items
WHERE product_id IS NOT NULL;


/* ---------------------------------------------------------------------
   Q1E - KPI Average Items per Order
   Metabase visualization: Number
   Metric: avg_items_per_order.
   --------------------------------------------------------------------- */

SELECT round(count() / count(DISTINCT order_id), 2) AS avg_items_per_order
FROM analytics.orders_order_items
WHERE product_id IS NOT NULL;


/* ---------------------------------------------------------------------
   Q1F - KPI Overall Reorder Rate
   Metabase visualization: Number or Gauge
   Metric: reorder_rate_percent.
   --------------------------------------------------------------------- */

SELECT round(avg(coalesce(reordered, 0)) * 100, 2) AS reorder_rate_percent
FROM analytics.orders_order_items
WHERE product_id IS NOT NULL;


/* ---------------------------------------------------------------------
   Q2 - Top 15 Products by Order Lines
   Metabase visualization: Bar
   X-axis: product_name.
   Y-axis metrics: total_order_lines, unique_orders.
   Optional series/grouping: department.
   Insight: produk yang paling sering muncul pada keranjang pelanggan.
   --------------------------------------------------------------------- */

SELECT
    product_name,
    department,
    aisle,
    total_order_lines,
    unique_orders,
    round(reorder_rate * 100, 2) AS reorder_rate_percent,
    round(avg_add_to_cart_order, 2) AS avg_add_to_cart_order
FROM analytics.orders_product_summary
ORDER BY total_order_lines DESC
LIMIT 15;


/* ---------------------------------------------------------------------
   Q3 - Most Loyal Products by Reorder Rate
   Metabase visualization: Bar or Table
   X-axis: product_name.
   Y-axis metrics: reorder_rate_percent, total_order_lines.
   Optional series/grouping: department.
   Insight: produk yang punya loyalitas tinggi, bukan sekadar volume tinggi.
   Filter minimum order line dipakai agar produk langka tidak terlihat bias.
   --------------------------------------------------------------------- */

SELECT
    product_name,
    department,
    aisle,
    total_order_lines,
    reordered_count,
    round(reorder_rate * 100, 2) AS reorder_rate_percent
FROM analytics.orders_product_summary
WHERE total_order_lines >= 3
ORDER BY reorder_rate DESC, total_order_lines DESC
LIMIT 15;


/* ---------------------------------------------------------------------
   Q4 - Department Contribution
   Metabase visualization: Pie
   Dimension/slice: department.
   Metric: total_order_lines.
   Optional tooltip metrics: share_of_order_lines_percent, reorder_rate_percent.
   Insight: department mana yang mendominasi isi order.
   --------------------------------------------------------------------- */

SELECT
    department,
    total_order_lines,
    unique_orders,
    unique_products,
    round(total_order_lines * 100.0 / sum(total_order_lines) OVER (), 2) AS share_of_order_lines_percent,
    round(reorder_rate * 100, 2) AS reorder_rate_percent
FROM analytics.orders_department_summary
ORDER BY total_order_lines DESC;


/* ---------------------------------------------------------------------
   Q5 - Department Quality Matrix
   Metabase visualization: Scatter
   X-axis: total_order_lines
   Y-axis metric: reorder_rate_percent
   Bubble size metric: unique_products
   Label/detail: department
   Insight: membedakan department besar yang repeatable vs department niche.
   --------------------------------------------------------------------- */

SELECT
    department,
    total_order_lines,
    unique_orders,
    unique_products,
    round(reorder_rate * 100, 2) AS reorder_rate_percent
FROM analytics.orders_department_summary
ORDER BY total_order_lines DESC;


/* ---------------------------------------------------------------------
   Q6 - Order Traffic by Hour of Day
   Metabase visualization: Line or Area
   X-axis: order_hour_of_day.
   Y-axis metrics: total_orders, total_order_lines.
   Optional second metric: avg_items_per_order.
   Insight: jam tersibuk untuk order, berguna untuk strategi promo/operasional.
   --------------------------------------------------------------------- */

WITH hourly AS (
    SELECT
        order_hour_of_day,
        sum(total_orders) AS orders_count,
        sum(total_order_lines) AS order_lines_count
    FROM analytics.orders_hourly_summary
    GROUP BY order_hour_of_day
)
SELECT
    order_hour_of_day,
    orders_count AS total_orders,
    order_lines_count AS total_order_lines,
    round(order_lines_count / orders_count, 2) AS avg_items_per_order
FROM hourly
ORDER BY order_hour_of_day;

/* ---------------------------------------------------------------------
   Q7 - Weekly Order by Day and Hour
   Metabase visualization: Table or Bar
   Note: Pivot Table di Metabase hanya didukung untuk question yang dibuat
   melalui Query Builder, bukan Native SQL.
   Table columns: day_name, order_hour_of_day.
   Metrics: total_orders, total_order_lines, avg_items_per_order.
   Insight: kombinasi hari dan jam dengan demand tertinggi.
   --------------------------------------------------------------------- */

SELECT
    order_dow,
    multiIf(
        order_dow = 0, 'Sunday',
        order_dow = 1, 'Monday',
        order_dow = 2, 'Tuesday',
        order_dow = 3, 'Wednesday',
        order_dow = 4, 'Thursday',
        order_dow = 5, 'Friday',
        order_dow = 6, 'Saturday',
        'Unknown'
    ) AS day_name,
    order_hour_of_day,
    total_orders,
    total_order_lines,
    round(avg_items_per_order, 2) AS avg_items_per_order
FROM analytics.orders_hourly_summary
ORDER BY order_dow, order_hour_of_day;


/* ---------------------------------------------------------------------
   Q8 - Basket Size Distribution
   Metabase visualization: Bar
   X-axis: basket_size.
   Y-axis metric: total_orders.
   Insight: seberapa besar keranjang belanja per order.
   --------------------------------------------------------------------- */

WITH basket AS (
    SELECT
        order_id,
        count() AS basket_size
    FROM analytics.orders_order_items
    WHERE product_id IS NOT NULL
    GROUP BY order_id
)
SELECT
    basket_size,
    count() AS total_orders
FROM basket
GROUP BY basket_size
ORDER BY basket_size;


/* ---------------------------------------------------------------------
   Q9 - Reorder vs First-Time Product Lines
   Metabase visualization: Pie or Bar
   Dimension/slice or X-axis: product_line_type.
   Metric: total_order_lines.
   Optional metric: percentage.
   Insight: proporsi item repeat purchase dibanding item baru.
   --------------------------------------------------------------------- */

SELECT
    if(coalesce(reordered, 0) = 1, 'Reordered', 'First-time') AS product_line_type,
    count() AS total_order_lines,
    round(count() * 100.0 / sum(count()) OVER (), 2) AS percentage
FROM analytics.orders_order_items
WHERE product_id IS NOT NULL
GROUP BY product_line_type
ORDER BY total_order_lines DESC;


/* ---------------------------------------------------------------------
   Q10 - Reorder Rate by Order Number Bucket
   Metabase visualization: Line
   X-axis: order_number_bucket.
   Y-axis metrics: reorder_rate_percent, total_orders.
   Insight: apakah pelanggan makin sering reorder saat order_number makin tinggi.
   --------------------------------------------------------------------- */

SELECT
    multiIf(
        order_number = 1, '01. First Order',
        order_number BETWEEN 2 AND 5, '02. Order 2-5',
        order_number BETWEEN 6 AND 10, '03. Order 6-10',
        order_number BETWEEN 11 AND 20, '04. Order 11-20',
        order_number BETWEEN 21 AND 50, '05. Order 21-50',
        '06. Order 51+'
    ) AS order_number_bucket,
    count(DISTINCT order_id) AS total_orders,
    count() AS total_order_lines,
    round(avg(coalesce(reordered, 0)) * 100, 2) AS reorder_rate_percent
FROM analytics.orders_order_items
WHERE product_id IS NOT NULL
GROUP BY order_number_bucket
ORDER BY order_number_bucket;


/* ---------------------------------------------------------------------
   Q11 - Days Since Prior Order Distribution
   Metabase visualization: Bar
   X-axis: days_since_prior_order_bucket.
   Y-axis metrics: total_orders, total_order_lines.
   Optional metric: reorder_rate_percent.
   Insight: interval repeat order pelanggan.
   --------------------------------------------------------------------- */

SELECT
    multiIf(
        days_since_prior_order IS NULL, 'Unknown / First Order',
        days_since_prior_order = 0, 'Same Day',
        days_since_prior_order BETWEEN 1 AND 3, '1-3 Days',
        days_since_prior_order BETWEEN 4 AND 7, '4-7 Days',
        days_since_prior_order BETWEEN 8 AND 14, '8-14 Days',
        days_since_prior_order BETWEEN 15 AND 30, '15-30 Days',
        '30+ Days'
    ) AS days_since_prior_order_bucket,
    count(DISTINCT order_id) AS total_orders,
    count() AS total_order_lines,
    round(avg(coalesce(reordered, 0)) * 100, 2) AS reorder_rate_percent
FROM analytics.orders_order_items
WHERE product_id IS NOT NULL
GROUP BY days_since_prior_order_bucket
ORDER BY
    indexOf(
        ['Unknown / First Order', 'Same Day', '1-3 Days', '4-7 Days', '8-14 Days', '15-30 Days', '30+ Days'],
        days_since_prior_order_bucket
    );


/* ---------------------------------------------------------------------
   Q12 - Top Aisles inside Top Departments
   Metabase visualization: Table or Bar
   X-axis: aisle.
   Y-axis metrics: total_order_lines, unique_orders, unique_products.
   Series/grouping: department.
   Insight: drill-down dari department ke aisle yang mendorong volume order.
   --------------------------------------------------------------------- */

SELECT
    department,
    aisle,
    count() AS total_order_lines,
    count(DISTINCT order_id) AS unique_orders,
    count(DISTINCT product_id) AS unique_products,
    round(avg(coalesce(reordered, 0)) * 100, 2) AS reorder_rate_percent
FROM analytics.orders_order_items
WHERE product_id IS NOT NULL
GROUP BY department, aisle
ORDER BY total_order_lines DESC
LIMIT 25;


/* ---------------------------------------------------------------------
   Q13 - Cart Position Analysis
   Metabase visualization: Bar
   X-axis: product_name.
   Y-axis metrics: avg_add_to_cart_order, reorder_rate_percent.
   Optional tooltip metric: total_order_lines.
   Insight: produk yang sering masuk cart paling awal biasanya high intent/staple.
   --------------------------------------------------------------------- */

SELECT
    product_name,
    department,
    total_order_lines,
    round(avg_add_to_cart_order, 2) AS avg_add_to_cart_order,
    round(reorder_rate * 100, 2) AS reorder_rate_percent
FROM analytics.orders_product_summary
WHERE total_order_lines >= 3
ORDER BY avg_add_to_cart_order ASC, total_order_lines DESC
LIMIT 15;


/* ---------------------------------------------------------------------
   Q14 - User Order Frequency Snapshot
   Metabase visualization: Table or Bar
   X-axis: user_id.
   Y-axis metrics: total_order_lines, total_orders, avg_items_per_order.
   Optional metric: reorder_rate_percent.
   Insight: user yang muncul dengan banyak order lines pada batch data.
   --------------------------------------------------------------------- */

SELECT
    user_id,
    count(DISTINCT order_id) AS total_orders,
    count() AS total_order_lines,
    count(DISTINCT product_id) AS unique_products,
    round(avg(coalesce(reordered, 0)) * 100, 2) AS reorder_rate_percent,
    round(count() / count(DISTINCT order_id), 2) AS avg_items_per_order
FROM analytics.orders_order_items
WHERE product_id IS NOT NULL
GROUP BY user_id
ORDER BY total_orders DESC, total_order_lines DESC
LIMIT 20;


/* ---------------------------------------------------------------------
   Q15 - Dashboard Refresh Timestamp
   Metabase visualization: Detail or Table
   Fields: latest_ingested_at, earliest_ingested_at,
   total_orders_in_current_load, total_order_lines_in_current_load.
   Insight: memastikan dashboard memakai data pipeline terbaru.
   --------------------------------------------------------------------- */

SELECT
    max(ingested_at) AS latest_ingested_at,
    min(ingested_at) AS earliest_ingested_at,
    count(DISTINCT order_id) AS total_orders_in_current_load,
    count() AS total_order_lines_in_current_load
FROM analytics.orders_order_items;


/* ---------------------------------------------------------------------
   Q16 - Reorder Rate Gauge
   Metabase visualization: Gauge
   Metric/value: reorder_rate_percent.
   Gauge scale: min 0, max 100.
   Insight: indikator cepat seberapa besar item merupakan repeat purchase.
   --------------------------------------------------------------------- */

SELECT round(avg(coalesce(reordered, 0)) * 100, 2) AS reorder_rate_percent
FROM analytics.orders_order_items
WHERE product_id IS NOT NULL;


/* ---------------------------------------------------------------------
   Q17 - Department to Aisle Flow
   Metabase visualization: Sankey
   Source axis: source.
   Target axis: target.
   Flow metric/value: total_order_lines.
   Insight: aliran kontribusi dari department ke aisle.
   --------------------------------------------------------------------- */

SELECT
    department AS source,
    aisle AS target,
    count() AS total_order_lines
FROM analytics.orders_order_items
WHERE product_id IS NOT NULL
GROUP BY source, target
ORDER BY total_order_lines DESC
LIMIT 30;


/* ---------------------------------------------------------------------
   Q18 - Basket Size by Day Distribution
   Metabase visualization: Box Plot
   Category axis: day_name.
   Value metric: basket_size.
   Insight: variasi ukuran keranjang untuk setiap hari order.
   --------------------------------------------------------------------- */

WITH basket AS (
    SELECT
        order_id,
        any(order_dow) AS order_dow,
        count() AS basket_size
    FROM analytics.orders_order_items
    WHERE product_id IS NOT NULL
    GROUP BY order_id
)
SELECT
    multiIf(
        order_dow = 0, 'Sunday',
        order_dow = 1, 'Monday',
        order_dow = 2, 'Tuesday',
        order_dow = 3, 'Wednesday',
        order_dow = 4, 'Thursday',
        order_dow = 5, 'Friday',
        order_dow = 6, 'Saturday',
        'Unknown'
    ) AS day_name,
    basket_size
FROM basket
ORDER BY day_name, basket_size;

/* ---------------------------------------------------------------------
Q19 - Peak Order Hours Trend
Metabase visualization: Line Chart
X-Axis: order_hour_of_day
Y-Axis: total_orders
Insight: Mengetahui jam-jam sibuk pelanggan melakukan checkout.
--------------------------------------------------------------------- */
SELECT 
    order_hour_of_day,
    SUM(total_orders) AS total_orders
FROM analytics.orders_hourly_summary
GROUP BY order_hour_of_day
ORDER BY order_hour_of_day;

/* ---------------------------------------------------------------------
Q20 - Top 10 Most Ordered Products
Metabase visualization: Row Chart / Bar Chart
Category: product_name
Value: total_order_lines
Insight: Identifikasi produk dengan volume penjualan tertinggi.
--------------------------------------------------------------------- */
SELECT 
    product_name,
    department,
    total_order_lines
FROM analytics.orders_product_summary
ORDER BY total_order_lines DESC
LIMIT 10;
