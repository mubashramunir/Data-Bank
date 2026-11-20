-- =============================================================
-- Data Bank – customer nodes and regions (README: "Customers and nodes")
-- Storage estimates and balance growth are in data_bank_improvements.sql
-- Run each query ONE AT A TIME: click inside it, press Ctrl+Enter
-- =============================================================

USE data_bank;


-- -------------------------------------------------------------
-- 1. How many unique nodes are there?
-- -------------------------------------------------------------
SELECT COUNT(DISTINCT node_id) AS unique_nodes
FROM customer_nodes;


-- -------------------------------------------------------------
-- 2. Customers per region
-- -------------------------------------------------------------
SELECT r.region_name,
       COUNT(DISTINCT cn.customer_id) AS total_customers
FROM customer_nodes cn
JOIN regions r ON cn.region_id = r.region_id
GROUP BY r.region_name
ORDER BY total_customers DESC;


-- -------------------------------------------------------------
-- 3. Average days a customer stays on a node before reallocation
--    (excludes the 9999-12-31 placeholder for current nodes)
-- -------------------------------------------------------------
SELECT ROUND(AVG(DATEDIFF(end_date, start_date)), 1) AS avg_days_on_node
FROM customer_nodes
WHERE end_date != '9999-12-31';


-- -------------------------------------------------------------
-- 4. Median, 80th and 95th percentile of days on node, per region
-- -------------------------------------------------------------
WITH node_days AS (
  SELECT r.region_name,
         DATEDIFF(cn.end_date, cn.start_date) AS days_on_node
  FROM customer_nodes cn
  JOIN regions r ON cn.region_id = r.region_id
  WHERE cn.end_date != '9999-12-31'
),
ranked AS (
  SELECT region_name, days_on_node,
         PERCENT_RANK() OVER (PARTITION BY region_name ORDER BY days_on_node) AS pct
  FROM node_days
)
SELECT region_name,
       MIN(CASE WHEN pct >= 0.50 THEN days_on_node END) AS median_days,
       MIN(CASE WHEN pct >= 0.80 THEN days_on_node END) AS p80_days,
       MIN(CASE WHEN pct >= 0.95 THEN days_on_node END) AS p95_days
FROM ranked
GROUP BY region_name
ORDER BY region_name;
