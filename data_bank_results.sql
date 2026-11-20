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

-- -------------------------------------------------------------
-- 5. % of customers whose closing balance grew by more than 5%
--    in a month (your original query)
-- -------------------------------------------------------------
WITH MonthlyClosingBalances AS (
  SELECT customer_id,
         EXTRACT(YEAR FROM txn_date) AS year,
         EXTRACT(MONTH FROM txn_date) AS month,
         SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE -txn_amount END)
           OVER (PARTITION BY customer_id
                 ORDER BY EXTRACT(YEAR FROM txn_date), EXTRACT(MONTH FROM txn_date)) AS closing_balance
  FROM customer_transactions
),
PercentageChanges AS (
  SELECT customer_id, year, month, closing_balance,
         ((closing_balance - LAG(closing_balance) OVER (PARTITION BY customer_id ORDER BY year, month))
           / LAG(closing_balance) OVER (PARTITION BY customer_id ORDER BY year, month)) * 100 AS percentage_change
  FROM MonthlyClosingBalances
),
CustomersWithIncrease AS (
  SELECT DISTINCT customer_id
  FROM PercentageChanges
  WHERE percentage_change > 5
)
SELECT ROUND(COUNT(DISTINCT customer_id) * 100.0
             / (SELECT COUNT(DISTINCT customer_id) FROM customer_transactions), 1)
       AS pct_customers_with_increase
FROM CustomersWithIncrease;


-- -------------------------------------------------------------
-- 6. Option 1: total storage per month, based on month-end balances
--    (all_balances = straight sum; positive_only = ignores negative balances)
-- -------------------------------------------------------------
WITH MonthlyNetChange AS (
  SELECT customer_id,
         EXTRACT(YEAR FROM txn_date) AS year,
         EXTRACT(MONTH FROM txn_date) AS month,
         SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE -txn_amount END) AS monthly_net_change
  FROM customer_transactions
  GROUP BY customer_id, EXTRACT(YEAR FROM txn_date), EXTRACT(MONTH FROM txn_date)
),
MonthEndBalances AS (
  SELECT customer_id, year, month,
         SUM(monthly_net_change) OVER (PARTITION BY customer_id ORDER BY year, month) AS month_end_balance
  FROM MonthlyNetChange
)
SELECT year, month,
       SUM(month_end_balance)               AS all_balances_mb,
       SUM(GREATEST(month_end_balance, 0))  AS positive_only_mb
FROM MonthEndBalances
GROUP BY year, month
ORDER BY year, month;


-- -------------------------------------------------------------
-- 7. Option 2: total storage per month, based on average balance
--    over the previous 30 days
-- -------------------------------------------------------------
WITH DailyChanges AS (
  SELECT customer_id, txn_date,
         SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE -txn_amount END) AS daily_net_change
  FROM customer_transactions
  GROUP BY customer_id, txn_date
),
RunningBalances AS (
  SELECT customer_id, txn_date,
         SUM(daily_net_change) OVER (PARTITION BY customer_id ORDER BY txn_date) AS running_balance
  FROM DailyChanges
),
EndOfMonthDates AS (
  SELECT DISTINCT LAST_DAY(txn_date) AS month_end_date
  FROM RunningBalances
),
MonthlyAverages AS (
  SELECT rb.customer_id, eom.month_end_date,
         AVG(rb.running_balance) AS avg_monthly_balance
  FROM RunningBalances rb
  JOIN EndOfMonthDates eom
    ON rb.txn_date BETWEEN eom.month_end_date - INTERVAL 29 DAY AND eom.month_end_date
  GROUP BY rb.customer_id, eom.month_end_date
)
SELECT month_end_date,
       ROUND(SUM(avg_monthly_balance))              AS all_balances_mb,
       ROUND(SUM(GREATEST(avg_monthly_balance, 0))) AS positive_only_mb
FROM MonthlyAverages
GROUP BY month_end_date
ORDER BY month_end_date;
