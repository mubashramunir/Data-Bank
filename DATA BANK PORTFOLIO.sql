select * from regions;
select * from customer_nodes;
select * from customer_transactions;

--      A. Customer Nodes Exploration
-- How many unique nodes are there on the Data Bank system?
SELECT customer_id, COUNT(DISTINCT node_id) AS Unique_Nodes
FROM customer_nodes
GROUP BY customer_id;

-- What is the number of nodes per region?
SELECT c.region_id, COUNT(c.node_id) AS number_of_nodes
FROM customer_nodes c
JOIN regions r ON c.region_id = r.region_id
GROUP BY c.region_id
ORDER BY c.region_id;

-- How many customers are allocated to each region?
SELECT region_id, COUNT(DISTINCT customer_id) AS TOTAL_CUSTOMERS
FROM customer_nodes
GROUP BY region_id;

-- How many days on average are customers reallocated to a different node?

WITH RankedAllocations AS (
  SELECT
    customer_id, node_id, start_date, end_date,
    LEAD(start_date) OVER (PARTITION BY customer_id ORDER BY start_date) AS next_start_date
  FROM customer_nodes
),
Gaps AS (
  SELECT customer_id, node_id, start_date, end_date, next_start_date,
    CASE
      WHEN next_start_date IS NOT NULL THEN DATEDIFF(next_start_date, end_date)
      ELSE NULL
    END AS gap_days
  FROM RankedAllocations
)
SELECT AVG(gap_days) AS average_reallocation_gap
FROM Gaps
WHERE gap_days IS NOT NULL;

-- What is the median, 80th and 95th percentile for this same reallocation days metric for each region?
ALTER TABLE customer_nodes
ADD COLUMN reallocation_days INT;
UPDATE customer_nodes
SET reallocation_days = DATEDIFF(end_date, start_date);

WITH RankedRegions AS (
  SELECT
    region_id,
    reallocation_days,
    ROW_NUMBER() OVER (PARTITION BY region_id ORDER BY reallocation_days) AS rn,
    COUNT(*) OVER (PARTITION BY region_id) AS total_rows
  FROM
    customer_nodes
),
Percentiles AS (
  SELECT
    region_id,
    reallocation_days,
    rn,
    total_rows,
    CASE
      WHEN rn = ROUND(total_rows * 0.5) THEN 'Median'
      WHEN rn = ROUND(total_rows * 0.8) THEN '80th Percentile'
      WHEN rn = ROUND(total_rows * 0.95) THEN '95th Percentile'
    END AS Percentile
  FROM RankedRegions
)
SELECT
  region_id,
  Percentile,
  AVG(reallocation_days) AS reallocation_days -- Averaging to handle rounding issues
FROM Percentiles
WHERE Percentile IS NOT NULL
GROUP BY region_id, Percentile
ORDER BY Percentile;



-- B. Customer Transactions
--  What is the unique count and total amount for each transaction type?
SELECT txn_type, Count(Distinct customer_id) as unique_count, SUM(txn_amount) as Total_amount
FROM customer_transactions
GROUP BY txn_type;

-- What is the average total historical deposit counts and amounts for all customers?
SELECT AVG(deposit_count) AS average_deposit_count, AVG(total_amount) AS average_deposit_amount
FROM (
  SELECT customer_id, COUNT(*) AS deposit_count, SUM(txn_amount) AS total_amount
  FROM customer_transactions
  WHERE txn_type = 'deposit'
  GROUP BY customer_id
) AS customer_deposits;

-- For each month - how many Data Bank customers make more than 1 deposit and either 1 purchase or 1 withdrawal in a single month?
WITH MonthlyTransactions AS (
  SELECT customer_id, EXTRACT(MONTH FROM txn_date) AS month, txn_type, COUNT(*) OVER (PARTITION BY customer_id, EXTRACT(MONTH FROM txn_date), txn_type) AS txn_count
  FROM customer_transactions
),
FilteredCustomers AS (
  SELECT  customer_id,  month
  FROM MonthlyTransactions
  WHERE (txn_type = 'deposit' AND txn_count > 1) OR (txn_type IN ('purchase', 'withdrawal') AND txn_count = 1)
  GROUP BY customer_id, month
  HAVING COUNT(DISTINCT txn_type) > 1
)
SELECT  month, COUNT(*) AS Number_of_Customers
FROM  FilteredCustomers
GROUP BY  month
ORDER BY  month;

--  What is the closing balance for each customer at the end of the month?
WITH MonthlyBalances AS (
  SELECT
    customer_id, EXTRACT(YEAR FROM txn_date) AS year, EXTRACT(MONTH FROM txn_date) AS month,
    SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE 0 END) - SUM(CASE WHEN txn_type IN ('withdrawal', 'purchase') THEN txn_amount ELSE 0 END) AS net_change
  FROM customer_transactions
  GROUP BY customer_id, EXTRACT(YEAR FROM txn_date), EXTRACT(MONTH FROM txn_date)
),
CumulativeBalances AS (
  SELECT customer_id, year, month, net_change, SUM(net_change) OVER (PARTITION BY customer_id ORDER BY year, month) AS cumulative_balance
  FROM MonthlyBalances
)
SELECT customer_id, year, month, cumulative_balance  
FROM CumulativeBalances
ORDER BY customer_id, year, month; -- month 1,2,3,4 indicates January, Feb, March, April 

-- What is the percentage of customers who increase their closing balance by more than 5%?

WITH MonthlyClosingBalances AS (
  SELECT
    customer_id,
    EXTRACT(YEAR FROM txn_date) AS year,
    EXTRACT(MONTH FROM txn_date) AS month,
    SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE -txn_amount END) OVER (PARTITION BY customer_id ORDER BY EXTRACT(YEAR FROM txn_date), EXTRACT(MONTH FROM txn_date)) AS closing_balance
  FROM
    customer_transactions
),
PercentageChanges AS (
  SELECT
    customer_id,
    year,
    month,
    closing_balance,
    LAG(closing_balance) OVER (PARTITION BY customer_id ORDER BY year, month) AS previous_balance,
    ((closing_balance - LAG(closing_balance) OVER (PARTITION BY customer_id ORDER BY year, month)) / LAG(closing_balance) OVER (PARTITION BY customer_id ORDER BY year, month)) * 100 AS percentage_change
  FROM
    MonthlyClosingBalances
),
CustomersWithIncrease AS (
  SELECT DISTINCT
    customer_id
  FROM
    PercentageChanges
  WHERE
    percentage_change > 5
)
SELECT
  (COUNT(DISTINCT customer_id) * 1.0 / (SELECT COUNT(DISTINCT customer_id) FROM customer_transactions)) * 100 AS percentage_of_customers_with_increase
FROM
  CustomersWithIncrease;
  
  
-- C.  Data Allocation Challenge
-- running customer balance column that includes the impact each transaction
SELECT customer_id, txn_date,
       SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE -txn_amount END)
       OVER (PARTITION BY customer_id ORDER BY txn_date) AS running_balance
FROM customer_transactions;

-- customer balance at the end of each month
WITH MonthlyNetChange AS (
  SELECT
    customer_id,
    EXTRACT(YEAR FROM txn_date) AS year,
    EXTRACT(MONTH FROM txn_date) AS month,
    SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE -txn_amount END) AS monthly_net_change
  FROM
    customer_transactions
  GROUP BY
    customer_id,
    EXTRACT(YEAR FROM txn_date),
    EXTRACT(MONTH FROM txn_date)
),
CumulativeMonthlyBalance AS (
  SELECT
    customer_id,
    year,
    month,
    monthly_net_change,
    SUM(monthly_net_change) OVER (PARTITION BY customer_id ORDER BY year, month) AS cumulative_monthly_balance
  FROM
    MonthlyNetChange
)
SELECT
  customer_id,
  year,
  month,
  cumulative_monthly_balance
FROM
  CumulativeMonthlyBalance
ORDER BY
  customer_id,
  year, month;


--  minimum, average and maximum values of the running balance for each customer
WITH RunningBalances AS (
  SELECT customer_id, txn_date, SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE -txn_amount END) OVER (PARTITION BY customer_id ORDER BY txn_date) AS running_balance
  FROM customer_transactions
)
SELECT customer_id, MIN(running_balance) AS min_balance, AVG(running_balance) AS avg_balance, MAX(running_balance) AS max_balance
FROM RunningBalances
GROUP BY customer_id;


-- For Option 1, where data is allocated based on the customer balance at the end of each month, the estimated total data required on a monthly basis is as follows (in MB, assuming 1 unit of balance equates to 1 MB of data):

-- January 2020: 126,091 MB
-- February 2020: 34,350 MB
-- March 2020: 194,916 MB
-- April 2020: 180,855 MB
-- These figures represent the total data allocation needed for all customers combined, based on their end-of-month balances for each month.

-- Option 2: data is allocated on the average amount of money kept in the account in the previous 30 days
WITH DailyChanges AS (
  SELECT 
    customer_id, 
    txn_date, 
    SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE -txn_amount END) AS daily_net_change
  FROM 
    customer_transactions
  GROUP BY 
    customer_id, txn_date
),
RunningBalances AS (
  SELECT 
    customer_id, 
    txn_date, 
    SUM(daily_net_change) OVER (PARTITION BY customer_id ORDER BY txn_date) AS running_balance
  FROM 
    DailyChanges
),
EndOfMonthDates AS (
  SELECT 
    DISTINCT LAST_DAY(txn_date) AS month_end_date
  FROM 
    RunningBalances
),
MonthlyAverages AS (
  SELECT 
    rb.customer_id, 
    eom.month_end_date,
    AVG(rb.running_balance) AS avg_monthly_balance
  FROM 
    RunningBalances rb
  JOIN 
    EndOfMonthDates eom ON rb.txn_date BETWEEN eom.month_end_date - INTERVAL '29' DAY AND eom.month_end_date
  GROUP BY 
    rb.customer_id, eom.month_end_date
)
SELECT 
  customer_id, 
  month_end_date, 
  avg_monthly_balance
FROM 
  MonthlyAverages
ORDER BY 
  customer_id, month_end_date;


-- Option 3 is beyond the scope of static analysis







