-- =============================================================
-- Data Bank – improvements
-- Run each query ONE AT A TIME: click inside it, press Ctrl+Enter
-- =============================================================

USE data_bank;


-- -------------------------------------------------------------
-- 8. Option 1 (improved): month-end balances, with customers who
--    made no transactions in a month carried forward
-- -------------------------------------------------------------
WITH months AS (                       -- every month in the data
  SELECT DISTINCT LAST_DAY(txn_date) AS month_end
  FROM customer_transactions
),
customers AS (                         -- every customer
  SELECT DISTINCT customer_id
  FROM customer_transactions
),
customer_months AS (                   -- one row per customer per month
  SELECT c.customer_id, m.month_end
  FROM customers c
  CROSS JOIN months m
),
monthly_net AS (                       -- net change per customer per month
  SELECT customer_id,
         LAST_DAY(txn_date) AS month_end,
         SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE -txn_amount END) AS net_change
  FROM customer_transactions
  GROUP BY customer_id, LAST_DAY(txn_date)
),
month_end_balances AS (                -- months with no transactions add 0, so the balance carries forward
  SELECT cm.customer_id, cm.month_end,
         SUM(COALESCE(mn.net_change, 0))
           OVER (PARTITION BY cm.customer_id ORDER BY cm.month_end) AS balance
  FROM customer_months cm
  LEFT JOIN monthly_net mn
    ON cm.customer_id = mn.customer_id
   AND cm.month_end  = mn.month_end
)
SELECT month_end,
       COUNT(*)                   AS customers_counted,
       SUM(GREATEST(balance, 0))  AS option1_storage_mb
FROM month_end_balances
GROUP BY month_end
ORDER BY month_end;


-- -------------------------------------------------------------
-- 9. Option 2 (improved): average balance over the previous 30 days,
--    using a balance for EVERY day (not just days with transactions)
-- -------------------------------------------------------------
WITH RECURSIVE calendar AS (           -- every day from 1 Jan to 30 Apr 2020
  SELECT DATE('2020-01-01') AS day
  UNION ALL
  SELECT day + INTERVAL 1 DAY
  FROM calendar
  WHERE day < '2020-04-30'
),
customers AS (
  SELECT DISTINCT customer_id
  FROM customer_transactions
),
customer_days AS (                     -- one row per customer per day
  SELECT c.customer_id, cal.day
  FROM customers c
  CROSS JOIN calendar cal
),
daily_net AS (
  SELECT customer_id, txn_date AS day,
         SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE -txn_amount END) AS net_change
  FROM customer_transactions
  GROUP BY customer_id, txn_date
),
daily_balances AS (                    -- days with no transactions carry the balance forward
  SELECT cd.customer_id, cd.day,
         SUM(COALESCE(dn.net_change, 0))
           OVER (PARTITION BY cd.customer_id ORDER BY cd.day) AS balance
  FROM customer_days cd
  LEFT JOIN daily_net dn
    ON cd.customer_id = dn.customer_id
   AND cd.day = dn.day
),
month_ends AS (
  SELECT DISTINCT LAST_DAY(day) AS month_end
  FROM calendar
),
avg_30_days AS (                       -- each customer's average balance over the 30 days up to month end
  SELECT db.customer_id, me.month_end,
         AVG(db.balance) AS avg_balance
  FROM daily_balances db
  JOIN month_ends me
    ON db.day BETWEEN me.month_end - INTERVAL 29 DAY AND me.month_end
  GROUP BY db.customer_id, me.month_end
)
SELECT month_end,
       COUNT(*)                                AS customers_counted,
       ROUND(SUM(GREATEST(avg_balance, 0)))    AS option2_storage_mb
FROM avg_30_days
GROUP BY month_end
ORDER BY month_end;


-- -------------------------------------------------------------
-- 10. % of customers whose balance grew by more than 5% in a month
--     (improved: growth only measured when the previous balance
--      was above zero, so negative/zero balances can't distort it)
-- -------------------------------------------------------------
WITH months AS (
  SELECT DISTINCT LAST_DAY(txn_date) AS month_end
  FROM customer_transactions
),
customers AS (
  SELECT DISTINCT customer_id
  FROM customer_transactions
),
customer_months AS (
  SELECT c.customer_id, m.month_end
  FROM customers c
  CROSS JOIN months m
),
monthly_net AS (
  SELECT customer_id,
         LAST_DAY(txn_date) AS month_end,
         SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE -txn_amount END) AS net_change
  FROM customer_transactions
  GROUP BY customer_id, LAST_DAY(txn_date)
),
month_end_balances AS (
  SELECT cm.customer_id, cm.month_end,
         SUM(COALESCE(mn.net_change, 0))
           OVER (PARTITION BY cm.customer_id ORDER BY cm.month_end) AS balance
  FROM customer_months cm
  LEFT JOIN monthly_net mn
    ON cm.customer_id = mn.customer_id
   AND cm.month_end  = mn.month_end
),
with_previous AS (
  SELECT customer_id, month_end, balance,
         LAG(balance) OVER (PARTITION BY customer_id ORDER BY month_end) AS prev_balance
  FROM month_end_balances
),
grew_5_pct AS (
  SELECT DISTINCT customer_id
  FROM with_previous
  WHERE prev_balance > 0
    AND (balance - prev_balance) / prev_balance > 0.05
)
SELECT
  (SELECT COUNT(*) FROM grew_5_pct)                                    AS customers_grew_5pct,
  (SELECT COUNT(*) FROM customers)                                     AS total_customers,
  ROUND((SELECT COUNT(*) FROM grew_5_pct) * 100.0
        / (SELECT COUNT(*) FROM customers), 1)                         AS pct_customers_grew_5pct;
