# Data Bank – SQL Case Study

SQL analysis of customer balances, transactions and node allocations for a fictional digital bank, used to estimate how much cloud data storage customers will need.

> **Source:** Case Study #4 of Danny Ma's [8 Week SQL Challenge](https://8weeksqlchallenge.com/case-study-4/). The scenario and dataset belong to the challenge. The SQL solutions and analysis in this repo are my own.

**Tools:** MySQL

---

## Business problem

Data Bank is a digital bank whose customers receive cloud storage limits linked to their account balances. Management wants to:

1. Understand how customers are allocated across regions and data nodes
2. Analyse deposits, withdrawals and purchases over time
3. Estimate monthly data storage needs under three different allocation rules

## Data model

Three related tables, joined on `region_id` and `customer_id`:

| Table | Description |
|---|---|
| `regions` | Region ID and region name |
| `customer_nodes` | Which node and region each customer is allocated to, with start and end dates for each allocation |
| `customer_transactions` | Every deposit, withdrawal and purchase, with customer ID, date, type and amount |

---

## What I did

### A. Customer node exploration
- Counted nodes and customers per region
- Measured how long customers stay on a node before being reallocated
- Calculated the median, 80th and 95th percentile of reallocation days for each region, using `ROW_NUMBER()` and `COUNT() OVER` to rank values within each region

### B. Customer transactions
- Summarised unique customer counts and total amounts by transaction type
- Calculated the average number and value of deposits per customer
- Identified, for each month, customers who made more than one deposit plus a purchase or withdrawal
- Built month-end closing balances for every customer using a running `SUM() OVER`
- Measured the share of customers whose closing balance grew by more than 5% month on month, using `LAG()`

### C. Data allocation challenge
- Calculated a running balance after every transaction
- Calculated each customer's minimum, average and maximum running balance
- Estimated monthly storage needed under three rules (1 unit of balance = 1 MB of storage):
  - **Option 1:** based on the customer's balance at the end of the previous month
  - **Option 2:** based on the customer's average balance over the previous 30 days
  - **Option 3:** based on real-time balance. Discussed only, because it needs live data rather than a static dataset

---

## SQL techniques used

`JOIN`s · CTEs · window functions (`LEAD`, `LAG`, `ROW_NUMBER`, running `SUM() OVER`, `COUNT() OVER`) · date functions (`DATEDIFF`, `EXTRACT`, `LAST_DAY`, `INTERVAL`) · manual percentile calculation · `CASE` logic · aggregation and grouping · subqueries

---

## Key findings

**Option 1: storage required based on month-end balances (all customers combined)**

| Month | Storage required |
|---|---|
| January 2020 | 126,091 MB |
| February 2020 | 34,350 MB |
| March 2020 | 194,916 MB |
| April 2020 | 180,855 MB |

- Demand peaked in **March 2020** at about 195,000 MB, and February was the lowest month at about 34,000 MB. That swing is more than five times.
- Because month-end balances move this much, Option 1 would mean resizing storage a lot from month to month. Option 2 (30-day average) smooths out short-term spikes and dips, so it would be easier to plan capacity around.

---

## Next steps / improvements

- Exclude the placeholder end date `9999-12-31` (used for customers' current node) from the reallocation calculations, so it doesn't distort the averages and percentiles
- Calculate the total storage required per month for Option 2 so it can be compared directly with Option 1
- Analyse without changing the source tables (currently a `reallocation_days` column is added with `ALTER TABLE`). Use a CTE instead
- Visualise the monthly storage estimates for each option in Power BI

---

## Files

- `DATA BANK PORTFOLIO.sql`: all queries, organised by section (A, B, C)
