# Data Bank – SQL Case Study

SQL analysis of customer balances, transactions and node allocations for a fictional digital bank, used to estimate how much cloud data storage customers will need.

> **Source:** Case Study #4 of Danny Ma's [8 Week SQL Challenge](https://8weeksqlchallenge.com/case-study-4/). The scenario and dataset belong to the challenge. The SQL solutions and analysis in this repo are my own.

**Tools:** MySQL 8 (MySQL Workbench)

---

## Business problem

Data Bank is a digital bank whose customers receive cloud storage limits linked to their account balances. Management wants to:

1. Understand how customers are allocated across regions and data nodes
2. Analyse deposits, withdrawals and purchases over time
3. Estimate monthly data storage needs under three different allocation rules

## Data model

Three related tables, joined on `region_id` and `customer_id`:

| Table | Rows | Description |
|---|---|---|
| `regions` | 5 | Region ID and region name |
| `customer_nodes` | 3,500 | Which node and region each customer is allocated to, with start and end dates for each allocation |
| `customer_transactions` | 5,868 | Every deposit, withdrawal and purchase, with customer ID, date, type and amount |

The data covers 500 customers from January to April 2020.

---

## What I did

### A. Customer node exploration
- Counted nodes and customers per region
- Measured how long customers stay on a node before being reallocated
- Calculated the median, 80th and 95th percentile of days on node for each region, using `PERCENT_RANK()`

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
  - **Option 1:** based on the customer's balance at the end of the month
  - **Option 2:** based on the customer's average balance over the previous 30 days
  - **Option 3:** based on real-time balance. Discussed only, because it needs live data rather than a static dataset

---

## Data quality issues handled

- **Placeholder end dates.** Each customer's current node allocation has an end date of `9999-12-31`, and 500 rows use it. Left in, these would add about 2.9 million days to each customer's figures, so I excluded them from all reallocation calculations.
- **Months with no transactions.** Customers who made no transactions in a month would drop out of that month's totals. I built a full customer × month (and customer × day) grid with `CROSS JOIN` and a recursive date calendar, so balances carry forward.
- **Negative balances.** Many customers' balances go below zero. A customer can't be given negative storage, so storage totals count negative balances as 0 MB. Adding up raw balances instead gives a negative storage total in some months, which isn't a real answer.

---

## SQL techniques used

`JOIN`s · CTEs · window functions (`LEAD`, `LAG`, `ROW_NUMBER`, `PERCENT_RANK`, running `SUM() OVER`) · date functions (`DATEDIFF`, `EXTRACT`, `LAST_DAY`, `INTERVAL`) · recursive CTEs (date calendar) · `CROSS JOIN` · `LEFT JOIN` with `COALESCE` · `CASE` logic · `GREATEST` · aggregation and grouping · subqueries

---

## Key findings

### Customers and nodes
- Data Bank has **5 nodes** across **5 regions**. **Australia** has the most customers (110) and **Europe** the fewest (88).
- On average, customers stay on a node for **14.6 days** before being reallocated.
- Reallocation patterns are almost the same in every region: median **16 days**, 80th percentile **24–25 days**, 95th percentile **29 days**. This suggests reallocation follows one company-wide rule rather than varying by region.

### Customer balances
- **37%** of customers (185 of 500) increased their month-end balance by more than 5% in at least one month. Growth is only measured when the previous balance was above zero. My first version of this calculation gave 75.8%, because it treated changes between negative balances as growth.

### Storage required (MB, negative balances counted as 0)

Every customer is counted in every month. Customers who made no transactions in a month keep their previous balance.

| Month | Option 1: month-end balance | Option 2: 30-day average |
|---|---|---|
| January 2020 | 235,595 | 128,433 |
| February 2020 | 261,508 | 244,605 |
| March 2020 | 260,971 | 257,327 |
| April 2020 | 264,857 | 256,885 |

📊 **[View the Power BI chart of monthly storage estimates (PDF)](storage_chart.pdf)**

- **Option 1 needs more storage than Option 2 in every month.** From February to April, Option 2 is 1–7% lower, because averaging over 30 days smooths out balance spikes at month end.
- Option 1 settles at about **261,000–265,000 MB** from February onwards.
- Option 2 is low in January because the dataset starts on 1 January. Customers have a zero balance before their first transaction, which pulls down the January 30-day average. This is a start-of-data effect, not a real difference in demand.
- **Carrying balances forward mattered.** My first version left out customers with no transactions in a month, which made April look like a 35% drop in demand (157,033 MB). With every customer counted, April is actually the highest month for Option 1.

---

## Next steps / improvements

- Extend the analysis with a longer time period, so storage trends can be checked beyond four months
- Model Option 3 (real-time balance) using the daily balances already built for Option 2, taking the maximum daily balance per month as the storage needed

---

## Files

- `DATA BANK PORTFOLIO.sql`: all case study queries, organised by section (A, B, C)
- `data_bank_results.sql`: corrected queries for nodes, regions and percentiles
- `data_bank_improvements.sql`: improved storage estimates (Options 1 and 2) and the 5% growth calculation
- `storage_chart.pdf`: Power BI chart of the monthly storage estimates
