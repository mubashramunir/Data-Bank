# Data Bank – SQL Case Study

SQL analysis of customer balances, transactions and node allocations for a fictional digital bank, used to estimate how much cloud data storage customers will need.

> **Source:** Case Study #4 of Danny Ma's [8 Week SQL Challenge](https://8weeksqlchallenge.com/case-study-4/). The scenario and dataset belong to the challenge. The SQL solutions and analysis in this repo are my own.

## Business problem

Data Bank is a digital bank whose customers receive cloud storage limits linked to their account balances. Management wants to:

1. Understand how customers are allocated across regions and data nodes
2. Analyse deposits, withdrawals and purchases over time
3. Estimate monthly data storage needs under three different allocation rules

## Data model

Three related tables: `regions`, `customer_nodes` and `customer_transactions` (joined on `region_id` and `customer_id`).

# Entity Relationship Diagram
![image](https://github.com/mubashramunir/Data-Bank/assets/88100648/12f35cc2-2bd6-4f63-b372-419700f5b460)

## What I did

**A. Customer node exploration**
- Counted unique nodes, and customers per region
- Calculated how often customers are reallocated between nodes (average, median, 80th and 95th percentiles per region)

**B. Customer transactions**
- Summarised transaction counts and totals by type
- Calculated average deposit count and amount per customer
- Identified customers with multiple deposits plus a purchase or withdrawal in the same month
- Built month end closing balances for every customer
- Measured the share of customers whose balance grew by more than 5% month on month

**C. Data allocation challenge**
- Calculated running balances per transaction and min / average / max balance per customer
- Estimated monthly storage required under:
  - **Option 1:** based on the previous month's closing balance
  - **Option 2:** based on the average balance over the previous 30 days
  - **Option 3:** real-time balance (discussed, as it needs live data rather than static analysis)

## SQL techniques used

`JOIN`s · CTEs · window functions (`LEAD`, `LAG`, `ROW_NUMBER`, running `SUM() OVER`) · date functions · percentile calculations · `CASE` logic · aggregation and grouping

## Key findings

- Customers are spread across 5 regions, with [region] holding the most customers ([number]).
- On average, customers are reallocated to a new node every [number] days.
- [x]% of customers increased their closing balance by more than 5% in a month.
- Option [x] required the most storage, peaking in [month] at [amount].

## Next steps / improvements

- Refine the reallocation calculation to measure the gap between consecutive allocations
- Use `PERCENTILE_CONT` for more robust percentile estimates
- Visualise monthly storage estimates in Power BI

## Files

- `DATA BANK PORTFOLIO.sql`: all queries, organised by section (A, B, C)

  ![image](https://github.com/mubashramunir/Data-Bank/assets/88100648/6f3eaf20-efb9-40b4-bba5-f92dd86639da)
  ![image](https://github.com/mubashramunir/Data-Bank/assets/88100648/843db796-8a37-4f1a-92ad-2f9301d94e5d)





