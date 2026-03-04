import sqlite3
import pandas as pd
from pathlib import Path

DB_PATH = Path("db/walmart.db")
OUT_DIR = Path("data/processed")
OUT_DIR.mkdir(parents=True, exist_ok=True)

conn = sqlite3.connect(DB_PATH)

# 1. Weekly sales trend
# Source: 02_SQL_KPIs.ipynb — DATE(Date) + AVG query
weekly = pd.read_sql_query(
    """
    SELECT
        DATE(Date)                  AS Date,
        ROUND(SUM(Weekly_Sales), 2) AS Total_Weekly_Sales
    FROM walmart_sales
    GROUP BY Date
    ORDER BY Date
    """,
    conn,
)
weekly.to_csv(OUT_DIR / "weekly_sales_trend.csv", index=False)
print("Saved: weekly_sales_trend.csv")

# 2. Monthly sales with month-over-month change
# Source: 04_Time_Series_Trend.ipynb — q1 (exact query)
monthly = pd.read_sql_query(
    """
    WITH monthly_sales AS (
        SELECT
            strftime('%Y-%m', Date) AS Month,
            SUM(Weekly_Sales)       AS Total_Sales
        FROM walmart_sales
        GROUP BY 1
    )
    SELECT
        Month,
        Total_Sales,
        (Total_Sales - LAG(Total_Sales) OVER (ORDER BY Month)) AS MoM_Change,
        (Total_Sales - LAG(Total_Sales) OVER (ORDER BY Month))
            / NULLIF(LAG(Total_Sales) OVER (ORDER BY Month), 0) * 100 AS MoM_Growth_Pct
    FROM monthly_sales
    GROUP BY 1
    """,
    conn,
)
monthly.to_csv(OUT_DIR / "monthly_sales_mom.csv", index=False)
print("Saved: monthly_sales_mom.csv")

# 3. Holiday summary
# Source: 02_SQL_KPIs.ipynb — CASE/IsHoliday query
holiday = pd.read_sql_query(
    """
    SELECT
        CASE
            WHEN IsHoliday = '1' THEN 'Holiday'
            WHEN IsHoliday = '0' THEN 'Non-Holiday'
            ELSE 'Unknown'
        END                              AS Period,
        ROUND(AVG(Weekly_Sales), 2)      AS Avg_Weekly_Sales,
        ROUND(SUM(Weekly_Sales), 2)      AS Total_Sales,
        COUNT(DISTINCT DATE)             AS Week_Count
    FROM walmart_sales
    GROUP BY IsHoliday
    """,
    conn,
)
holiday.to_csv(OUT_DIR / "holiday_summary.csv", index=False)
print("Saved: holiday_summary.csv")

# 4. Store ranking
# Source: 02_SQL_KPIs.ipynb — store total sales query (extended to all stores + Rank)
store_ranking = pd.read_sql_query(
    """
    SELECT
        Store,
        ROUND(SUM(Weekly_Sales), 2)                            AS Total_Sales,
        ROUND(AVG(Weekly_Sales), 2)                            AS Avg_Weekly_Sales,
        RANK() OVER (ORDER BY SUM(Weekly_Sales) DESC)          AS Rank
    FROM walmart_sales
    GROUP BY Store
    ORDER BY Rank
    """,
    conn,
)
store_ranking.to_csv(OUT_DIR / "store_ranking.csv", index=False)
print("Saved: store_ranking.csv")

conn.close()
