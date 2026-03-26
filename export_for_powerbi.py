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
franchise_avg = pd.read_sql_query("SELECT ROUND(AVG(Weekly_Sales), 2) AS val FROM walmart_sales", conn).iloc[0, 0]
weekly['Franchise_Avg_Weekly_Sales'] = franchise_avg
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

# 5. Store summary (volatility + downside risk)
# Source: 03_Volatility&Risk.ipynb — store_summary export
df = pd.read_sql_query("SELECT * FROM walmart_sales", conn)

df["Total_Weekly_Sales"] = df.groupby(["Date", "Store"])["Weekly_Sales"].transform("sum")

store_summary = (
    df.groupby("Store")
    .agg(
        Avg_Weekly_Sales=("Total_Weekly_Sales", "mean"),
        STD_Weekly_Sales=("Total_Weekly_Sales", "std"),
    )
    .reset_index()
)
store_summary["CV"] = store_summary["STD_Weekly_Sales"] / store_summary["Avg_Weekly_Sales"]

total_sales = (
    df.groupby("Store")["Weekly_Sales"]
    .sum()
    .reset_index(name="Total_Sales")
)
store_summary = store_summary.merge(total_sales, on="Store", how="left")

decline_df = pd.read_sql_query(
    """
    WITH weekly_store_sales AS (
        SELECT DATE(Date) AS Date, Store, SUM(Weekly_Sales) AS Total_Weekly_Sales
        FROM walmart_sales
        GROUP BY Store, Date
    ),
    store_wow AS (
        SELECT Date, Store, Total_Weekly_Sales,
               LAG(Total_Weekly_Sales) OVER (PARTITION BY Store ORDER BY Date) AS Prev_Weekly_Sales
        FROM weekly_store_sales
    )
    SELECT Store, COUNT(*) AS Decline_Sales_Count
    FROM store_wow
    WHERE Prev_Weekly_Sales IS NOT NULL
      AND (Total_Weekly_Sales - Prev_Weekly_Sales) / NULLIF(Prev_Weekly_Sales, 0) * 100 < 0
    GROUP BY Store
    ORDER BY Decline_Sales_Count DESC
    """,
    conn,
)
store_summary = store_summary.merge(decline_df, on="Store", how="left")
store_summary = store_summary[
    ["Store", "Total_Sales", "Avg_Weekly_Sales", "STD_Weekly_Sales", "CV", "Decline_Sales_Count"]
]
store_summary.to_csv(OUT_DIR / "store_summary.csv", index=False)
print("Saved: store_summary.csv")

# 6. Week-over-week change (per store, so slicer can filter by Store)
wow = pd.read_sql_query(
    """
    WITH weekly_store AS (
        SELECT
            DATE(Date)        AS Date,
            Store,
            SUM(Weekly_Sales) AS Weekly_Sales,
            LAG(SUM(Weekly_Sales)) OVER (PARTITION BY Store ORDER BY Date) AS Last_Weeks_Sales
        FROM walmart_sales
        GROUP BY Date, Store
    )
    SELECT
        Date,
        Store,
        ROUND(Weekly_Sales, 2)                                              AS Avg_Weekly_Sales,
        ROUND(Last_Weeks_Sales, 2)                                          AS Last_Weeks_Sales,
        ROUND(Weekly_Sales - Last_Weeks_Sales, 2)                           AS WoW_Change,
        ROUND((Weekly_Sales - Last_Weeks_Sales)
              / NULLIF(Last_Weeks_Sales, 0) * 100, 2)                       AS WoW_Percent_Change
    FROM weekly_store
    """,
    conn,
)
wow.to_csv(OUT_DIR / "weekly_wow_change.csv", index=False)
print("Saved: weekly_wow_change.csv")

# 7. Store rank consistency
# Source: 02_SQL_KPIs.ipynb — q4 query
rank_consistency = pd.read_sql_query(
    """
    WITH weekly_store_sales AS (
        SELECT DATE(Date) AS Date, Store, SUM(Weekly_Sales) AS Total_Sales
        FROM walmart_sales
        GROUP BY Date, Store
    ),
    weekly_ranks AS (
        SELECT Date, Store, Total_Sales,
               DENSE_RANK() OVER (PARTITION BY Date ORDER BY Total_Sales DESC) AS Weekly_Rank
        FROM weekly_store_sales
    ),
    rank_changes AS (
        SELECT Date, Store, Weekly_Rank,
               LAG(Weekly_Rank) OVER (PARTITION BY Store ORDER BY Date) AS Last_Week_Rank
        FROM weekly_ranks
    )
    SELECT
        Store,
        ROUND(AVG(ABS(Weekly_Rank - Last_Week_Rank)), 2) AS Avg_Rank_Change
    FROM rank_changes
    WHERE Last_Week_Rank IS NOT NULL
    GROUP BY Store
    ORDER BY Avg_Rank_Change ASC
    """,
    conn,
)
rank_consistency.to_csv(OUT_DIR / "store_rank_consistency.csv", index=False)
print("Saved: store_rank_consistency.csv")

# 8. Store momentum score
# Source: 02_SQL_KPIs.ipynb — q5 query
momentum = pd.read_sql_query(
    """
    WITH weekly_store_sales AS (
        SELECT DATE(Date) AS Date, Store, SUM(Weekly_Sales) AS Total_Weekly_Sales
        FROM walmart_sales
        GROUP BY Store, Date
    ),
    store_wow AS (
        SELECT Date, Store, Total_Weekly_Sales,
               LAG(Total_Weekly_Sales) OVER (PARTITION BY Store ORDER BY Date) AS Prev_Weekly_Sales
        FROM weekly_store_sales
    )
    SELECT
        Store,
        ROUND(AVG((Total_Weekly_Sales - Prev_Weekly_Sales)
              / NULLIF(Prev_Weekly_Sales, 0) * 100), 6) AS Momentum_Score
    FROM store_wow
    WHERE Prev_Weekly_Sales IS NOT NULL
    GROUP BY Store
    ORDER BY Momentum_Score DESC
    """,
    conn,
)
momentum.to_csv(OUT_DIR / "store_momentum.csv", index=False)
print("Saved: store_momentum.csv")

# 9. Seasonality by calendar month
# Source: 04_Time_Series_Trend.ipynb — q2 query
seasonality = pd.read_sql_query(
    """
    SELECT
        strftime('%m', Date) AS Month,
        ROUND(AVG(Weekly_Sales), 2) AS Avg_Weekly_Sales
    FROM walmart_sales
    GROUP BY Month
    ORDER BY Month
    """,
    conn,
)
seasonality.to_csv(OUT_DIR / "seasonality_by_month.csv", index=False)
print("Saved: seasonality_by_month.csv")

# 10. Z-score anomaly detection on MoM growth
# Source: 04_Time_Series_Trend.ipynb — q4 (derived from q1)
monthly_for_zscore = pd.read_sql_query(
    """
    WITH monthly_sales AS (
        SELECT strftime('%Y-%m', Date) AS Month, SUM(Weekly_Sales) AS Total_Sales
        FROM walmart_sales
        GROUP BY 1
    )
    SELECT
        Month,
        Total_Sales,
        (Total_Sales - LAG(Total_Sales) OVER (ORDER BY Month))
            / NULLIF(LAG(Total_Sales) OVER (ORDER BY Month), 0) * 100 AS MoM_Growth_Pct
    FROM monthly_sales
    GROUP BY 1
    """,
    conn,
)
mean_growth = monthly_for_zscore["MoM_Growth_Pct"].mean()
std_growth  = monthly_for_zscore["MoM_Growth_Pct"].std()
monthly_for_zscore["Z_Score"] = (
    (monthly_for_zscore["MoM_Growth_Pct"] - mean_growth) / std_growth
)
monthly_for_zscore["MoM_Rolling_3M"] = (
    monthly_for_zscore["MoM_Growth_Pct"].rolling(window=3).mean()
)
monthly_for_zscore = monthly_for_zscore.round(6)
monthly_for_zscore.to_csv(OUT_DIR / "monthly_zscore_anomaly.csv", index=False)
print("Saved: monthly_zscore_anomaly.csv")

conn.close()
