"""
Walmart KPI Executive Summary Generator
Reads store_summary.csv + SQLite DB, calls Claude API, outputs .txt + .docx

Requirements:
    pip install anthropic pandas
    npm install -g docx

Usage:
    Run this script directly from your project root:
        python generate_summary.py

    Authentication is handled automatically via Claude Code (Pro plan).
    No API key setup required.
"""

import sqlite3
import pandas as pd
import json
import os
import subprocess
import anthropic
from datetime import datetime

# ── Config — update these paths if your folder structure differs ──────────────
DB_PATH           = "db/walmart.db"
STORE_SUMMARY_CSV = "data/processed/store_summary.csv"
OUTPUT_DIR        = "reports"
# ──────────────────────────────────────────────────────────────────────────────

os.makedirs(OUTPUT_DIR, exist_ok=True)


def load_data():
    """Pull key metrics from DB and store_summary CSV."""
    conn = sqlite3.connect(DB_PATH)

    # Overall KPIs
    overall = pd.read_sql("""
        SELECT
            ROUND(SUM(Weekly_Sales)/1000000000, 4) AS Total_Sales_Billions,
            ROUND(AVG(Weekly_Sales), 2)             AS Avg_Weekly_Sales,
            COUNT(DISTINCT Store)                   AS Num_Stores,
            COUNT(DISTINCT DATE(Date))              AS Num_Weeks
        FROM walmart_sales
    """, conn).iloc[0]

    # Holiday vs Non-Holiday average sales
    holiday = pd.read_sql("""
        SELECT
            CASE WHEN IsHoliday='1' THEN 'Holiday' ELSE 'Non-Holiday' END AS Period,
            ROUND(AVG(Weekly_Sales), 2) AS Avg_Weekly_Sales
        FROM walmart_sales
        GROUP BY IsHoliday
    """, conn)

    # Top 5 stores by total sales
    top_stores = pd.read_sql("""
        SELECT Store, ROUND(SUM(Weekly_Sales)/1000000, 2) AS Total_Sales_M
        FROM walmart_sales
        GROUP BY Store
        ORDER BY Total_Sales_M DESC
        LIMIT 5
    """, conn)

    # Month-over-Month trend
    mom = pd.read_sql("""
        WITH m AS (
            SELECT strftime('%Y-%m', Date) AS Month,
                   SUM(Weekly_Sales) AS Total_Sales
            FROM walmart_sales
            GROUP BY 1
        )
        SELECT Month, Total_Sales,
               (Total_Sales - LAG(Total_Sales) OVER (ORDER BY Month))
               / NULLIF(LAG(Total_Sales) OVER (ORDER BY Month), 0) * 100 AS MoM_Pct
        FROM m
    """, conn)

    conn.close()

    # Store-level volatility from exported CSV
    store_summary = pd.read_csv(STORE_SUMMARY_CSV)
    top_volatile  = store_summary.nlargest(5, 'CV')[['Store', 'CV', 'Avg_Weekly_Sales']]
    top_stable    = store_summary.nsmallest(5, 'CV')[['Store', 'CV', 'Avg_Weekly_Sales']]
    high_risk     = store_summary[
        store_summary['Decline_Sales_Count'] > store_summary['Decline_Sales_Count'].median()
    ]

    # Peak and trough months
    monthly_avg  = mom.groupby(mom['Month'].str[5:7])['Total_Sales'].mean()
    peak_month   = monthly_avg.idxmax()
    trough_month = monthly_avg.idxmin()

    return {
        "overall":               overall.to_dict(),
        "holiday":               holiday.to_dict(orient='records'),
        "top_stores":            top_stores.to_dict(orient='records'),
        "top_volatile":          top_volatile.to_dict(orient='records'),
        "top_stable":            top_stable.to_dict(orient='records'),
        "high_risk_store_count": len(high_risk),
        "peak_month":            peak_month,
        "trough_month":          trough_month,
        "mom_sample":            mom.dropna().tail(6).to_dict(orient='records'),
    }


def call_claude(metrics: dict) -> str:
    """Send metrics to Claude API and return a narrative executive summary."""
    client = anthropic.Anthropic()

    prompt = f"""
You are a senior retail data analyst writing an executive summary for Walmart leadership.

Using ONLY the metrics below, write a compelling narrative executive summary (4-6 paragraphs).
Do NOT use bullet points. Write in a storytelling style — show that the data tells a cohesive story.
Start with the big picture, move into performance highlights, then risk, then seasonality,
and close with a forward-looking recommendation sentence.

METRICS:
{json.dumps(metrics, indent=2)}

Guidelines:
- Use specific numbers from the data
- Refer to stores by their Store ID where relevant
- Highlight contrast (best vs worst, holiday vs non-holiday, high risk vs stable)
- Tone: confident, analytical, executive-level
- Do NOT add headers or section labels — pure flowing prose only
"""

    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1000,
        messages=[{"role": "user", "content": prompt}]
    )

    return message.content[0].text


def save_txt(summary: str, path: str):
    """Save summary as a plain text file."""
    timestamp = datetime.now().strftime("%B %d, %Y")
    with open(path, "w", encoding="utf-8") as f:
        f.write("WALMART KPI EXECUTIVE SUMMARY\n")
        f.write(f"Generated: {timestamp}\n")
        f.write("=" * 60 + "\n\n")
        f.write(summary)
    print(f"  ✓ TXT saved  → {path}")


def save_docx(summary: str, path: str):
    """Generate a formatted .docx using docx-js via Node."""
    timestamp = datetime.now().strftime("%B %d, %Y")
    paragraphs    = [p.strip() for p in summary.split("\n\n") if p.strip()]
    paragraphs_js = json.dumps(paragraphs)
    safe_path     = path.replace("\\", "/")

    js = f"""
const {{ Document, Packer, Paragraph, TextRun, HeadingLevel, AlignmentType, BorderStyle }} = require('docx');
const fs = require('fs');

const paragraphs = {paragraphs_js};

const children = [
  new Paragraph({{
    heading: HeadingLevel.HEADING_1,
    children: [new TextRun({{ text: "Walmart KPI Executive Summary", bold: true, font: "Arial", size: 36 }})]
  }}),
  new Paragraph({{
    children: [new TextRun({{ text: "Generated: {timestamp}", italics: true, font: "Arial", size: 20, color: "666666" }})],
    spacing: {{ after: 400 }}
  }}),
  new Paragraph({{
    border: {{ bottom: {{ style: BorderStyle.SINGLE, size: 6, color: "2E75B6", space: 1 }} }},
    spacing: {{ after: 300 }}
  }}),
  ...paragraphs.map(p => new Paragraph({{
    children: [new TextRun({{ text: p, font: "Arial", size: 24 }})],
    spacing: {{ after: 200 }},
    alignment: AlignmentType.JUSTIFIED
  }}))
];

const doc = new Document({{
  styles: {{
    default: {{ document: {{ run: {{ font: "Arial", size: 24 }} }} }},
    paragraphStyles: [
      {{ id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Normal", quickFormat: true,
         run: {{ size: 36, bold: true, font: "Arial", color: "1F3864" }},
         paragraph: {{ spacing: {{ before: 0, after: 240 }}, outlineLevel: 0 }} }}
    ]
  }},
  sections: [{{
    properties: {{
      page: {{
        size: {{ width: 12240, height: 15840 }},
        margin: {{ top: 1440, right: 1440, bottom: 1440, left: 1440 }}
      }}
    }},
    children
  }}]
}});

Packer.toBuffer(doc).then(buf => {{
  fs.writeFileSync("{safe_path}", buf);
  console.log("docx ok");
}});
"""
    js_path = os.path.join(os.path.dirname(path), "_gen_docx_tmp.js")
    with open(js_path, "w") as f:
        f.write(js)

    result = subprocess.run(["node", js_path], capture_output=True, text=True)
    os.remove(js_path)

    if result.returncode == 0:
        print(f"  ✓ DOCX saved → {path}")
    else:
        print(f"  ✗ DOCX error: {result.stderr}")


def main():
    print("\n📊 Loading Walmart KPI data...")
    metrics = load_data()

    print("🤖 Calling Claude API for narrative summary...")
    summary = call_claude(metrics)

    print("💾 Saving outputs...")
    txt_path  = os.path.join(OUTPUT_DIR, "walmart_executive_summary.txt")
    docx_path = os.path.join(OUTPUT_DIR, "walmart_executive_summary.docx")

    save_txt(summary, txt_path)
    save_docx(summary, docx_path)

    print("\n✅ Done. Summary preview:\n")
    print("-" * 60)
    print(summary[:600] + "..." if len(summary) > 600 else summary)
    print("-" * 60)


if __name__ == "__main__":
    main()