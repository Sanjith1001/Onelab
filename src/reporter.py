import json
import logging
import os
import sys
from html import escape
from datetime import datetime

import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.config import ASSESSIONS_ASSUMPTIONS, CURRENCY_SYMBOL, DATASET_NOTE
from src.config import GAP_MATCHED, MISMATCHES_FILE, OUTPUT_DIR, REPORT_FILE, SUMMARY_FILE


logger = logging.getLogger("payrecon.reporter")

METRICS_FILE = os.path.join(OUTPUT_DIR, "evaluation_metrics.csv")
DASHBOARD_FILE = os.path.join(OUTPUT_DIR, "dashboard.html")
ASSUMPTIONS_FILE = os.path.join(OUTPUT_DIR, "assessment_assumptions.txt")


def save_all(recon: pd.DataFrame, summary: pd.DataFrame, metrics: pd.DataFrame):
    """Save all generated reports to the output directory."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    _save_full_report(recon)
    _save_mismatches(recon)
    _save_summary(summary)
    _save_metrics(metrics)
    _save_assumptions()
    _save_dashboard(recon, summary, metrics)

    logger.info("All files saved to: %s", OUTPUT_DIR)
    logger.info("saved %s", REPORT_FILE)
    logger.info("saved %s", MISMATCHES_FILE)
    logger.info("saved %s", SUMMARY_FILE)
    logger.info("saved %s", METRICS_FILE)
    logger.info("saved %s", ASSUMPTIONS_FILE)
    logger.info("saved %s", DASHBOARD_FILE)


def _save_full_report(recon: pd.DataFrame):
    """Write the full reconciliation table."""
    df = _format_date_columns(recon.copy())
    df.to_csv(REPORT_FILE, index=False)
    logger.info("Full report saved: %s rows -> %s", len(df), REPORT_FILE)


def _save_mismatches(recon: pd.DataFrame):
    """Write only the rows that require investigation."""
    df = _format_date_columns(recon[recon["gap_type"] != GAP_MATCHED].copy())
    df.to_csv(MISMATCHES_FILE, index=False)
    logger.info("Mismatch report saved: %s rows -> %s", len(df), MISMATCHES_FILE)


def _save_summary(summary: pd.DataFrame):
    """Write the grouped summary report."""
    summary.to_csv(SUMMARY_FILE, index=False)
    logger.info("Summary report saved: %s rows -> %s", len(summary), SUMMARY_FILE)


def _save_metrics(metrics: pd.DataFrame):
    metrics.to_csv(METRICS_FILE, index=False)
    logger.info("Evaluation metrics saved: %s rows -> %s", len(metrics), METRICS_FILE)


def _save_dashboard(recon: pd.DataFrame, summary: pd.DataFrame, metrics: pd.DataFrame):
    dashboard_html = _build_dashboard_html(recon, summary, metrics)
    with open(DASHBOARD_FILE, "w", encoding="utf-8") as f:
        f.write(dashboard_html)
    logger.info("Dashboard saved -> %s", DASHBOARD_FILE)


def _save_assumptions():
    lines = [DATASET_NOTE, "", "Assumptions:"]
    lines.extend(f"- {assumption}" for assumption in ASSESSIONS_ASSUMPTIONS)
    with open(ASSUMPTIONS_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    logger.info("Assessment assumptions saved -> %s", ASSUMPTIONS_FILE)


def print_tables(recon: pd.DataFrame, summary: pd.DataFrame, metrics: pd.DataFrame | None = None):
    """Pretty-print both tables to the terminal."""
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 140)
    pd.set_option("display.float_format", "{:,.5f}".format)

    print("\n" + "-" * 100)
    print("  FULL RECONCILIATION TABLE")
    print("-" * 100)

    display_cols = [
        "transaction_id",
        "txn_date",
        "settlement_date",
        "txn_amount_inr",
        "settled_amount_inr",
        "delta_inr",
        "gap_type",
    ]
    display_cols = [c for c in display_cols if c in recon.columns]

    df_print = recon[display_cols].copy()
    df_print["txn_date"] = recon["txn_date"].dt.strftime("%Y-%m-%d").where(recon["txn_date"].notna(), "-")
    df_print["settlement_date"] = recon["settlement_date"].dt.strftime("%Y-%m-%d").where(
        recon["settlement_date"].notna(), "-"
    )
    df_print["txn_amount_inr"] = recon["txn_amount_inr"].apply(
        lambda x: f"{CURRENCY_SYMBOL}{x:,.3f}" if pd.notna(x) else "-"
    )
    df_print["settled_amount_inr"] = recon["settled_amount_inr"].apply(
        lambda x: f"{CURRENCY_SYMBOL}{x:,.3f}" if pd.notna(x) else "-"
    )
    df_print["delta_inr"] = recon["delta_inr"].apply(lambda x: f"{x:+.5f}" if pd.notna(x) else "-")

    print(df_print.to_string(index=False))

    print("\n" + "-" * 70)
    print("  SUMMARY BY GAP TYPE")
    print("-" * 70)

    summary_print = summary.copy()
    summary_print["total_txn_amount_inr"] = summary_print["total_txn_amount_inr"].apply(
        lambda x: f"{CURRENCY_SYMBOL}{x:,.2f}"
    )
    summary_print["total_settled_inr"] = summary_print["total_settled_inr"].apply(
        lambda x: f"{CURRENCY_SYMBOL}{x:,.2f}"
    )
    summary_print["net_delta_inr"] = summary_print["net_delta_inr"].apply(lambda x: f"{x:+.5f}")
    print(summary_print.to_string(index=False))

    if metrics is not None:
        print("\n" + "-" * 70)
        print("  EVALUATION METRICS")
        print("-" * 70)
        print(metrics[["metric", "display_value", "status", "description"]].to_string(index=False))


def _format_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    if "txn_date" in df.columns:
        df["txn_date"] = df["txn_date"].dt.strftime("%Y-%m-%d").where(df["txn_date"].notna(), "")
    if "settlement_date" in df.columns:
        df["settlement_date"] = df["settlement_date"].dt.strftime("%Y-%m-%d").where(
            df["settlement_date"].notna(), ""
        )
    return df


def _build_dashboard_html(recon: pd.DataFrame, summary: pd.DataFrame, metrics: pd.DataFrame) -> str:
    period_month = recon.attrs.get("period_month", "")
    period_year = recon.attrs.get("period_year", "")
    title = f"PayRecon Dashboard - {period_month:02d}/{period_year}" if period_month and period_year else "PayRecon Dashboard"
    status_classes = {"good": "pill-good", "warn": "pill-warn", "bad": "pill-bad", "info": "pill-info"}
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    assumptions_html = "".join(f"<li>{escape(item)}</li>" for item in ASSESSIONS_ASSUMPTIONS)

    metric_cards = []
    for _, row in metrics.iterrows():
        metric_cards.append(
            f"""
            <article class="metric-card">
              <div class="metric-top">
                <span class="metric-label">{escape(str(row['metric']).replace('_', ' ').title())}</span>
                <span class="pill {status_classes.get(str(row['status']), 'pill-info')}">{escape(str(row['status']).upper())}</span>
              </div>
              <div class="metric-value">{escape(str(row['display_value']))}</div>
              <p class="metric-desc">{escape(str(row['description']))}</p>
            </article>
            """
        )

    summary_rows = []
    for _, row in summary.iterrows():
        summary_rows.append(
            f"""
            <tr>
              <td>{escape(str(row['gap_type']))}</td>
              <td>{int(row['transaction_count'])}</td>
              <td>{CURRENCY_SYMBOL}{row['total_txn_amount_inr']:,.2f}</td>
              <td>{CURRENCY_SYMBOL}{row['total_settled_inr']:,.2f}</td>
              <td>{row['net_delta_inr']:+.5f}</td>
            </tr>
            """
        )

    mismatch_rows = recon[recon["gap_type"] != GAP_MATCHED].head(12).copy()
    mismatch_rows_html = []
    for _, row in mismatch_rows.iterrows():
        mismatch_rows_html.append(
            f"""
            <tr>
              <td>{escape(str(row.get('transaction_id', '')))}</td>
              <td>{escape(str(row.get('gap_type', '')))}</td>
              <td>{'' if pd.isna(row.get('txn_amount_inr')) else f"{CURRENCY_SYMBOL}{float(row['txn_amount_inr']):,.2f}"}</td>
              <td>{'' if pd.isna(row.get('settled_amount_inr')) else f"{CURRENCY_SYMBOL}{float(row['settled_amount_inr']):,.2f}"}</td>
              <td>{'' if pd.isna(row.get('delta_inr')) else f"{float(row['delta_inr']):+.5f}"}</td>
            </tr>
            """
        )

    chart_data = json.dumps(
        {
            "labels": summary["gap_type"].tolist(),
            "values": summary["transaction_count"].astype(int).tolist(),
        }
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{escape(title)}</title>
  <style>
    :root {{
      --bg: #f7f2e8;
      --ink: #16221d;
      --muted: #58645d;
      --panel: rgba(255,255,255,0.78);
      --accent: #0f766e;
      --accent-soft: #c7efe6;
      --warn: #f59e0b;
      --bad: #dc2626;
      --good: #15803d;
      --line: rgba(22,34,29,0.12);
      --shadow: 0 18px 40px rgba(40, 56, 49, 0.12);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Georgia, "Times New Roman", serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(15,118,110,0.16), transparent 30%),
        radial-gradient(circle at top right, rgba(245,158,11,0.18), transparent 24%),
        linear-gradient(180deg, #f8f5ef 0%, #efe5d1 100%);
    }}
    .page {{
      max-width: 1200px;
      margin: 0 auto;
      padding: 32px 20px 48px;
    }}
    .hero {{
      background: linear-gradient(135deg, rgba(15,118,110,0.95), rgba(18,48,43,0.96));
      color: #f7fbf9;
      border-radius: 28px;
      padding: 28px;
      box-shadow: var(--shadow);
      overflow: hidden;
      position: relative;
    }}
    .hero::after {{
      content: "";
      position: absolute;
      inset: auto -40px -60px auto;
      width: 220px;
      height: 220px;
      border-radius: 999px;
      background: rgba(255,255,255,0.08);
    }}
    .eyebrow {{
      letter-spacing: 0.2em;
      text-transform: uppercase;
      font-size: 12px;
      opacity: 0.8;
    }}
    h1 {{
      margin: 10px 0 8px;
      font-size: clamp(34px, 5vw, 58px);
      line-height: 0.95;
    }}
    .hero-sub {{
      max-width: 720px;
      font-size: 16px;
      color: rgba(247,251,249,0.86);
    }}
    .hero-stats {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 14px;
      margin-top: 24px;
    }}
    .hero-stat {{
      background: rgba(255,255,255,0.08);
      border: 1px solid rgba(255,255,255,0.1);
      border-radius: 18px;
      padding: 14px 16px;
      backdrop-filter: blur(8px);
    }}
    .hero-stat-label {{
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.12em;
      opacity: 0.74;
    }}
    .hero-stat-value {{
      margin-top: 8px;
      font-size: 28px;
      font-weight: bold;
    }}
    .section {{
      margin-top: 26px;
    }}
    .section-head {{
      display: flex;
      justify-content: space-between;
      align-items: end;
      gap: 16px;
      margin-bottom: 12px;
    }}
    .section-title {{
      font-size: 26px;
      margin: 0;
    }}
    .section-copy {{
      color: var(--muted);
      margin: 0;
      max-width: 720px;
    }}
    .metrics-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 16px;
    }}
    .metric-card, .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 22px;
      padding: 18px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(10px);
    }}
    .metric-top {{
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: center;
    }}
    .metric-label {{
      font-size: 13px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}
    .metric-value {{
      font-size: 34px;
      margin-top: 12px;
      font-weight: bold;
    }}
    .metric-desc {{
      margin: 12px 0 0;
      color: var(--muted);
      line-height: 1.45;
      font-size: 14px;
    }}
    .pill {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      border-radius: 999px;
      padding: 6px 10px;
      font-size: 11px;
      font-weight: bold;
      letter-spacing: 0.08em;
    }}
    .pill-good {{ background: rgba(21,128,61,0.14); color: var(--good); }}
    .pill-warn {{ background: rgba(245,158,11,0.18); color: #9a6700; }}
    .pill-bad {{ background: rgba(220,38,38,0.14); color: var(--bad); }}
    .pill-info {{ background: rgba(15,118,110,0.12); color: var(--accent); }}
    .two-col {{
      display: grid;
      grid-template-columns: 1.05fr 0.95fr;
      gap: 16px;
    }}
    .table-wrap {{
      overflow-x: auto;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-family: "Segoe UI", Tahoma, sans-serif;
      font-size: 14px;
    }}
    th, td {{
      text-align: left;
      padding: 12px 10px;
      border-bottom: 1px solid var(--line);
      white-space: nowrap;
    }}
    th {{
      color: var(--muted);
      font-size: 12px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }}
    .chart-box {{
      display: grid;
      gap: 12px;
      padding-top: 10px;
    }}
    .bar-row {{
      display: grid;
      grid-template-columns: 140px 1fr 54px;
      gap: 12px;
      align-items: center;
      font-family: "Segoe UI", Tahoma, sans-serif;
    }}
    .bar-track {{
      height: 14px;
      background: rgba(15,118,110,0.08);
      border-radius: 999px;
      overflow: hidden;
    }}
    .bar-fill {{
      height: 100%;
      border-radius: 999px;
      background: linear-gradient(90deg, #0f766e, #14b8a6);
    }}
    .footer-note {{
      margin-top: 20px;
      color: var(--muted);
      font-size: 14px;
    }}
    @media (max-width: 900px) {{
      .two-col {{ grid-template-columns: 1fr; }}
      .bar-row {{ grid-template-columns: 100px 1fr 44px; }}
    }}
  </style>
</head>
<body>
  <main class="page">
    <section class="hero">
      <div class="eyebrow">Operations dashboard</div>
      <h1>Reconciliation Health</h1>
      <p class="hero-sub">A static audit dashboard for PayRecon that turns the CSV outputs into decision-ready metrics for finance and operations.</p>
      <div class="hero-stats">
        <div class="hero-stat">
          <div class="hero-stat-label">Period</div>
          <div class="hero-stat-value">{escape(f"{period_month:02d}/{period_year}")}</div>
        </div>
        <div class="hero-stat">
          <div class="hero-stat-label">Transactions</div>
          <div class="hero-stat-value">{len(recon)}</div>
        </div>
        <div class="hero-stat">
          <div class="hero-stat-label">Action Items</div>
          <div class="hero-stat-value">{len(recon[recon['gap_type'] != GAP_MATCHED])}</div>
        </div>
        <div class="hero-stat">
          <div class="hero-stat-label">Net Gap</div>
          <div class="hero-stat-value">{CURRENCY_SYMBOL}{recon['delta_inr'].sum(skipna=True):,.5f}</div>
        </div>
      </div>
    </section>

    <section class="section">
      <div class="section-head">
        <div>
          <h2 class="section-title">Evaluation Metrics</h2>
          <p class="section-copy">These KPIs help you judge whether the reconciliation engine is behaving well operationally, not just whether it ran successfully.</p>
        </div>
      </div>
      <div class="metrics-grid">
        {''.join(metric_cards)}
      </div>
    </section>

    <section class="section">
      <div class="panel">
        <div class="section-head">
          <div>
            <h2 class="section-title">Assessment Assumptions</h2>
            <p class="section-copy">{escape(DATASET_NOTE)}</p>
          </div>
        </div>
        <ul style="margin:0;padding-left:20px;color:var(--muted);line-height:1.7">
          {assumptions_html}
        </ul>
      </div>
    </section>

    <section class="section two-col">
      <div class="panel">
        <div class="section-head">
          <div>
            <h2 class="section-title">Gap Distribution</h2>
            <p class="section-copy">A compact view of where exception volume is clustering.</p>
          </div>
        </div>
        <div class="chart-box" id="gap-chart"></div>
      </div>
      <div class="panel">
        <div class="section-head">
          <div>
            <h2 class="section-title">Summary Table</h2>
            <p class="section-copy">Grouped totals by reconciliation outcome.</p>
          </div>
        </div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Gap Type</th>
                <th>Count</th>
                <th>Txn Total</th>
                <th>Settled Total</th>
                <th>Delta</th>
              </tr>
            </thead>
            <tbody>
              {''.join(summary_rows)}
            </tbody>
          </table>
        </div>
      </div>
    </section>

    <section class="section">
      <div class="panel">
        <div class="section-head">
          <div>
            <h2 class="section-title">Top Exceptions</h2>
            <p class="section-copy">The first mismatches to investigate, ordered by the reconciliation output.</p>
          </div>
        </div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Transaction</th>
                <th>Gap Type</th>
                <th>Txn Amount</th>
                <th>Settled Amount</th>
                <th>Delta</th>
              </tr>
            </thead>
            <tbody>
              {''.join(mismatch_rows_html)}
            </tbody>
          </table>
        </div>
        <p class="footer-note">This dashboard is generated as plain HTML, so you can open it directly in a browser or attach it to a reconciliation run artifact. Generated at {escape(generated_at)}.</p>
      </div>
    </section>
  </main>
  <script>
    const chartData = {chart_data};
    const host = document.getElementById("gap-chart");
    const max = Math.max(...chartData.values, 1);
    chartData.labels.forEach((label, idx) => {{
      const value = chartData.values[idx];
      const row = document.createElement("div");
      row.className = "bar-row";
      row.innerHTML = `
        <div>${{label}}</div>
        <div class="bar-track"><div class="bar-fill" style="width:${{(value / max) * 100}}%"></div></div>
        <div>${{value}}</div>
      `;
      host.appendChild(row);
    }});
  </script>
</body>
</html>
"""


if __name__ == "__main__":
    from src.analyzer import compute_evaluation_metrics, compute_summary
    from src.loader import load_bank_settlements, load_platform_transactions
    from src.reconciler import reconcile

    platform = load_platform_transactions()
    bank = load_bank_settlements()
    recon = reconcile(platform, bank)
    summary = compute_summary(recon)
    metrics = compute_evaluation_metrics(recon)

    print_tables(recon, summary, metrics)
    save_all(recon, summary, metrics)
