# forecasting/visualise_forecasts.py
# Generates forecast dashboard as a self-contained HTML file
# Uses Chart.js — no Plotly/kaleido dependency required
# Usage: python forecasting/visualise_forecasts.py
# Output: forecasting/dashboard.html

import duckdb
import json
from pathlib import Path
from datetime import datetime

DB_PATH   = Path("warehouse.db")
OUT_PATH  = Path("forecasting/dashboard.html")
OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

# ── Colour maps ────────────────────────────────────────────────────────────────

CATEGORY_COLOURS = {
    "fresh_produce": "#1D9E75",
    "dairy":         "#378ADD",
    "bakery":        "#BA7517",
    "meat_seafood":  "#D85A30",
    "pantry":        "#7F77DD",
    "frozen":        "#0F6E56",
    "beverages":     "#EF9F27",
    "health_beauty": "#D4537E",
}

STATUS_COLOURS = {
    "stockout": "#E24B4A",
    "critical": "#D85A30",
    "low":      "#EF9F27",
    "healthy":  "#639922",
}

CLUSTER_ORDER = [
    "metro_large", "metro_small", "suburban",
    "regional", "rural", "convenience",
]


# ── Data loading ───────────────────────────────────────────────────────────────

def load_all(con: duckdb.DuckDBPyConnection) -> dict:
    print("  Loading data...")

    # Weekly historical sales — main_mart schema
    sales_weekly = con.execute("""
        SELECT
            STRFTIME(DATE_TRUNC('week', sale_date), '%Y-%m-%d') AS week_start,
            category,
            SUM(quantity)   AS total_quantity,
            SUM(revenue)    AS total_revenue
        FROM main_mart.fct_sales
        GROUP BY DATE_TRUNC('week', sale_date), category
        ORDER BY week_start, category
    """).df()

    # Forecasts — demand schema
    forecasts = con.execute("""
        SELECT
            STRFTIME(CAST(forecast_date AS DATE), '%Y-%m-%d') AS forecast_date,
            category,
            ROUND(yhat,       2) AS yhat,
            ROUND(yhat_lower, 2) AS yhat_lower,
            ROUND(yhat_upper, 2) AS yhat_upper
        FROM demand.fct_forecasts
        ORDER BY category, forecast_date
    """).df()

    # Model metadata — demand schema
    metadata = con.execute("""
        SELECT
            category,
            ROUND(mape, 2)  AS mape,
            ROUND(rmse, 2)  AS rmse,
            n_weeks
        FROM demand.forecast_metadata
        ORDER BY mape ASC
    """).df()

    # Inventory status — main_mart schema
    inventory = con.execute("""
        SELECT
            category,
            store_cluster,
            stock_status,
            COUNT(*) AS cnt
        FROM main_mart.fct_inventory_snapshots
        WHERE snapshot_date = (
            SELECT MAX(snapshot_date)
            FROM main_mart.fct_inventory_snapshots
        )
        GROUP BY category, store_cluster, stock_status
        ORDER BY category, store_cluster, stock_status
    """).df()

    # Reorder urgency — demand schema
    reorders = con.execute("""
        SELECT
            category,
            stock_status,
            COUNT(*)                                         AS total,
            SUM(CASE WHEN should_reorder THEN 1 ELSE 0 END) AS needs_reorder
        FROM demand.reorder_recommendations
        WHERE stock_status IN ('stockout', 'critical', 'low')
        GROUP BY category, stock_status
        ORDER BY category, stock_status
    """).df()

    # Monthly seasonal averages
    seasonal = con.execute("""
        SELECT
            EXTRACT(MONTH FROM sale_date)   AS month_num,
            STRFTIME(sale_date, '%b')        AS month_name,
            category,
            ROUND(AVG(quantity), 1)          AS avg_daily_qty
        FROM main_mart.fct_sales
        GROUP BY
            EXTRACT(MONTH FROM sale_date),
            STRFTIME(sale_date, '%b'),
            category
        ORDER BY month_num, category
    """).df()

    print("  ✓  All data loaded")
    return {
        "sales_weekly": sales_weekly,
        "forecasts":    forecasts,
        "metadata":     metadata,
        "inventory":    inventory,
        "reorders":     reorders,
        "seasonal":     seasonal,
    }


# ── Chart data builders ────────────────────────────────────────────────────────

def build_forecast_chart_data(data: dict) -> dict:
    """
    Per-category: historical weekly qty + forecast yhat + CI band.
    Returns one dataset object per category for Chart.js.
    """
    sales     = data["sales_weekly"]
    forecasts = data["forecasts"]
    categories = list(CATEGORY_COLOURS.keys())

    # Collect all weeks (historical + forecast) sorted
    all_weeks = sorted(set(
        sales["week_start"].tolist() +
        forecasts["forecast_date"].tolist()
    ))

    # Last historical date — used to draw the vertical divider
    last_hist = sales["week_start"].max()

    datasets = []
    for cat in categories:
        colour = CATEGORY_COLOURS[cat]
        hist   = sales[sales["category"] == cat].set_index("week_start")["total_quantity"].to_dict()
        fcast  = forecasts[forecasts["category"] == cat].set_index("forecast_date")

        hist_data  = [round(hist.get(w, None) or 0) for w in all_weeks]
        fcast_data = []
        lower_data = []
        upper_data = []
        for w in all_weeks:
            if w in fcast.index:
                fcast_data.append(round(fcast.loc[w, "yhat"]))
                lower_data.append(round(fcast.loc[w, "yhat_lower"]))
                upper_data.append(round(fcast.loc[w, "yhat_upper"]))
            else:
                fcast_data.append(None)
                lower_data.append(None)
                upper_data.append(None)

        datasets.append({
            "category":  cat,
            "colour":    colour,
            "hist":      hist_data,
            "forecast":  fcast_data,
            "lower":     lower_data,
            "upper":     upper_data,
        })

    return {
        "labels":    all_weeks,
        "last_hist": last_hist,
        "datasets":  datasets,
    }


def build_mape_data(data: dict) -> dict:
    meta = data["metadata"].sort_values("mape")
    return {
        "labels": meta["category"].tolist(),
        "mape":   meta["mape"].tolist(),
        "colours": [
            "#639922" if m < 25 else
            "#378ADD" if m < 60 else
            "#EF9F27" if m < 100 else
            "#E24B4A"
            for m in meta["mape"].tolist()
        ],
    }


def build_stockout_data(data: dict) -> dict:
    inv = data["inventory"]
    total = inv.groupby(["category", "store_cluster"])["cnt"].sum().reset_index()
    stockouts = inv[inv["stock_status"] == "stockout"].groupby(
        ["category", "store_cluster"]
    )["cnt"].sum().reset_index().rename(columns={"cnt": "so_cnt"})

    merged = total.merge(stockouts, on=["category", "store_cluster"], how="left")
    merged["so_cnt"] = merged["so_cnt"].fillna(0)
    merged["pct"]    = (merged["so_cnt"] / merged["cnt"] * 100).round(1)

    categories = list(CATEGORY_COLOURS.keys())
    clusters   = [c for c in CLUSTER_ORDER
                  if c in merged["store_cluster"].unique()]

    # Build heatmap matrix
    matrix = []
    for cat in categories:
        row = []
        for cl in clusters:
            val = merged[
                (merged["category"] == cat) &
                (merged["store_cluster"] == cl)
            ]["pct"]
            row.append(float(val.values[0]) if len(val) > 0 else 0.0)
        matrix.append(row)

    return {
        "categories": categories,
        "clusters":   clusters,
        "matrix":     matrix,
    }


def build_reorder_data(data: dict) -> dict:
    reorders   = data["reorders"]
    categories = list(CATEGORY_COLOURS.keys())
    statuses   = ["stockout", "critical", "low"]

    datasets = []
    for status in statuses:
        sub = reorders[reorders["stock_status"] == status].set_index("category")
        datasets.append({
            "label":  status.capitalize(),
            "colour": STATUS_COLOURS[status],
            "data":   [
                int(sub.loc[cat, "needs_reorder"]) if cat in sub.index else 0
                for cat in categories
            ],
        })

    return {
        "labels":   categories,
        "datasets": datasets,
    }


def build_seasonal_data(data: dict) -> dict:
    seasonal   = data["seasonal"]
    categories = list(CATEGORY_COLOURS.keys())
    months     = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    datasets = []
    for cat in categories:
        sub = seasonal[seasonal["category"] == cat]\
                .set_index("month_name")["avg_daily_qty"].to_dict()
        datasets.append({
            "label":  cat,
            "colour": CATEGORY_COLOURS[cat],
            "data":   [round(sub.get(m, 0)) for m in months],
        })

    return {
        "labels":   months,
        "datasets": datasets,
    }


def build_summary_stats(data: dict) -> dict:
    inv      = data["inventory"]
    reorders = data["reorders"]
    meta     = data["metadata"]

    total_stockouts = int(inv[inv["stock_status"] == "stockout"]["cnt"].sum())
    total_critical  = int(inv[inv["stock_status"] == "critical"]["cnt"].sum())
    total_reorders  = int(reorders["needs_reorder"].sum())
    best_mape       = float(meta["mape"].min())
    worst_mape      = float(meta["mape"].max())

    return {
        "stockouts":   total_stockouts,
        "critical":    total_critical,
        "reorders":    total_reorders,
        "best_mape":   f"{best_mape:.1f}%",
        "worst_mape":  f"{worst_mape:.1f}%",
        "models":      len(meta),
        "generated":   datetime.now().strftime("%d %b %Y %H:%M"),
    }


# ── HTML generation ────────────────────────────────────────────────────────────

def build_html(chart_data: dict, stats: dict) -> str:
    fc_json       = json.dumps(chart_data["forecast"])
    mape_json     = json.dumps(chart_data["mape"])
    stockout_json = json.dumps(chart_data["stockout"])
    reorder_json  = json.dumps(chart_data["reorder"])
    seasonal_json = json.dumps(chart_data["seasonal"])

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Supply Chain Intelligence — Forecast Dashboard</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<style>
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
  :root {{
    --bg:      #f5f4f0;
    --surface: #ffffff;
    --border:  rgba(0,0,0,0.08);
    --text:    #2c2c2a;
    --muted:   #73726c;
    --radius:  12px;
  }}
  @media (prefers-color-scheme: dark) {{
    :root {{
      --bg:      #1a1a18;
      --surface: #232321;
      --border:  rgba(255,255,255,0.08);
      --text:    #e8e6de;
      --muted:   #888780;
    }}
  }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    background: var(--bg);
    color: var(--text);
    padding: 24px 20px 48px;
    max-width: 1200px;
    margin: 0 auto;
  }}
  h1 {{
    font-size: 22px;
    font-weight: 500;
    margin-bottom: 4px;
  }}
  .subtitle {{
    font-size: 13px;
    color: var(--muted);
    margin-bottom: 28px;
  }}
  .metrics {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
    gap: 12px;
    margin-bottom: 28px;
  }}
  .metric {{
    background: var(--surface);
    border: 0.5px solid var(--border);
    border-radius: var(--radius);
    padding: 14px 16px;
  }}
  .metric-label {{
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    color: var(--muted);
    margin-bottom: 6px;
  }}
  .metric-value {{
    font-size: 24px;
    font-weight: 500;
    line-height: 1;
  }}
  .metric-value.warn  {{ color: #EF9F27; }}
  .metric-value.bad   {{ color: #E24B4A; }}
  .metric-value.good  {{ color: #639922; }}
  .card {{
    background: var(--surface);
    border: 0.5px solid var(--border);
    border-radius: var(--radius);
    padding: 20px 20px 16px;
    margin-bottom: 20px;
  }}
  .card h2 {{
    font-size: 14px;
    font-weight: 500;
    margin-bottom: 4px;
  }}
  .card-desc {{
    font-size: 12px;
    color: var(--muted);
    margin-bottom: 16px;
  }}
  .chart-wrap {{
    position: relative;
    width: 100%;
  }}
  .legend {{
    display: flex;
    flex-wrap: wrap;
    gap: 10px 16px;
    margin-bottom: 12px;
  }}
  .legend-item {{
    display: flex;
    align-items: center;
    gap: 5px;
    font-size: 12px;
    color: var(--muted);
    cursor: pointer;
  }}
  .swatch {{
    width: 10px; height: 10px;
    border-radius: 2px;
    flex-shrink: 0;
  }}
  .grid-2 {{
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 20px;
  }}
  @media (max-width: 700px) {{
    .grid-2 {{ grid-template-columns: 1fr; }}
  }}
  .cat-tabs {{
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
    margin-bottom: 16px;
  }}
  .cat-tab {{
    font-size: 12px;
    padding: 4px 10px;
    border-radius: 20px;
    border: 0.5px solid var(--border);
    background: transparent;
    color: var(--muted);
    cursor: pointer;
    transition: all 0.15s;
  }}
  .cat-tab.active {{
    color: var(--text);
    border-color: currentColor;
    font-weight: 500;
  }}
  .mape-legend {{
    display: flex;
    gap: 16px;
    flex-wrap: wrap;
    margin-bottom: 12px;
    font-size: 12px;
    color: var(--muted);
  }}
  .mape-legend span {{
    display: flex;
    align-items: center;
    gap: 5px;
  }}
  .band {{
    width: 10px; height: 10px;
    border-radius: 2px;
    flex-shrink: 0;
  }}
</style>
</head>
<body>

<h1>🛒 Retail Supply Chain Intelligence</h1>
<p class="subtitle">Demand Forecast Dashboard &nbsp;·&nbsp; 12-week forward view &nbsp;·&nbsp; Generated {stats['generated']}</p>

<!-- Summary metrics -->
<div class="metrics">
  <div class="metric">
    <div class="metric-label">Models trained</div>
    <div class="metric-value">{stats['models']}</div>
  </div>
  <div class="metric">
    <div class="metric-label">Best MAPE</div>
    <div class="metric-value good">{stats['best_mape']}</div>
  </div>
  <div class="metric">
    <div class="metric-label">Worst MAPE</div>
    <div class="metric-value bad">{stats['worst_mape']}</div>
  </div>
  <div class="metric">
    <div class="metric-label">Active stockouts</div>
    <div class="metric-value bad">{stats['stockouts']:,}</div>
  </div>
  <div class="metric">
    <div class="metric-label">Critical stock</div>
    <div class="metric-value warn">{stats['critical']:,}</div>
  </div>
  <div class="metric">
    <div class="metric-label">Reorders needed</div>
    <div class="metric-value warn">{stats['reorders']:,}</div>
  </div>
</div>

<!-- Chart 1: Forecast vs actuals per category -->
<div class="card">
  <h2>Demand forecast vs historical sales</h2>
  <p class="card-desc">Solid line = historical weekly units sold. Dashed line = Prophet forecast. Shaded band = 80% confidence interval. Vertical line marks forecast start.</p>
  <div class="cat-tabs" id="cat-tabs"></div>
  <div class="chart-wrap" style="height:300px">
    <canvas id="forecastChart"></canvas>
  </div>
</div>

<!-- Chart 2 + 3: side by side -->
<div class="grid-2">

  <div class="card">
    <h2>Forecast accuracy by category (MAPE)</h2>
    <p class="card-desc">Lower is better. Grocery benchmark: &lt;25% excellent, &lt;60% acceptable, &gt;100% unreliable.</p>
    <div class="mape-legend">
      <span><span class="band" style="background:#639922"></span>Excellent &lt;25%</span>
      <span><span class="band" style="background:#378ADD"></span>Acceptable &lt;60%</span>
      <span><span class="band" style="background:#EF9F27"></span>Poor &lt;100%</span>
      <span><span class="band" style="background:#E24B4A"></span>Unreliable &gt;100%</span>
    </div>
    <div class="chart-wrap" style="height:280px">
      <canvas id="mapeChart"></canvas>
    </div>
  </div>

  <div class="card">
    <h2>Reorder urgency by category</h2>
    <p class="card-desc">Store/product combinations requiring a purchase order, grouped by urgency level.</p>
    <div class="legend" id="reorder-legend"></div>
    <div class="chart-wrap" style="height:280px">
      <canvas id="reorderChart"></canvas>
    </div>
  </div>

</div>

<!-- Chart 4: Seasonal heatmap (simulated with bar chart) -->
<div class="card">
  <h2>Seasonal demand patterns</h2>
  <p class="card-desc">Average daily units sold per category by month. Reveals summer/winter peaks and promotional seasonality.</p>
  <div class="legend" id="seasonal-legend"></div>
  <div class="chart-wrap" style="height:320px">
    <canvas id="seasonalChart"></canvas>
  </div>
</div>

<script>
const FC_DATA       = {fc_json};
const MAPE_DATA     = {mape_json};
const STOCKOUT_DATA = {stockout_json};
const REORDER_DATA  = {reorder_json};
const SEASONAL_DATA = {seasonal_json};

const isDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
const gridColor  = isDark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.06)';
const tickColor  = isDark ? '#888780' : '#73726c';
const labelColor = isDark ? '#e8e6de' : '#2c2c2a';

Chart.defaults.font.family = '-apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif';
Chart.defaults.font.size   = 12;

// ── Chart 1: Forecast per category ──────────────────────────────────────────

let activeCat = FC_DATA.datasets[0].category;
let forecastChart;

function buildForecastDatasets(cat) {{
  const d = FC_DATA.datasets.find(x => x.category === cat);
  if (!d) return [];
  const col = d.colour;
  const hex2rgba = (hex, a) => {{
    const r = parseInt(hex.slice(1,3),16);
    const g = parseInt(hex.slice(3,5),16);
    const b = parseInt(hex.slice(5,7),16);
    return `rgba(${{r}},${{g}},${{b}},${{a}})`;
  }};
  return [
    {{
      label: 'Historical',
      data:  d.hist,
      borderColor: col,
      backgroundColor: 'transparent',
      borderWidth: 2,
      pointRadius: 0,
      tension: 0.3,
    }},
    {{
      label: 'Forecast',
      data:  d.forecast,
      borderColor: col,
      backgroundColor: 'transparent',
      borderWidth: 2,
      borderDash: [5,4],
      pointRadius: 0,
      tension: 0.3,
    }},
    {{
      label: '80% CI upper',
      data:  d.upper,
      borderColor: 'transparent',
      backgroundColor: hex2rgba(col, 0.12),
      fill: '+1',
      pointRadius: 0,
      tension: 0.3,
    }},
    {{
      label: '80% CI lower',
      data:  d.lower,
      borderColor: 'transparent',
      backgroundColor: 'transparent',
      fill: false,
      pointRadius: 0,
      tension: 0.3,
    }},
  ];
}}

function initForecastChart() {{
  const ctx = document.getElementById('forecastChart').getContext('2d');
  const lastHistIdx = FC_DATA.labels.indexOf(FC_DATA.last_hist);

  forecastChart = new Chart(ctx, {{
    type: 'line',
    data: {{
      labels: FC_DATA.labels,
      datasets: buildForecastDatasets(activeCat),
    }},
    options: {{
      responsive: true,
      maintainAspectRatio: false,
      interaction: {{ mode: 'index', intersect: false }},
      plugins: {{
        legend: {{ display: false }},
        tooltip: {{
          callbacks: {{
            title: (items) => items[0].label,
            label: (item) => {{
              if (item.datasetIndex > 1) return null;
              const label = item.datasetIndex === 0 ? 'Actual' : 'Forecast';
              const val = item.raw;
              return val != null ? `${{label}}: ${{Math.round(val).toLocaleString()}} units` : null;
            }},
          }},
        }},
        annotation: {{
          annotations: {{
            divider: {{
              type: 'line',
              xMin: lastHistIdx,
              xMax: lastHistIdx,
              borderColor: gridColor,
              borderWidth: 1.5,
              borderDash: [4,3],
            }}
          }}
        }}
      }},
      scales: {{
        x: {{
          ticks: {{
            color: tickColor,
            maxRotation: 30,
            autoSkip: true,
            maxTicksLimit: 12,
          }},
          grid: {{ color: gridColor }},
        }},
        y: {{
          ticks: {{
            color: tickColor,
            callback: (v) => v >= 1000 ? (v/1000).toFixed(0)+'k' : v,
          }},
          grid: {{ color: gridColor }},
          title: {{
            display: true,
            text: 'Weekly units',
            color: tickColor,
            font: {{ size: 11 }},
          }},
        }},
      }},
    }},
  }});
}}

function buildCatTabs() {{
  const wrap = document.getElementById('cat-tabs');
  FC_DATA.datasets.forEach(d => {{
    const btn = document.createElement('button');
    btn.className = 'cat-tab' + (d.category === activeCat ? ' active' : '');
    btn.textContent = d.category.replace('_', ' ');
    btn.style.borderColor = d.category === activeCat ? d.colour : '';
    btn.style.color       = d.category === activeCat ? d.colour : '';
    btn.onclick = () => {{
      activeCat = d.category;
      wrap.querySelectorAll('.cat-tab').forEach((b,i) => {{
        b.classList.toggle('active', FC_DATA.datasets[i].category === activeCat);
        b.style.borderColor = FC_DATA.datasets[i].category === activeCat ? FC_DATA.datasets[i].colour : '';
        b.style.color       = FC_DATA.datasets[i].category === activeCat ? FC_DATA.datasets[i].colour : '';
      }});
      forecastChart.data.datasets = buildForecastDatasets(activeCat);
      forecastChart.update();
    }};
    wrap.appendChild(btn);
  }});
}}


// ── Chart 2: MAPE horizontal bar ─────────────────────────────────────────────

function initMapeChart() {{
  const ctx = document.getElementById('mapeChart').getContext('2d');
  new Chart(ctx, {{
    type: 'bar',
    data: {{
      labels: MAPE_DATA.labels.map(l => l.replace('_',' ')),
      datasets: [{{
        data:            MAPE_DATA.mape,
        backgroundColor: MAPE_DATA.colours,
        borderRadius:    4,
        borderSkipped:   false,
      }}],
    }},
    options: {{
      indexAxis: 'y',
      responsive: true,
      maintainAspectRatio: false,
      plugins: {{
        legend: {{ display: false }},
        tooltip: {{
          callbacks: {{
            label: (item) => ` MAPE: ${{item.raw.toFixed(1)}}%`,
          }},
        }},
      }},
      scales: {{
        x: {{
          ticks: {{ color: tickColor, callback: (v) => v+'%' }},
          grid:  {{ color: gridColor }},
          title: {{ display: true, text: 'MAPE (%)', color: tickColor, font: {{ size: 11 }} }},
        }},
        y: {{
          ticks: {{ color: tickColor }},
          grid:  {{ display: false }},
        }},
      }},
    }},
  }});
}}


// ── Chart 3: Reorder urgency stacked bar ─────────────────────────────────────

function initReorderChart() {{
  const ctx    = document.getElementById('reorderChart').getContext('2d');
  const legend = document.getElementById('reorder-legend');
  REORDER_DATA.datasets.forEach(d => {{
    legend.innerHTML += `<span class="legend-item">
      <span class="swatch" style="background:${{d.colour}}"></span>${{d.label}}
    </span>`;
  }});
  new Chart(ctx, {{
    type: 'bar',
    data: {{
      labels: REORDER_DATA.labels.map(l => l.replace('_',' ')),
      datasets: REORDER_DATA.datasets.map(d => ({{
        label:           d.label,
        data:            d.data,
        backgroundColor: d.colour,
        borderRadius:    d.label === 'Low' ? 4 : 0,
        borderSkipped:   false,
      }})),
    }},
    options: {{
      responsive: true,
      maintainAspectRatio: false,
      plugins: {{
        legend: {{ display: false }},
        tooltip: {{
          callbacks: {{
            label: (item) => ` ${{item.dataset.label}}: ${{item.raw.toLocaleString()}}`,
          }},
        }},
      }},
      scales: {{
        x: {{
          stacked: true,
          ticks: {{ color: tickColor, maxRotation: 30 }},
          grid:  {{ display: false }},
        }},
        y: {{
          stacked: true,
          ticks: {{ color: tickColor }},
          grid:  {{ color: gridColor }},
          title: {{ display: true, text: 'Store/product combos', color: tickColor, font: {{ size: 11 }} }},
        }},
      }},
    }},
  }});
}}


// ── Chart 4: Seasonal line chart ──────────────────────────────────────────────

function initSeasonalChart() {{
  const ctx    = document.getElementById('seasonalChart').getContext('2d');
  const legend = document.getElementById('seasonal-legend');
  SEASONAL_DATA.datasets.forEach(d => {{
    legend.innerHTML += `<span class="legend-item">
      <span class="swatch" style="background:${{d.colour}}"></span>${{d.label.replace('_',' ')}}
    </span>`;
  }});
  new Chart(ctx, {{
    type: 'line',
    data: {{
      labels: SEASONAL_DATA.labels,
      datasets: SEASONAL_DATA.datasets.map(d => ({{
        label:           d.label.replace('_',' '),
        data:            d.data,
        borderColor:     d.colour,
        backgroundColor: 'transparent',
        borderWidth:     2,
        pointRadius:     3,
        pointHoverRadius:5,
        tension:         0.4,
      }})),
    }},
    options: {{
      responsive: true,
      maintainAspectRatio: false,
      interaction: {{ mode: 'index', intersect: false }},
      plugins: {{
        legend: {{ display: false }},
        tooltip: {{
          callbacks: {{
            label: (item) => ` ${{item.dataset.label}}: ${{Math.round(item.raw).toLocaleString()}} units/day`,
          }},
        }},
      }},
      scales: {{
        x: {{
          ticks: {{ color: tickColor, autoSkip: false }},
          grid:  {{ color: gridColor }},
        }},
        y: {{
          ticks: {{
            color: tickColor,
            callback: (v) => v >= 1000 ? (v/1000).toFixed(0)+'k' : v,
          }},
          grid:  {{ color: gridColor }},
          title: {{ display: true, text: 'Avg daily units', color: tickColor, font: {{ size: 11 }} }},
        }},
      }},
    }},
  }});
}}


// ── Init all ──────────────────────────────────────────────────────────────────
buildCatTabs();
initForecastChart();
initMapeChart();
initReorderChart();
initSeasonalChart();
</script>
</body>
</html>"""


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print(f"\n📊  Supply Chain Intelligence — Dashboard Builder")
    print(f"    Database : {DB_PATH}")
    print(f"    Output   : {OUT_PATH}\n")

    if not DB_PATH.exists():
        print("  ✗  warehouse.db not found")
        return

    con  = duckdb.connect(str(DB_PATH))
    data = load_all(con)
    con.close()

    chart_data = {
        "forecast": build_forecast_chart_data(data),
        "mape":     build_mape_data(data),
        "stockout": build_stockout_data(data),
        "reorder":  build_reorder_data(data),
        "seasonal": build_seasonal_data(data),
    }

    stats = build_summary_stats(data)
    html  = build_html(chart_data, stats)

    OUT_PATH.write_text(html, encoding="utf-8")

    print(f"  ✓  Dashboard written to {OUT_PATH}")
    print(f"     Open: file://{OUT_PATH.resolve()}\n")
    print("Done\n")


if __name__ == "__main__":
    main()