# Frontend HTML Structure
# Save this file as `static/index.html`

<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>QuantScreener Dashboard</title>
    <link rel="stylesheet" href="/static/style.css">
    <script src="https://unpkg.com/lightweight-charts/dist/lightweight-charts.standalone.production.js"></script>
</head>
<body>
    <div class="dashboard-container">
        <aside class="screener-panel">
            <h2>Today's Momentum Triggers</h2>
            <div id="market-regime-warning" class="hidden">
                ⚠️ Bearish Regime: Proceed with caution.
            </div>
            <table id="screener-table">
                <thead>
                    <tr>
                        <th>Ticker</th>
                        <th>RVOL</th>
                        <th>ATR (%)</th>
                    </tr>
                </thead>
                <tbody id="screener-body">
                    </tbody>
            </table>
        </aside>

        <main class="chart-panel">
            <header class="ticker-header">
                <h1 id="active-ticker">Select a Ticker</h1>
                <div class="metrics-grid">
                    <div class="metric-card">Win Rate: <span id="win-rate">--</span>%</div>
                    <div class="metric-card">Profit Factor: <span id="profit-factor">--</span></div>
                    <div class="metric-card">Max Drawdown: <span id="max-drawdown">--</span>%</div>
                </div>
            </header>
            <div id="tvchart"></div>
        </main>
    </div>
    <script src="/static/app.js"></script>
</body>
</html>