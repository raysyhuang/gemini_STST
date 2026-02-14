/* ============================================================
   QuantScreener — Frontend Application
   ============================================================ */

// ---- TradingView Lightweight Chart setup ----
const chartContainer = document.getElementById('tvchart');
let chart = null;
let equitySeries = null;

function initChart() {
    chart = LightweightCharts.createChart(chartContainer, {
        width: chartContainer.clientWidth,
        height: chartContainer.clientHeight || 500,
        layout: {
            background: { type: 'solid', color: '#0f111a' },
            textColor: '#8b949e',
            fontFamily: "'Inter', sans-serif",
            fontSize: 12,
        },
        grid: {
            vertLines: { color: 'rgba(48,54,61,0.4)' },
            horzLines: { color: 'rgba(48,54,61,0.4)' },
        },
        crosshair: {
            mode: LightweightCharts.CrosshairMode.Normal,
            vertLine: { color: '#2962FF', width: 1, style: 2 },
            horzLine: { color: '#2962FF', width: 1, style: 2 },
        },
        rightPriceScale: {
            borderColor: '#30363d',
        },
        timeScale: {
            borderColor: '#30363d',
            timeVisible: false,
        },
    });

    equitySeries = chart.addLineSeries({
        color: '#2962FF',
        lineWidth: 2,
        crosshairMarkerVisible: true,
        crosshairMarkerRadius: 4,
        priceFormat: { type: 'custom', formatter: (p) => '$' + p.toFixed(0) },
    });
}

// Resize chart when window resizes
window.addEventListener('resize', () => {
    if (chart) {
        chart.applyOptions({
            width: chartContainer.clientWidth,
            height: chartContainer.clientHeight,
        });
    }
});

// ---- State ----
let screenerData = null;    // Cached momentum screener response
let reversionData = null;   // Cached reversion screener response
let activeView = 'momentum'; // 'momentum' | 'reversion' | 'performance'
let minQualityFilter = 0;   // Quality score filter threshold

// ---- Vol-Scaled Sizing Constants (match backtester) ----
const ACCOUNT_SIZE = 10000;
const TARGET_RISK = 0.01;
const MIN_SIZE = 0.05;
const MAX_SIZE = 0.20;

// ---- DOM references ----
const panelTitle     = document.getElementById('panel-title');
const screenerBody   = document.getElementById('screener-body');
const screenerTable  = document.getElementById('screener-table');
const reversionBody  = document.getElementById('reversion-body');
const reversionTable = document.getElementById('reversion-table');
const screenerEmpty  = document.getElementById('screener-empty');
const screenerDate   = document.getElementById('screener-date');
const regimeWarn     = document.getElementById('market-regime-warning');
const regimeBull     = document.getElementById('market-regime-bullish');
const newsPanel      = document.getElementById('news-panel');
const newsList       = document.getElementById('news-list');
const newsLabel      = document.getElementById('news-ticker-label');
const activeTicker   = document.getElementById('active-ticker');
const chartPlaceholder = document.getElementById('chart-placeholder');
const loadingOverlay = document.getElementById('loading-overlay');
const btnMomentum    = document.getElementById('btn-momentum');
const btnReversion   = document.getElementById('btn-reversion');
const btnPerformance = document.getElementById('btn-performance');
const perfPanel      = document.getElementById('performance-panel');
const perfMetricsGrid = document.getElementById('perf-metrics-grid');
const tradeLogTable  = document.getElementById('trade-log-table');
const tradeLogBody   = document.getElementById('trade-log-body');
const perfEmpty      = document.getElementById('perf-empty');

// ---- View toggle ----

function switchView(view) {
    activeView = view;

    // Toggle button active states
    btnMomentum.classList.toggle('active', view === 'momentum');
    btnReversion.classList.toggle('active', view === 'reversion');
    btnPerformance.classList.toggle('active', view === 'performance');

    // Hide all panels first
    screenerTable.classList.add('hidden');
    reversionTable.classList.add('hidden');
    perfPanel.classList.add('hidden');
    newsPanel.classList.add('hidden');
    screenerEmpty.classList.add('hidden');

    // Show/hide quality filter (only for signal views)
    const qualityFilter = document.getElementById('quality-filter');
    qualityFilter.classList.toggle('hidden', view === 'performance');

    // Update panel title and show relevant content
    if (view === 'momentum') {
        panelTitle.textContent = 'Momentum Triggers';
        restoreBacktestChartMode();
        renderMomentumSignals();
    } else if (view === 'reversion') {
        panelTitle.textContent = 'Oversold Reversions';
        restoreBacktestChartMode();
        renderReversionSignals();
    } else if (view === 'performance') {
        panelTitle.textContent = 'Paper Trading';
        perfPanel.classList.remove('hidden');
        fetchPerformanceData();
        showEquityCurveChart();
    }
}

// ---- Quality Score + Confluence helpers ----

function formatQualityScore(score) {
    if (score == null) return '--';
    let color;
    if (score >= 70) color = 'var(--green, #3fb950)';
    else if (score >= 40) color = 'var(--yellow, #d29922)';
    else color = 'var(--text-muted, #8b949e)';
    return `<span style="color:${color};font-weight:600">${score.toFixed(0)}</span>`;
}

function formatTicker(stock) {
    const sym = stock.ticker;
    if (stock.confluence) {
        return `<span class="confluence-badge" title="Dual-strategy confluence">\u2B50</span> ${sym}`;
    }
    return sym;
}

// ---- Vol-Scaled Position Size ----

function computePositionSize(atrPct) {
    if (!atrPct || atrPct <= 0) return ACCOUNT_SIZE * 0.10;
    const frac = Math.min(Math.max(TARGET_RISK / (atrPct / 100), MIN_SIZE), MAX_SIZE);
    return Math.round(ACCOUNT_SIZE * frac);
}

function formatPositionSize(atrPct) {
    const size = computePositionSize(atrPct);
    return `$${size.toLocaleString()}`;
}

// ---- Quality Filter ----

function onQualityFilterChange() {
    const select = document.getElementById('min-quality-select');
    minQualityFilter = parseFloat(select.value) || 0;
    if (activeView === 'momentum') {
        renderMomentumSignals();
    } else if (activeView === 'reversion') {
        renderReversionSignals();
    }
}

// ---- Options Flow helper ----

function formatFlow(stock) {
    const s = stock.options_sentiment;
    const pcr = stock.put_call_ratio;
    if (!s || s === 'Neutral') {
        return pcr != null ? `<span class="flow-neutral">${pcr.toFixed(2)}</span>` : '--';
    }
    if (s === 'Bullish') {
        const label = pcr != null ? pcr.toFixed(2) : '';
        return `<span class="flow-bullish" title="P/C: ${label}">\u{1F402} ${label}</span>`;
    }
    if (s === 'Bearish') {
        const label = pcr != null ? pcr.toFixed(2) : '';
        return `<span class="flow-bearish" title="P/C: ${label}">\u{1F43B} ${label}</span>`;
    }
    return '--';
}

// ---- Momentum Screener ----

async function fetchScreenerData() {
    try {
        const resp = await fetch('/api/screener/today');
        const data = await resp.json();
        screenerData = data;

        // Date badge
        screenerDate.textContent = data.date;

        // Market regime
        regimeWarn.classList.add('hidden');
        regimeBull.classList.add('hidden');
        if (data.regime.regime === 'Bearish') {
            regimeWarn.classList.remove('hidden');
        } else if (data.regime.regime === 'Bullish') {
            regimeBull.classList.remove('hidden');
        }

        if (activeView === 'momentum') {
            renderMomentumSignals();
        }

    } catch (err) {
        console.error('Error fetching screener data:', err);
    }
}

function renderMomentumSignals() {
    if (!screenerData || screenerData.signals.length === 0) {
        screenerTable.classList.add('hidden');
        screenerEmpty.classList.remove('hidden');
        return;
    }

    // Apply quality filter
    const filtered = screenerData.signals.filter(
        s => (s.quality_score || 0) >= minQualityFilter
    );

    if (filtered.length === 0) {
        screenerTable.classList.add('hidden');
        screenerEmpty.classList.remove('hidden');
        return;
    }

    screenerEmpty.classList.add('hidden');
    screenerTable.classList.remove('hidden');
    screenerBody.innerHTML = '';

    filtered.forEach((stock, idx) => {
        const origIdx = screenerData.signals.indexOf(stock);
        const tr = document.createElement('tr');
        tr.dataset.idx = origIdx;
        if (stock.confluence) tr.classList.add('confluence-row');
        tr.innerHTML = `
            <td>${formatTicker(stock)}</td>
            <td>${formatQualityScore(stock.quality_score)}</td>
            <td>${stock.rvol_at_trigger.toFixed(2)}</td>
            <td>${stock.atr_pct_at_trigger.toFixed(1)}%</td>
            <td>${formatPositionSize(stock.atr_pct_at_trigger)}</td>
            <td>${formatFlow(stock)}</td>
            <td>$${stock.trigger_price.toFixed(2)}</td>
        `;
        tr.addEventListener('click', () => onMomentumClick(origIdx));
        screenerBody.appendChild(tr);
    });
}

function onMomentumClick(idx) {
    const stock = screenerData.signals[idx];

    // Highlight active row
    document.querySelectorAll('#screener-body tr').forEach(r => r.classList.remove('active'));
    document.querySelector(`#screener-body tr[data-idx="${idx}"]`).classList.add('active');

    // Show news
    showNews(stock);

    // Load backtest (momentum strategy)
    loadBacktest(stock.ticker, 'momentum');
}

// ---- Reversion Screener ----

async function fetchReversionData() {
    try {
        const resp = await fetch('/api/reversion/today');
        const data = await resp.json();
        reversionData = data;

        if (activeView === 'reversion') {
            screenerDate.textContent = data.date;
            renderReversionSignals();
        }

    } catch (err) {
        console.error('Error fetching reversion data:', err);
    }
}

function renderReversionSignals() {
    if (!reversionData || reversionData.signals.length === 0) {
        reversionTable.classList.add('hidden');
        screenerEmpty.classList.remove('hidden');
        return;
    }

    // Apply quality filter
    const filtered = reversionData.signals.filter(
        s => (s.quality_score || 0) >= minQualityFilter
    );

    if (filtered.length === 0) {
        reversionTable.classList.add('hidden');
        screenerEmpty.classList.remove('hidden');
        return;
    }

    screenerEmpty.classList.add('hidden');
    reversionTable.classList.remove('hidden');
    reversionBody.innerHTML = '';

    filtered.forEach((stock, idx) => {
        const origIdx = reversionData.signals.indexOf(stock);
        const tr = document.createElement('tr');
        tr.dataset.idx = origIdx;
        if (stock.confluence) tr.classList.add('confluence-row');
        // Reversion signals need atr_pct for sizing; use a default if not present
        const atrPct = stock.atr_pct_at_trigger || 10;
        tr.innerHTML = `
            <td>${formatTicker(stock)}</td>
            <td>${formatQualityScore(stock.quality_score)}</td>
            <td>${stock.rsi2.toFixed(1)}</td>
            <td>${stock.drawdown_3d_pct.toFixed(1)}%</td>
            <td>${formatPositionSize(atrPct)}</td>
            <td>${formatFlow(stock)}</td>
            <td>$${stock.trigger_price.toFixed(2)}</td>
        `;
        tr.addEventListener('click', () => onReversionClick(origIdx));
        reversionBody.appendChild(tr);
    });
}

function onReversionClick(idx) {
    const stock = reversionData.signals[idx];

    // Highlight active row
    document.querySelectorAll('#reversion-body tr').forEach(r => r.classList.remove('active'));
    document.querySelector(`#reversion-body tr[data-idx="${idx}"]`).classList.add('active');

    // Hide news (reversion doesn't have inline news)
    newsPanel.classList.add('hidden');

    // Load backtest (reversion strategy)
    loadBacktest(stock.ticker, 'reversion');
}

// ---- News ----

function showNews(stock) {
    if (!stock.news || stock.news.length === 0) {
        newsPanel.classList.add('hidden');
        return;
    }

    newsLabel.textContent = `${stock.ticker} — Latest News`;
    newsList.innerHTML = '';

    stock.news.forEach(article => {
        const li = document.createElement('li');
        li.innerHTML = `
            <a href="${article.url}" target="_blank" rel="noopener">${article.headline}</a>
            <div class="news-meta">${article.source} &middot; ${article.published}</div>
        `;
        newsList.appendChild(li);
    });

    newsPanel.classList.remove('hidden');
}

// ---- Backtest ----

async function loadBacktest(ticker, strategy = 'momentum') {
    const label = strategy === 'reversion' ? 'Reversion BT' : 'Backtest';
    activeTicker.textContent = `${label}: ${ticker}`;
    chartPlaceholder.classList.add('hidden');
    loadingOverlay.classList.remove('hidden');

    // Lazy-init the chart on first use
    if (!chart) {
        initChart();
    }

    try {
        const resp = await fetch(`/api/backtest/${ticker}?strategy=${strategy}`);
        if (!resp.ok) {
            const err = await resp.json();
            console.error('Backtest error:', err.detail);
            activeTicker.textContent = `${ticker} — ${err.detail}`;
            loadingOverlay.classList.add('hidden');
            return;
        }

        const data = await resp.json();

        // Update metric cards
        document.getElementById('win-rate').textContent      = data.win_rate.toFixed(1);
        document.getElementById('profit-factor').textContent  = data.profit_factor.toFixed(2);
        document.getElementById('max-drawdown').textContent   = data.max_drawdown_pct.toFixed(1);
        document.getElementById('total-trades').textContent   = data.total_trades;
        document.getElementById('total-return').textContent   = data.total_return_pct.toFixed(1);
        document.getElementById('avg-pos-size').textContent   = data.avg_position_size_pct.toFixed(1);

        // Color-code return
        const retEl = document.getElementById('total-return').parentElement;
        retEl.style.color = data.total_return_pct >= 0 ? '#3fb950' : '#f85149';

        // Render equity curve with blue line for backtest
        equitySeries.applyOptions({ color: '#2962FF' });
        equitySeries.setData(data.equity_curve);
        chart.timeScale().fitContent();

    } catch (err) {
        console.error(`Error fetching backtest for ${ticker}:`, err);
    } finally {
        loadingOverlay.classList.add('hidden');
    }
}

// ---- Performance Tab ----

async function fetchPerformanceData() {
    try {
        const [metricsResp, tradesResp] = await Promise.all([
            fetch('/api/paper/metrics'),
            fetch('/api/paper/trades?status=all'),
        ]);
        const metrics = await metricsResp.json();
        const tradesData = await tradesResp.json();

        renderPerfMetrics(metrics);
        renderTradeLog(tradesData.trades);
    } catch (err) {
        console.error('Error fetching performance data:', err);
    }
}

function renderPerfMetrics(m) {
    const pnlColor = m.total_pnl >= 0 ? 'var(--green)' : 'var(--red)';
    const avgColor = m.avg_return_pct >= 0 ? 'var(--green)' : 'var(--red)';

    perfMetricsGrid.innerHTML = `
        <div class="perf-metric-card">
            <span class="metric-label">Total PnL</span>
            <span class="metric-value" style="color:${pnlColor}">$${m.total_pnl.toFixed(2)}</span>
        </div>
        <div class="perf-metric-card">
            <span class="metric-label">Win Rate</span>
            <span class="metric-value">${m.win_rate.toFixed(1)}%</span>
        </div>
        <div class="perf-metric-card">
            <span class="metric-label">Profit Factor</span>
            <span class="metric-value">${m.profit_factor.toFixed(2)}</span>
        </div>
        <div class="perf-metric-card">
            <span class="metric-label">Trades</span>
            <span class="metric-value">${m.closed_trades} <small style="color:var(--text-muted)">/ ${m.total_trades}</small></span>
        </div>
        <div class="perf-metric-card">
            <span class="metric-label">Avg Return</span>
            <span class="metric-value" style="color:${avgColor}">${m.avg_return_pct.toFixed(2)}%</span>
        </div>
        <div class="perf-metric-card">
            <span class="metric-label">Avg Hold</span>
            <span class="metric-value">${m.avg_hold_days.toFixed(1)}d</span>
        </div>
    `;
}

function renderTradeLog(trades) {
    if (!trades || trades.length === 0) {
        tradeLogTable.classList.add('hidden');
        perfEmpty.classList.remove('hidden');
        return;
    }

    perfEmpty.classList.add('hidden');
    tradeLogTable.classList.remove('hidden');
    tradeLogBody.innerHTML = '';

    trades.forEach(t => {
        const tr = document.createElement('tr');
        const pnlVal = t.pnl_pct != null ? `${t.pnl_pct.toFixed(2)}%` : '--';
        const pnlClass = t.pnl_pct > 0 ? 'pnl-positive' : t.pnl_pct < 0 ? 'pnl-negative' : '';
        const entryStr = t.entry_price != null ? `$${t.entry_price.toFixed(2)}` : '--';
        const exitStr = t.exit_price != null ? `$${t.exit_price.toFixed(2)}` : '--';

        let badgeClass = 'status-pending';
        if (t.status === 'open') badgeClass = 'status-open';
        else if (t.status === 'closed') badgeClass = 'status-closed';

        const sizeStr = t.position_size != null ? `$${t.position_size.toFixed(0)}` : '--';
        const qScore = t.quality_score != null ? formatQualityScore(t.quality_score) : '--';

        tr.innerHTML = `
            <td>${t.ticker}</td>
            <td><span class="strategy-badge strategy-${t.strategy}">${t.strategy}</span></td>
            <td>${qScore}</td>
            <td>${sizeStr}</td>
            <td>${entryStr}</td>
            <td>${exitStr}</td>
            <td class="${pnlClass}">${pnlVal}</td>
            <td><span class="status-badge ${badgeClass}">${t.status}</span></td>
        `;
        tradeLogBody.appendChild(tr);
    });
}

// ---- Equity Curve Chart (Performance Tab) ----

async function showEquityCurveChart() {
    activeTicker.textContent = 'Paper Portfolio Equity Curve';
    chartPlaceholder.classList.add('hidden');
    loadingOverlay.classList.remove('hidden');

    // Lazy-init the chart on first use
    if (!chart) {
        initChart();
    }

    try {
        const [curveResp, metricsResp] = await Promise.all([
            fetch('/api/paper/equity-curve'),
            fetch('/api/paper/metrics'),
        ]);
        const curveData = await curveResp.json();
        const metrics = await metricsResp.json();

        // Update metric card labels for paper trading mode
        document.getElementById('label-win-rate').textContent = 'Win Rate';
        document.getElementById('label-profit-factor').textContent = 'Profit Factor';
        document.getElementById('label-max-drawdown').textContent = 'Best Trade';
        document.getElementById('label-total-trades').textContent = 'Closed Trades';
        document.getElementById('label-total-return').textContent = 'Total PnL';
        document.getElementById('label-avg-pos-size').textContent = 'Avg Hold';

        // Update metric card values
        document.getElementById('win-rate-wrap').innerHTML = `<span id="win-rate">${metrics.win_rate.toFixed(1)}</span>%`;
        document.getElementById('profit-factor').textContent = metrics.profit_factor.toFixed(2);
        document.getElementById('max-drawdown-wrap').innerHTML = `<span id="max-drawdown">${metrics.best_trade_pct.toFixed(1)}</span>%`;
        document.getElementById('total-trades').textContent = metrics.closed_trades;
        const pnlSign = metrics.total_pnl >= 0 ? '+' : '';
        document.getElementById('total-return-wrap').innerHTML = `${pnlSign}$${metrics.total_pnl.toFixed(2)}`;
        document.getElementById('total-return-wrap').style.color = metrics.total_pnl >= 0 ? '#3fb950' : '#f85149';
        document.getElementById('avg-pos-size-wrap').innerHTML = `${metrics.avg_hold_days.toFixed(1)}d`;

        // Render equity curve
        if (curveData.equity_curve && curveData.equity_curve.length > 0) {
            const lineColor = metrics.total_pnl >= 0 ? '#3fb950' : '#f85149';
            equitySeries.applyOptions({ color: lineColor });
            equitySeries.setData(curveData.equity_curve);
            chart.timeScale().fitContent();
        } else {
            equitySeries.setData([]);
            chartPlaceholder.classList.remove('hidden');
        }

    } catch (err) {
        console.error('Error loading equity curve:', err);
    } finally {
        loadingOverlay.classList.add('hidden');
    }
}

function restoreBacktestChartMode() {
    // Reset metric card labels to backtest defaults
    document.getElementById('label-win-rate').textContent = 'Win Rate';
    document.getElementById('label-profit-factor').textContent = 'Profit Factor';
    document.getElementById('label-max-drawdown').textContent = 'Max Drawdown';
    document.getElementById('label-total-trades').textContent = 'Total Trades';
    document.getElementById('label-total-return').textContent = 'Total Return';
    document.getElementById('label-avg-pos-size').textContent = 'Avg Size';

    // Reset metric card values and suffixes
    document.getElementById('win-rate-wrap').innerHTML = '<span id="win-rate">--</span>%';
    document.getElementById('profit-factor').textContent = '--';
    document.getElementById('max-drawdown-wrap').innerHTML = '<span id="max-drawdown">--</span>%';
    document.getElementById('total-trades').textContent = '--';
    document.getElementById('total-return-wrap').innerHTML = '<span id="total-return">--</span>%';
    document.getElementById('total-return-wrap').style.color = '';
    document.getElementById('avg-pos-size-wrap').innerHTML = '<span id="avg-pos-size">--</span>%';

    // Reset line color to accent blue
    if (equitySeries) {
        equitySeries.applyOptions({ color: '#2962FF' });
    }

    // Reset header
    activeTicker.textContent = 'Select a Ticker';
    chartPlaceholder.classList.remove('hidden');
}

// ---- Init ----
// Fetch both datasets in parallel on page load
fetchScreenerData();
fetchReversionData();
