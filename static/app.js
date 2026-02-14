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
let screenerData = null; // Cached screener response

// ---- DOM references ----
const screenerBody   = document.getElementById('screener-body');
const screenerTable  = document.getElementById('screener-table');
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

// ---- Screener ----

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

        // Signals
        if (data.signals.length === 0) {
            screenerTable.classList.add('hidden');
            screenerEmpty.classList.remove('hidden');
            return;
        }

        screenerEmpty.classList.add('hidden');
        screenerTable.classList.remove('hidden');
        screenerBody.innerHTML = '';

        data.signals.forEach((stock, idx) => {
            const tr = document.createElement('tr');
            tr.dataset.idx = idx;
            tr.innerHTML = `
                <td>${stock.ticker}</td>
                <td>${stock.rvol_at_trigger.toFixed(2)}</td>
                <td>${stock.atr_pct_at_trigger.toFixed(1)}%</td>
                <td>$${stock.trigger_price.toFixed(2)}</td>
            `;
            tr.addEventListener('click', () => onTickerClick(idx));
            screenerBody.appendChild(tr);
        });

    } catch (err) {
        console.error('Error fetching screener data:', err);
    }
}

function onTickerClick(idx) {
    const stock = screenerData.signals[idx];

    // Highlight active row
    document.querySelectorAll('#screener-body tr').forEach(r => r.classList.remove('active'));
    document.querySelector(`#screener-body tr[data-idx="${idx}"]`).classList.add('active');

    // Show news
    showNews(stock);

    // Load backtest
    loadBacktest(stock.ticker);
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

async function loadBacktest(ticker) {
    activeTicker.textContent = `Backtest: ${ticker}`;
    chartPlaceholder.classList.add('hidden');
    loadingOverlay.classList.remove('hidden');

    // Lazy-init the chart on first use
    if (!chart) {
        initChart();
    }

    try {
        const resp = await fetch(`/api/backtest/${ticker}`);
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

        // Color-code return
        const retEl = document.getElementById('total-return').parentElement;
        retEl.style.color = data.total_return_pct >= 0 ? '#3fb950' : '#f85149';

        // Render equity curve
        equitySeries.setData(data.equity_curve);
        chart.timeScale().fitContent();

    } catch (err) {
        console.error(`Error fetching backtest for ${ticker}:`, err);
    } finally {
        loadingOverlay.classList.add('hidden');
    }
}

// ---- Init ----
fetchScreenerData();
