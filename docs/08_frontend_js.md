# Frontend JavaScript Logic
# Save this file as `static/app.js`

// Initialize the TradingView Chart instance
const chartContainer = document.getElementById('tvchart');
const chart = LightweightCharts.createChart(chartContainer, {
    width: chartContainer.clientWidth,
    height: 500,
    layout: {
        background: { type: 'solid', color: '#131722' },
        textColor: '#d1d4dc',
    },
    grid: {
        vertLines: { color: '#2B2B43' },
        horzLines: { color: '#2B2B43' },
    }
});

// Create a line series for the VectorBT Equity Curve
const equitySeries = chart.addLineSeries({
    color: '#2962FF',
    lineWidth: 2,
});

// Fetch today's screened tickers on load
async function fetchScreenerData() {
    try {
        const response = await fetch('/api/screener/today');
        const data = await response.json();
        const tbody = document.getElementById('screener-body');
        
        data.forEach(stock => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td><strong>${stock.ticker}</strong></td>
                <td>${stock.rvol_at_trigger.toFixed(2)}</td>
                <td>${stock.atr_pct_at_trigger.toFixed(1)}%</td>
            `;
            // Add click listener to load the backtest chart
            tr.addEventListener('click', () => loadBacktest(stock.ticker));
            tbody.appendChild(tr);
        });
    } catch (error) {
        console.error("Error fetching screener data:", error);
    }
}

// Fetch and render the backtest data for a specific ticker
async function loadBacktest(ticker) {
    document.getElementById('active-ticker').innerText = `Backtest: ${ticker}`;
    
    try {
        const response = await fetch(`/api/backtest/${ticker}`);
        const data = await response.json();
        
        // Update UI Metrics
        document.getElementById('win-rate').innerText = data.win_rate.toFixed(1);
        document.getElementById('profit-factor').innerText = data.profit_factor.toFixed(2);
        document.getElementById('max-drawdown').innerText = data.max_drawdown_pct.toFixed(1);
        
        // Render the equity curve data array onto the chart
        equitySeries.setData(data.equity_curve);
        chart.timeScale().fitContent();

    } catch (error) {
        console.error(`Error fetching backtest for ${ticker}:`, error);
    }
}

// Initialize
fetchScreenerData();