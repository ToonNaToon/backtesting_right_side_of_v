// Main Initialization
document.addEventListener('DOMContentLoaded', () => {
    console.log("Chart.js loaded");

    const symbolSelect = document.getElementById('symbol-select');
    const startDateInput = document.getElementById('start-date');
    const endDateInput = document.getElementById('end-date');
    const loadBtn = document.getElementById('load-btn');
    const loadingDiv = document.getElementById('loading');
    const chartContainer = document.getElementById('chart-container');

    // Check if library loaded
    if (!window.LightweightCharts) {
        console.error("LightweightCharts library not found!");
        alert("Error: Chart library not loaded. Check connection or file path.");
        return;
    }

    // Initialize Chart
    let chart;
    let candleSeries, volumeSeries, vwapSeries, emaSeries;

    try {
        chart = LightweightCharts.createChart(chartContainer, {
            layout: {
                background: { color: '#1a1a1a' },
                textColor: '#d1d4dc',
            },
            grid: {
                vertLines: { color: '#2d2d2d' },
                horzLines: { color: '#2d2d2d' },
            },
            timeScale: {
                timeVisible: true,
                secondsVisible: false,
            },
            crosshair: {
                mode: LightweightCharts.CrosshairMode.Normal,
            },
        });

        // Create Floating Tooltip
        const container = document.getElementById('chart-container');
        const legend = document.createElement('div');
        legend.className = 'chart-legend';
        legend.style = `position: absolute; left: 12px; top: 12px; z-index: 1000; font-size: 14px; font-family: sans-serif; line-height: 18px; font-weight: 300; pointer-events: none; color: white;`;
        container.appendChild(legend);

        // Create Series
        candleSeries = chart.addCandlestickSeries({
            upColor: '#26a69a',
            downColor: '#ef5350',
            borderVisible: false,
            wickUpColor: '#26a69a',
            wickDownColor: '#ef5350',
        });

        volumeSeries = chart.addHistogramSeries({
            color: '#26a69a',
            priceFormat: {
                type: 'volume',
            },
            priceScaleId: '', // Set as an overlay
            scaleMargins: {
                top: 0.8, // Place volume at the bottom
                bottom: 0,
            },
        });

        // Update Legend on Crosshair Move
        chart.subscribeCrosshairMove(param => {
            if (
                param.point === undefined ||
                !param.time ||
                param.point.x < 0 ||
                param.point.x > container.clientWidth ||
                param.point.y < 0 ||
                param.point.y > container.clientHeight
            ) {
                // Hide legend when crosshair is outside chart
                legend.style.display = 'none';
                return;
            }

            // Get Data
            const candleData = param.seriesData.get(candleSeries);
            const volumeData = param.seriesData.get(volumeSeries);

            if (!candleData) {
                // Hide legend if no candle data (chart not loaded yet)
                legend.style.display = 'none';
                return;
            }

            // Show legend when we have data
            legend.style.display = 'block';

            const open = candleData.open !== undefined ? candleData.open.toFixed(2) : '-';
            const high = candleData.high !== undefined ? candleData.high.toFixed(2) : '-';
            const low = candleData.low !== undefined ? candleData.low.toFixed(2) : '-';
            const close = candleData.close !== undefined ? candleData.close.toFixed(2) : '-';
            const vol = volumeData && volumeData.value !== undefined ? volumeData.value.toLocaleString() : '-';

            // Format Date (Simple approximation or from param.time if strictly needed)
            // param.time is epoch seconds.
            const dateStr = new Date(param.time * 1000).toLocaleString();

            // Get current symbol from dropdown
            const currentSymbol = symbolSelect.value || 'N/A';

            legend.innerHTML = `
                <div style="font-size: 18px; margin-bottom: 6px; font-weight: 600;">${currentSymbol}</div>
                <div style="font-size: 14px; margin-bottom: 4px; color: #999;">${dateStr}</div>
                <div>O: <span style="color: #26a69a">${open}</span> H: <span style="color: #26a69a">${high}</span> L: <span style="color: #ef5350">${low}</span> C: <span style="color: #d1d4dc">${close}</span></div>
                <div>Vol: <span style="color: #d1d4dc">${vol}</span></div>
            `;
        });

        vwapSeries = chart.addLineSeries({
            color: '#ff9800', // Orange
            lineWidth: 1,
            title: 'VWAP',
            priceLineVisible: false,
            crosshairMarkerVisible: false,
        });

        emaSeries = chart.addLineSeries({
            color: '#2962ff', // Blue
            lineWidth: 1,
            title: 'EMA 9',
            priceLineVisible: false,
            crosshairMarkerVisible: false,
        });

        // Resize Observer
        const resizeObserver = new ResizeObserver(entries => {
            if (entries.length === 0 || entries[0].target !== chartContainer) { return; }
            const newRect = entries[0].contentRect;
            chart.applyOptions({ height: newRect.height, width: newRect.width });
        });
        resizeObserver.observe(chartContainer);

    } catch (e) {
        console.error("Error initializing chart:", e);
        alert("Error initializing chart: " + e.message);
    }

    // Fetch Symbols
    fetchSymbols();

    // Fetch Symbols on Load
    async function fetchSymbols() {
        console.log("Fetching symbols...");

        // Show loading state in dropdown
        symbolSelect.innerHTML = '<option value="" disabled selected>Loading symbols...</option>';
        symbolSelect.disabled = true;

        try {
            const response = await fetch('/symbols');
            if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
            const data = await response.json();
            console.log("Symbols received:", data);

            // Clear loading state
            symbolSelect.innerHTML = '<option value="" disabled selected>Select Symbol</option>';

            data.symbols.forEach(symbol => {
                const option = document.createElement('option');
                option.value = symbol;
                option.textContent = symbol;
                symbolSelect.appendChild(option);
            });

            // Re-enable dropdown
            symbolSelect.disabled = false;

            // Auto-select first symbol but DON'T auto-load
            // This allows users to see the UI before data loads
            if (data.symbols.length > 0) {
                symbolSelect.value = data.symbols[0];
                // Removed auto-load to show loading state properly
            }
        } catch (error) {
            console.error('Error fetching symbols:', error);
            symbolSelect.innerHTML = '<option value="" disabled selected>Error loading symbols</option>';
            alert("Failed to load symbols: " + error.message);
        }
    }

    // Fetch Data for Symbol
    async function loadData(symbol) {
        if (!symbol) return;
        console.log("Loading data for", symbol);

        // Show loading indicator with CSS class
        loadingDiv.classList.add('active');

        const startDate = startDateInput.value;
        const endDate = endDateInput.value;

        let url = `/data/${symbol}`;
        const params = new URLSearchParams();
        if (startDate) params.append('start_date', startDate);
        if (endDate) params.append('end_date', endDate);

        if (params.toString()) {
            url += `?${params.toString()}`;
        }

        try {
            const response = await fetch(url);
            if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
            const data = await response.json();

            console.log("Data received:", data);

            // Check if data is empty
            if (!data.ohlc || data.ohlc.length === 0) {
                alert("No data found for the selected criteria.");
                return;
            }

            // Sort OHLC by time (ascending)
            const sortedOhlc = data.ohlc.sort((a, b) => a.time - b.time);
            console.log("OHLC Data Sample:", sortedOhlc[0]);

            candleSeries.setData(sortedOhlc);
            volumeSeries.setData(data.volume);
            vwapSeries.setData(data.vwap);
            emaSeries.setData(data.ema);

            // Set Markers
            candleSeries.setMarkers(data.markers.sort((a, b) => a.time - b.time));

            // Fit Content
            chart.timeScale().fitContent();

        } catch (error) {
            console.error('Error loading data:', error);
            alert('Error loading data for ' + symbol);
        } finally {
            // Hide loading indicator
            loadingDiv.classList.remove('active');
        }
    }

    // Event Listeners
    loadBtn.addEventListener('click', () => {
        loadData(symbolSelect.value);
    });

    symbolSelect.addEventListener('change', () => {
        loadData(symbolSelect.value);
    });

    // Auto-reload when date filters change
    startDateInput.addEventListener('change', () => {
        if (symbolSelect.value) {
            loadData(symbolSelect.value);
        }
    });

    endDateInput.addEventListener('change', () => {
        if (symbolSelect.value) {
            loadData(symbolSelect.value);
        }
    });
});
