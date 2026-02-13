from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
import pandas as pd
import duckdb
import os
import json
from typing import List, Dict, Any
import numpy as np

# Import the backtester logic
# Note: Ensure backtest_right_side_v.py is in the same directory
try:
    from backtest_right_side_v import RightSideVBacktester
except ImportError:
    raise ImportError("Could not import backtest_right_side_v.py. Ensure it is in the same directory.")

app = FastAPI(title="Right Side V Strategy visualizer")

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Initialize Backtester (reuses connection logic)
# We might need to handle the DB connection carefully if it's locked.
# The backtester creates a new connection in __init__.
# We should probably instantiate it once or per request?
# DuckDB supports concurrent reads but only one writer if avoiding WAL issues.
# Read-only mode might be better if we are just reading.
# But the backtester might write temp tables? It creates filtered views/tables.
# Let's use a single instance for now.

DB_PATH = 'trading_data.duckdb'

@app.get("/")
async def read_index():
    return FileResponse('templates/index.html')

@app.get("/symbols")
async def get_symbols():
    """Get list of available symbols from the database"""
    try:
        # Use a temporary read-only connection to get symbols
        conn = duckdb.connect(DB_PATH, read_only=True)
        # Assuming table name is 'trading_data_2m' or similar as per backtester
        # Let's check backtester code... it uses 'price_data'
        symbols = conn.execute("SELECT DISTINCT symbol FROM trading_data_2m ORDER BY symbol").fetchall()
        conn.close()
        return {"symbols": [s[0] for s in symbols]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/data/{symbol}")
async def get_symbol_data(symbol: str, start_date: str = None, end_date: str = None):
    """
    Get OHLCV data, Indicators, and Trade Markers for a symbol.
    Runs the strategy on-the-fly to get the latest trade signals.
    """
    try:
        # 1. Initialize Backtester
        # We instantiate a new one to avoid state issues, but might be slow if it reconnects every time.
        # Ideally, we should reuse connection/backtester if safe.
        backtester = RightSideVBacktester(db_path=DB_PATH)
        
        # 2. Get Data & Calculate Indicators (using internal methods)
        # The backtester has methods to get data and process metrics.
        # We need to expose slightly deeper logic or use public methods.
        # `get_symbol_data` gets raw data.
        # `calculate_atr` adds ATR.
        
        df = backtester.get_symbol_data(symbol, start_date=start_date, end_date=end_date)
        if df.empty:
            raise HTTPException(status_code=404, detail=f"No data found for {symbol}")
            
        # 3. Run Strategy Logic to get Indicators & Trades/Markers
        # We can re-use the `calculate_indicators` logic from backtester if it exists,
        # or just run `identify_capitulation` and `identify_entry_triggers`.
        
        # NOTE: backtester.identify_capitulation calls calculate_vwap_distance internally
        df = backtester.identify_capitulation(df)
        df = backtester.calculate_atr(df)
        df = backtester.identify_pivot_points(df)
        df = backtester.identify_entry_triggers(df)
        
        # 4. Simulate Trades to get Exit points
        trades = backtester.simulate_trades(df)
        
        # 5. Format for Frontend
        # Convert timestamp to epoch seconds for Lightweight Charts
        df['time'] = df['timestamp'].apply(lambda x: int(x.timestamp()))
        
        # OHLCV
        ohlcv = df[['time', 'open_price', 'high_price', 'low_price', 'close_price', 'volume']].to_dict(orient='records')
        
        # Renaming keys to match Lightweight Charts expectation (open, high, low, close, value instead of volume sometimes)
        # LW Charts expects: { time: ..., open: ..., high: ..., low: ..., close: ... }
        # Volume: { time: ..., value: ..., color: ... }
        
        formatted_ohlc = []
        formatted_volume = []
        formatted_vwap = []
        formatted_ema = [] # EMA 9
        
        for _, row in df.iterrows():
            t = int(row['time'])
            # Candle
            formatted_ohlc.append({
                'time': t,
                'open': float(row['open_price']) if pd.notnull(row['open_price']) else None,
                'high': float(row['high_price']) if pd.notnull(row['high_price']) else None,
                'low': float(row['low_price']) if pd.notnull(row['low_price']) else None,
                'close': float(row['close_price']) if pd.notnull(row['close_price']) else None
            })
            
            # Volume (Color based on up/down)
            color = 'rgba(0, 150, 136, 0.5)' if row['close_price'] >= row['open_price'] else 'rgba(255, 82, 82, 0.5)'
            formatted_volume.append({
                'time': t,
                'value': row['volume'],
                'color': color
            })
            
            # Indicators
            if not pd.isna(row.get('vwap')):
                formatted_vwap.append({'time': t, 'value': float(row['vwap'])})
            if not pd.isna(row.get('ema_9')):
                formatted_ema.append({'time': t, 'value': float(row['ema_9'])})

        # Markers
        markers = []
        
        # Add Entry Markers
        # Filter where entry_trigger is True
        # entries = df[df['entry_trigger'] == True] # This might miss exits if we purely use df
        # Better to use the `trades` list returned by simulate_trades for confirmed trades
        
        for trade in trades:
            # Entry Marker
            entry_time = int(pd.to_datetime(trade['entry_time']).timestamp())
            markers.append({
                'time': entry_time,
                'position': 'belowBar',
                'color': '#2196F3', # Blue
                'shape': 'arrowUp',
                'text': f"Buy {trade['type']} @ {trade['entry_price']:.2f}"
            })
            
            # Exit Marker
            if trade['exit_time']:
                exit_time = int(pd.to_datetime(trade['exit_time']).timestamp())
                pnl = trade['pnl_pct']
                color = '#4CAF50' if pnl > 0 else '#F44336' # Green or Red
                shape = 'arrowDown'
                markers.append({
                    'time': exit_time,
                    'position': 'aboveBar',
                    'color': color,
                    'shape': shape,
                    'text': f"Sell ({trade['exit_reason']}) {pnl:.2f}%"
                })

        return {
            "ohlc": formatted_ohlc,
            "volume": formatted_volume,
            "vwap": formatted_vwap,
            "ema": formatted_ema,
            "markers": markers,
            "trades": trades  # Raw trade data for table if needed
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
