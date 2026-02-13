import duckdb
import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Dict, Optional

class RightSideVBacktester:
    def __init__(self, db_path: str = 'trading_data.duckdb'):
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
    
    def get_symbol_data(self, symbol: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """Get historical data for a symbol"""
        query = f"""
        SELECT * FROM trading_data_2m 
        WHERE symbol = '{symbol}'
        """
        
        if start_date:
            query += f" AND timestamp >= '{start_date}'"
        if end_date:
            query += f" AND timestamp <= '{end_date}'"
        
        query += " ORDER BY timestamp"
        
        df = self.conn.execute(query).fetchdf()
        return df.sort_values('timestamp').reset_index(drop=True)
    
    def filter_trading_window(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter data to only include trading hours (8:30-15:00 CST)"""
        # Convert timestamp to datetime if it's not already
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Extract hour and minute
        df['hour'] = df['timestamp'].dt.hour
        df['minute'] = df['timestamp'].dt.minute
        
        # Filter for trading window (8:30 AM to 3:00 PM CST)
        trading_hours = (
            ((df['hour'] == 8) & (df['minute'] >= 30)) |  # 8:30 AM onwards
            (df['hour'] > 8) |  # 9 AM onwards
            (df['hour'] < 15) |  # Before 3 PM
            ((df['hour'] == 15) & (df['minute'] == 0))  # Exactly 3:00 PM
        )
        
        filtered_df = df[trading_hours].copy()
        
        print(f"Original data points: {len(df)}")
        print(f"After trading window filter: {len(filtered_df)}")
        
        return filtered_df
    
    def calculate_atr(self, df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """Calculate Average True Range (ATR)"""
        high = df['high_price']
        low = df['low_price']
        close = df['close_price'].shift(1)
        
        tr1 = high - low
        tr2 = abs(high - close)
        tr3 = abs(low - close)
        
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        df['atr'] = tr.rolling(window=period).mean()
        return df

    def calculate_vwap_distance(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate price distance from VWAP in standard deviations"""
        # Calculate rolling standard deviation of price from VWAP
        df['vwap_distance_pct'] = ((df['close_price'] - df['vwap']) / df['vwap']) * 100
        df['vwap_distance_std'] = (df['close_price'] - df['vwap']) / df['vwap'].rolling(20).std()
        
        # Calculate EMA 8/9 for trend (using 9 as standard for day trading)
        df['ema_9'] = df['close_price'].ewm(span=9, adjust=False).mean()
        
        return self.calculate_atr(df)
    
    def identify_capitulation(self, df: pd.DataFrame, rvol_threshold: float = 1.5, 
                            vwap_distance_threshold: float = 0.5, atr_drop_mult: float = 3.0) -> pd.DataFrame:
        """Identify capitulation points using ATR for dynamic drop thresholds"""
        df = self.calculate_vwap_distance(df)
        
        # Calculate drop from recent high in terms of ATR
        df['rolling_max_20'] = df['close_price'].rolling(20).max()
        # Avoid division by zero if ATR is 0 (unlikely but safe)
        df['drop_atr'] = np.where(df['atr'] > 0, (df['rolling_max_20'] - df['close_price']) / df['atr'], 0)
        
        # Capitulation conditions:
        # 1. Elevated relative volume (> 1.5x average)
        # 2. Price below VWAP (> 0.5% for our data)
        # 3. Price has dropped significantly (> 3x ATR from recent high)
        # 4. RSI oversold (< 35) or Panic Selling (Hammer/Wik) - stick to simple oversold + drop
        
        df['is_capitulation'] = (
            (df['relative_volume'] > rvol_threshold) &
            (df['vwap_distance_pct'] < -vwap_distance_threshold) &
            (df['drop_atr'] > atr_drop_mult) &  # Dynamic Drop Threshold
            (df['rsi'] < 35)  # Stricter oversold
        )
        
        return df
    
    def identify_pivot_points(self, df: pd.DataFrame) -> pd.DataFrame:
        """Identify structural pivot lows"""
        df['is_pivot_low'] = False
        
        # Find capitulation points
        capitulation_points = df[df['is_capitulation']].copy()
        
        # Use enumerate to get correct iteration indices
        for i, (idx, row) in enumerate(capitulation_points.iterrows()):
            if i > 0:  # Need at least one previous capitulation point
                # Look for lowest price in next 10 bars using iteration index
                window_start = max(0, i - 10)
                window_end = min(len(capitulation_points), i + 10)
                
                window = capitulation_points.iloc[window_start:window_end]
                if not window.empty:
                    lowest_price = window['low_price'].min()
                    lowest_idx = window['low_price'].idxmin()
                    
                    # Mark as pivot low if it's the lowest in the window
                    if row['low_price'] == lowest_price:
                        df.loc[idx, 'is_pivot_low'] = True
                        break
        
        return df
    
    def identify_entry_triggers(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Identify Two Types of Entry Triggers:
        1. Aggressive V-Turn: Immediate reclaim of EMA9 with Volume.
        2. Conservative Higher Low: Structural W-pattern.
        """
        df['entry_trigger'] = False
        df['entry_type'] = None
        df['stop_loss'] = None
        df['stop_loss_price'] = None
        
        capitulation_points = df[df['is_capitulation']].copy()
        processed_indices = set()
        
        for idx in capitulation_points.index:
            if idx in processed_indices:
                continue
            
            # Found Capitulation
            cap_row = df.loc[idx]
            cap_time = pd.to_datetime(cap_row['timestamp'])
            if cap_time.hour >= 15: continue
            
            pivot_confirmed = False
            pivot_idx = -1
            pivot_price = -1
            
            # --- PHASE 1: FIND PIVOT LOW (V-Bottom) ---
            # Search next 20 bars for the absolute low
            search_window = 20
            if idx + search_window >= len(df): continue
            
            window_df = df.iloc[idx : idx + search_window]
            pivot_idx = window_df['low_price'].idxmin()
            pivot_price = window_df.loc[pivot_idx, 'low_price']
            
            # --- PHASE 2: LOOK FOR ENTRIES (Aggressive or Conservative) ---
            
            # We start looking immediately after the Pivot Low
            
            trade_found = False
            
            for i in range(pivot_idx + 1, min(len(df), pivot_idx + 40)):
                if trade_found: break
                
                curr_row = df.loc[i]
                prev_row = df.loc[i-1]
                
                # Check for Pivot Violation (New Low) - Pattern Failed
                if curr_row['low_price'] < pivot_price:
                    break # Restart search from this new low? Simplified: just abort this cap instance.
                
                # ENTRY TYPE 1: AGGRESSIVE V-TURN
                # Conditions:
                # - Close > EMA 9
                # - Volume > 1.5x (Strong buying)
                # - Close > Open (Green Candle)
                # - Valid risk: Stop at Pivot Low is < 2% away (don't buy extended)
                
                dist_to_stop = (curr_row['close_price'] - pivot_price) / curr_row['close_price']
                
                if (curr_row['close_price'] > curr_row['ema_9'] and 
                    curr_row['relative_volume'] > 1.5 and
                    curr_row['close_price'] > curr_row['open_price'] and
                    dist_to_stop < 0.025): # Max 2.5% risk
                    
                    df.loc[i, 'entry_trigger'] = True
                    df.loc[i, 'entry_type'] = 'v_turn'
                    df.loc[i, 'stop_loss_price'] = pivot_price
                    trade_found = True
                    processed_indices.update(range(idx, i + 1))
                    break

                # ENTRY TYPE 2: CONSERVATIVE HIGHER LOW
                # Conditions:
                # - Price made a bounce (measured by ATR or %)
                # - Price pulled back (HL)
                # - Price breaks bounce high
                
                # This logic is complex to iterate. Let's simplify:
                # If we are at `i` and `close > highest in last 5 bars` 
                # AND `min low in last 5 bars > pivot_price` (Higher Low exists)
                
                recent_window = df.loc[max(pivot_idx, i-5):i-1]
                if not recent_window.empty:
                    recent_low = recent_window['low_price'].min()
                    recent_high = recent_window['high_price'].max()
                    
                    # Ensure we had a pullback (Higher Low)
                    # HL > Pivot
                    if recent_low > pivot_price * 1.0005: # Slight buffer
                        # Breakout of structure
                        if curr_row['close_price'] > recent_high:
                            # Valid Structure Breakout
                            df.loc[i, 'entry_trigger'] = True
                            df.loc[i, 'entry_type'] = 'higher_low'
                            df.loc[i, 'stop_loss_price'] = recent_low
                            trade_found = True
                            processed_indices.update(range(idx, i + 1))
                            break
        
        return df
    
    def simulate_trades(self, df: pd.DataFrame, target_vwap: bool = True, 
                       trail_stops: bool = True) -> List[Dict]:
        """Simulate trades with dynamic risk management (Breakeven, Trailing)"""
        trades = []
        
        # First, collect all entry triggers
        entry_triggers = df[df['entry_trigger']].copy()
        
        for i_trade, (_, trigger_row) in enumerate(entry_triggers.iterrows()):
                
            entry_idx = df[df['timestamp'] == trigger_row['timestamp']].index[0]
            entry_time = pd.to_datetime(trigger_row['timestamp'])
            
            # Optimization: Define EOD index for THIS day
            # Since df is filtered for trading hours, we just look ahead until day changes
            # But simpler: Just simple-slice next 200 bars (approx 1 day) and check date
            # 6.5 hours * 30 bars = 195 bars.
            
            # Safe slice:
            max_idx = min(len(df), entry_idx + 250)
            trade_data_slice = df.iloc[entry_idx:max_idx] 
            # Note: No .copy() to save memory/time, treat as read-only or precise
            
            # Initial Trade Setup
            entry_price = trigger_row['close_price']
            initial_stop_price = trigger_row.get('stop_loss_price') 
            if pd.isna(initial_stop_price):
                 initial_stop_price = entry_price * 0.98

            stop_price = initial_stop_price
            
            # Risk Management Flags
            moved_to_breakeven = False
            
            trade = {
                'entry_time': trigger_row['timestamp'],
                'entry_price': entry_price,
                'initial_stop': initial_stop_price,
                'type': trigger_row.get('entry_type', 'unknown'),
                'exit_time': None,
                'exit_price': None,
                'status': 'open',
                'pnl_pct': 0,
                'max_unrealized_pnl': 0,
                'max_profit': 0,
                'bars_held': 0,
                'exit_reason': None
            }
            
            entry_risk = entry_price - initial_stop_price
            if entry_risk <= 0: entry_risk = entry_price * 0.01
            
            # Process trade bars
            for i, (idx, row) in enumerate(trade_data_slice.iterrows()):
                if i == 0: continue # Skip entry bar
                
                # Check if we moved to a new day (shouldn't happen with EOD exit, but safety)
                curr_time = pd.to_datetime(row['timestamp'])
                if curr_time.date() != entry_time.date():
                    trade['status'] = 'closed'
                    trade['exit_time'] = row['timestamp']
                    trade['exit_price'] = row['open_price'] # Exit at open of next day if held
                    trade['exit_reason'] = 'next_day_force_close'
                    break
                
                trade['bars_held'] += 1
                current_price = row['close_price']
                current_low = row['low_price']
                current_high = row['high_price']
                

                # 1. Start with Previous Candle Low Stop
                # For the first bar after entry, the "previous" bar is the Entry Bar.
                # So we set stop_loss to the low of the PREVIOUS bar (i-1 in the slice).
                # trade_data_slice includes entry bar at index 0.
                # We are iterating `row` which is at index `i` (where i starts at 1).
                # So `i-1` is the previous bar.
                
                prev_bar = trade_data_slice.iloc[i-1]
                stop_price = prev_bar['low_price']
                
                # Check Stop Loss (if current low went below previous low)
                if current_low < stop_price:
                    trade['status'] = 'closed'
                    trade['exit_time'] = row['timestamp']
                    # We exit at the stop price (slippage aside) or Open if it gaped down?
                    # "Exit when price goes lower". Usually a stop limit. 
                    # If Open < Stop, we exit at Open. Else at Stop.
                    trade['exit_price'] = min(row['open_price'], stop_price)
                    trade['exit_reason'] = 'prev_candle_low'
                    break
                
                # EOD Close (15:00 CST)
                # Market closes at 15:00 CST. We close at 15:00 or first bar after.
                if curr_time.hour >= 15:
                    trade['status'] = 'closed'
                    trade['exit_time'] = row['timestamp']
                    trade['exit_price'] = current_price
                    trade['exit_reason'] = 'eod_1500'
                    break
            
            # End of Data Close (if slice ended and still open)
            if trade['status'] == 'open':
                last_row = trade_data_slice.iloc[-1]
                trade['status'] = 'closed'
                trade['exit_time'] = last_row['timestamp']
                trade['exit_price'] = last_row['close_price']
                trade['exit_reason'] = 'end_of_slice'
            
            # Calculate Final PnL (ensure exit_price is set)
            if trade['exit_price'] is None: 
                trade['exit_price'] = trade_data_slice.iloc[-1]['close_price']
                
            trade['pnl_pct'] = ((trade['exit_price'] - trade['entry_price']) / trade['entry_price']) * 100
            trades.append(trade)
        
        return trades
    
    def calculate_performance_metrics(self, trades: List[Dict]) -> Dict:
        """Calculate comprehensive performance metrics"""
        if not trades:
            return {}
        
        closed_trades = [t for t in trades if t['status'] == 'closed']
        if not closed_trades:
            return {}
        
        pnl_list = [t['pnl_pct'] for t in closed_trades]
        wins = [p for p in pnl_list if p > 0]
        losses = [p for p in pnl_list if p < 0]
        
        total_trades = len(closed_trades)
        win_rate = len(wins) / total_trades * 100 if total_trades > 0 else 0
        avg_win = np.mean(wins) if wins else 0
        avg_loss = np.mean(losses) if losses else 0
        
        total_profit = sum(wins) if wins else 0
        total_loss = abs(sum(losses)) if losses else 0
        profit_factor = total_profit / total_loss if total_loss > 0 else float('inf')
        
        total_pnl = sum(pnl_list)
        max_drawdown = min(pnl_list) if pnl_list else 0
        recovery_factor = total_profit / abs(max_drawdown) if max_drawdown < 0 else float('inf')
        
        avg_bars_held = np.mean([t['bars_held'] for t in closed_trades])
        largest_win = max(pnl_list) if pnl_list else 0
        largest_loss = min(pnl_list) if pnl_list else 0
        
        metrics = {
            'total_trades': total_trades,
            'win_rate': win_rate,
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'profit_factor': profit_factor,
            'total_pnl': total_pnl,
            'max_drawdown': max_drawdown,
            'recovery_factor': recovery_factor,
            'avg_bars_held': avg_bars_held,
            'largest_win': largest_win,
            'largest_loss': largest_loss,
            'trades_by_type': {}
        }
        
        # Breakdown by type
        if closed_trades:
            trade_df = pd.DataFrame(closed_trades)
            if 'type' in trade_df.columns:
                 metrics['trades_by_type'] = trade_df['type'].value_counts().to_dict()
        
        return metrics
    
    def backtest_symbol(self, symbol: str, start_date: str = None, end_date: str = None,
                       rvol_threshold: float = 1.5, vwap_distance_threshold: float = 0.5) -> Dict:
        """Backtest a single symbol with trading window filter"""
        # Get data for symbol
        df = self.get_symbol_data(symbol, start_date, end_date)
        
        if df.empty:
            return {'symbol': symbol, 'trades': [], 'data_points': 0, 'capitulation_points': 0, 'pivot_lows': 0, 'entry_triggers': 0, 'data': df}
        
        # Filter for trading window (9:30-16:00 EST)
        df = self.filter_trading_window(df)
        
        # Identify capitulation points
        df = self.identify_capitulation(df, rvol_threshold, vwap_distance_threshold)
        
        # Identify pivot points
        df = self.identify_pivot_points(df)
        
        # Identify entry triggers
        df = self.identify_entry_triggers(df)
        
        # Simulate trades
        trades = self.simulate_trades(df, target_vwap=True, trail_stops=False)
        
        # Calculate performance metrics
        metrics = self.calculate_performance_metrics(trades)
        
        return {
            'symbol': symbol,
            'trades': trades,
            'data_points': len(df),
            'capitulation_points': df['is_capitulation'].sum(),
            'pivot_lows': df['is_pivot_low'].sum(),
            'entry_triggers': df['entry_trigger'].sum(),
            'metrics': metrics,
            'data': df
        }
    
    def run_backtest_suite(self, symbols: List[str] = None) -> Dict:
        """Run backtest for all symbols or specific list"""
        if symbols is None:
            symbols_query = "SELECT DISTINCT symbol FROM trading_data_2m"
            symbols_df = self.conn.execute(symbols_query).fetchdf()
            symbols = symbols_df['symbol'].tolist()
        
        all_results = {}
        combined_trades = []
        
        for symbol in symbols:
            result = self.backtest_symbol(symbol)
            all_results[symbol] = result
            
            if 'trades' in result:
                combined_trades.extend(result['trades'])
        
        # Calculate combined metrics
        combined_metrics = self.calculate_performance_metrics(combined_trades)
        
        return {
            'combined_results': all_results,
            'combined_metrics': combined_metrics,
            'total_trades': len(combined_trades)
        }
    
    def print_results(self, results: Dict):
        """Print backtest results"""
        print(f"{'='*60}")
        print(f"{'='*60}")
        
        # Print combined metrics
        if 'combined_metrics' in results:
            metrics = results['combined_metrics']
            if metrics:  # Check if metrics dict is not empty
                print(f"\n📊 COMBINED PERFORMANCE ({results['total_trades']} total trades)")
                print(f"{'='*60}")
                print(f"Win Rate: {metrics['win_rate']:.1f}%")
                print(f"Profit Factor: {metrics['profit_factor']:.2f}")
                print(f"Total P&L: {metrics['total_pnl']:.2f}%")
                print(f"Max Drawdown: {metrics['max_drawdown']:.2f}%")
                print(f"Recovery Factor: {metrics['recovery_factor']:.2f}")
                print(f"Avg Win: {metrics['avg_win']:.2f}%")
                print(f"Avg Loss: {metrics['avg_loss']:.2f}%")
                print(f"Avg Bars Held: {metrics['avg_bars_held']:.1f}")
                print(f"Largest Win: {metrics['largest_win']:.2f}%")
                print(f"Largest Loss: {metrics['largest_loss']:.2f}%")
                if 'trades_by_type' in metrics:
                     print(f"Trade Types: {metrics['trades_by_type']}")
                print(f"{'='*60}")
            else:
                print(f"\n📊 COMBINED PERFORMANCE ({results['total_trades']} total trades)")
                print(f"No trades found - no metrics to display")
        
        # Print individual symbol results
        if 'combined_results' in results:
            for symbol, result in results['combined_results'].items():
                if 'metrics' in result:
                    metrics = result['metrics']
                    print(f"\n📈 {symbol} ({len(result.get('trades', []))} trades) | Win Rate: {metrics.get('win_rate', 0):.1f}% | P&L: {metrics.get('total_pnl', 0):.2f}% | PF: {metrics.get('profit_factor', 0):.2f}")
        
        print(f"{'='*60}")
    
    def close(self):
        """Close database connection"""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()

if __name__ == "__main__":
    backtester = RightSideVBacktester()
    
    # Run backtest on all available symbols
    results = backtester.run_backtest_suite()
    
    # Print results
    backtester.print_results(results)
    
    backtester.close()
