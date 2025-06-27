"""
Backtesting module for evaluating signal generator performance.
Implements historical data processing and performance analysis.
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from signal_generator import SignalGenerator

class BacktestEngine:
    def __init__(self, db_path, initial_capital=100000.0, commission_per_trade=0.0, commission_pct=0.0, slippage_pct=0.0, bid_ask_spread_pct=0.0, min_commission=0.0, max_commission=None, commission_tiers=None, symbol_cost_overrides=None, custom_cost_fn=None, risk_per_trade=0.01, stop_loss_pct=None, take_profit_pct=None, max_exposure=1.0, max_positions=None):
        """
        Initialize the backtesting engine.
        
        Args:
            db_path (str): Path to the SQLite database
            initial_capital (float): Initial capital for backtesting
            commission_per_trade (float): Fixed commission per trade (absolute value)
            commission_pct (float): Commission as a percentage of trade value
            slippage_pct (float): Slippage as a percentage of price
            bid_ask_spread_pct (float): Bid-ask spread as a percentage of price
            min_commission (float): Minimum commission per trade
            max_commission (float or None): Maximum commission per trade
            commission_tiers (list of tuples or None): [(volume_threshold, pct), ...] for tiered commissions
            symbol_cost_overrides (dict or None): Per-symbol overrides for cost params
            custom_cost_fn (callable or None): Custom function for transaction cost
            risk_per_trade (float): Fraction of capital to risk per trade (for position sizing)
            stop_loss_pct (float or None): Stop-loss as % from entry (e.g., 0.02 for 2%)
            take_profit_pct (float or None): Take-profit as % from entry
            max_exposure (float): Max fraction of capital exposed at once
            max_positions (int or None): Max number of open positions
        """
        self.db_path = db_path
        self.initial_capital = initial_capital
        self.current_capital = initial_capital
        self.positions = {}  # symbol -> {entry_price, quantity, entry_time}
        self.commission_per_trade = commission_per_trade
        self.commission_pct = commission_pct
        self.slippage_pct = slippage_pct
        self.bid_ask_spread_pct = bid_ask_spread_pct
        self.min_commission = min_commission
        self.max_commission = max_commission
        self.commission_tiers = commission_tiers or []
        self.symbol_cost_overrides = symbol_cost_overrides or {}
        self.custom_cost_fn = custom_cost_fn
        self.risk_per_trade = risk_per_trade
        self.stop_loss_pct = stop_loss_pct
        self.take_profit_pct = take_profit_pct
        self.max_exposure = max_exposure
        self.max_positions = max_positions
        
        # Set up logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        
        # Initialize signal generator
        self.signal_generator = SignalGenerator(db_path)
        
        # Initialize performance metrics
        self.metrics = {
            'total_trades': 0,
            'winning_trades': 0,
            'losing_trades': 0,
            'total_profit_loss': 0.0,
            'win_rate': 0.0,
            'avg_profit_per_trade': 0.0,
            'max_drawdown': 0.0,
            'sharpe_ratio': 0.0
        }
        
        self._init_database()
    
    def _init_database(self):
        """Initialize database tables for backtesting results."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Create backtesting results table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS backtest_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME NOT NULL,
                    symbol TEXT NOT NULL,
                    signal_type TEXT NOT NULL,
                    entry_price REAL,
                    exit_price REAL,
                    quantity REAL,
                    profit_loss REAL,
                    hold_time_hours REAL,
                    market_condition TEXT,
                    confidence REAL,
                    success BOOLEAN
                )
            """)
            
            # Create performance metrics table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS backtest_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME NOT NULL,
                    total_trades INTEGER,
                    winning_trades INTEGER,
                    losing_trades INTEGER,
                    total_profit_loss REAL,
                    win_rate REAL,
                    avg_profit_per_trade REAL,
                    max_drawdown REAL,
                    sharpe_ratio REAL,
                    initial_capital REAL,
                    final_capital REAL
                )
            """)
            
            conn.commit()
            conn.close()
            self.logger.info("Backtesting tables initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Error initializing database: {e}")
            raise
    
    def run_backtest(self, symbols, start_date, end_date, position_size=0.1):
        """
        Run backtest for given symbols and date range.
        
        Args:
            symbols (list): List of symbols to trade
            start_date (datetime): Start date for backtest
            end_date (datetime): End date for backtest
            position_size (float): Size of each position as fraction of capital
        """
        try:
            self.logger.info(f"Starting backtest from {start_date} to {end_date}")
            current_date = start_date
            
            while current_date <= end_date:
                # Get signals for the current date
                signals = self.signal_generator.generate_signals(symbols)
                
                # Process signals and update positions
                for signal in signals:
                    symbol = signal['symbol']
                    signal_type = signal['signal_type']
                    confidence = signal['confidence']
                    price = signal['close_price']

                    # Enforce max positions and exposure
                    if self.max_positions and len(self.positions) >= self.max_positions:
                        continue
                    total_exposure = sum(p['entry_price'] * p['quantity'] for p in self.positions.values()) / self.current_capital if self.current_capital > 0 else 0
                    if total_exposure >= self.max_exposure:
                        continue

                    # Close existing position if opposite signal
                    if symbol in self.positions:
                        position = self.positions[symbol]
                        if (signal_type == 'SELL' and position['type'] == 'BUY') or \
                           (signal_type == 'BUY' and position['type'] == 'SELL'):
                            self._close_position(symbol, price, current_date)

                    # Open new position if confidence is high enough
                    if signal_type != 'NEUTRAL' and confidence >= self.signal_generator.confidence_threshold:
                        # Risk-based position sizing
                        if self.stop_loss_pct:
                            risk_amount = self.current_capital * self.risk_per_trade
                            stop_loss_price = price * (1 - self.stop_loss_pct) if signal_type == 'BUY' else price * (1 + self.stop_loss_pct)
                            per_unit_risk = abs(price - stop_loss_price)
                            quantity = risk_amount / per_unit_risk if per_unit_risk > 0 else 0
                        else:
                            position_value = self.current_capital * position_size
                            quantity = position_value / price
                        self.positions[symbol] = {
                            'type': signal_type,
                            'entry_price': price,
                            'quantity': quantity,
                            'entry_time': current_date,
                            'market_condition': signal['market_condition'],
                            'confidence': confidence,
                            'stop_loss': price * (1 - self.stop_loss_pct) if self.stop_loss_pct and signal_type == 'BUY' else (price * (1 + self.stop_loss_pct) if self.stop_loss_pct else None),
                            'take_profit': price * (1 + self.take_profit_pct) if self.take_profit_pct and signal_type == 'BUY' else (price * (1 - self.take_profit_pct) if self.take_profit_pct else None)
                        }
                        self.logger.info(
                            f"Opening {signal_type} position for {symbol} at {price:.2f} "
                            f"(Confidence: {confidence:.2%})"
                        )

                # Check stop-loss/take-profit for all open positions
                for symbol, position in list(self.positions.items()):
                    current_price = price  # In real use, fetch current price for symbol
                    if position.get('stop_loss') and ((position['type'] == 'BUY' and current_price <= position['stop_loss']) or (position['type'] == 'SELL' and current_price >= position['stop_loss'])):
                        self.logger.info(f"Stop-loss triggered for {symbol} at {current_price:.2f}")
                        self._close_position(symbol, current_price, current_date)
                    elif position.get('take_profit') and ((position['type'] == 'BUY' and current_price >= position['take_profit']) or (position['type'] == 'SELL' and current_price <= position['take_profit'])):
                        self.logger.info(f"Take-profit triggered for {symbol} at {current_price:.2f}")
                        self._close_position(symbol, current_price, current_date)
                # Move to next day
                current_date += timedelta(days=1)
            
            # Close any remaining positions
            for symbol in list(self.positions.keys()):
                self._close_position(symbol, self._get_last_price(symbol), end_date)
            
            # Calculate final metrics
            self._calculate_metrics()
            self._save_metrics()
            
            return self.metrics
            
        except Exception as e:
            self.logger.error(f"Error during backtest: {e}")
            raise
    
    def run_walk_forward_analysis(self, symbols, start_date, end_date, window_size=30, step_size=7, position_size=0.1):
        """
        Perform walk-forward analysis: run rolling window backtests with periodic retraining.
        
        Args:
            symbols (list): List of symbols to trade
            start_date (datetime): Start date for analysis
            end_date (datetime): End date for analysis
            window_size (int): Number of days in each backtest window
            step_size (int): Number of days to move the window forward each step
            position_size (float): Size of each position as fraction of capital
        
        Returns:
            List of metrics dicts for each window
        """
        results = []
        current_start = start_date
        while current_start + timedelta(days=window_size) <= end_date:
            current_end = current_start + timedelta(days=window_size)
            self.logger.info(f"Walk-forward window: {current_start} to {current_end}")
            # Reset capital and positions for each window
            self.current_capital = self.initial_capital
            self.positions = {}
            metrics = self.run_backtest(symbols, current_start, current_end, position_size)
            results.append(metrics.copy())
            current_start += timedelta(days=step_size)
        return results

    def run_monte_carlo_simulation(self, n_simulations=1000, n_days=252):
        """
        Perform Monte Carlo simulation using historical daily returns.
        
        Args:
            n_simulations (int): Number of simulation paths
            n_days (int): Number of days per simulation
        
        Returns:
            List of final portfolio values for each simulation
        """
        # Gather historical daily returns from backtest_results
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query("""
            SELECT profit_loss FROM backtest_results ORDER BY timestamp
        """, conn)
        conn.close()
        if df.empty:
            raise ValueError("No backtest results available for Monte Carlo simulation.")
        daily_returns = df['profit_loss'] / self.initial_capital
        results = []
        for _ in range(n_simulations):
            simulated_returns = np.random.choice(daily_returns, size=n_days, replace=True)
            portfolio = self.initial_capital * (1 + simulated_returns).cumprod()
            results.append(portfolio[-1])
        return results

    def _get_commission(self, symbol, trade_value):
        # Per-symbol override
        if symbol in self.symbol_cost_overrides:
            params = self.symbol_cost_overrides[symbol]
            commission_per_trade = params.get('commission_per_trade', self.commission_per_trade)
            commission_pct = params.get('commission_pct', self.commission_pct)
            min_commission = params.get('min_commission', self.min_commission)
            max_commission = params.get('max_commission', self.max_commission)
            commission_tiers = params.get('commission_tiers', self.commission_tiers)
        else:
            commission_per_trade = self.commission_per_trade
            commission_pct = self.commission_pct
            min_commission = self.min_commission
            max_commission = self.max_commission
            commission_tiers = self.commission_tiers
        # Tiered commission
        if commission_tiers:
            for threshold, pct in sorted(commission_tiers, reverse=True):
                if trade_value >= threshold:
                    commission_pct = pct
                    break
        # Calculate commission
        commission = commission_per_trade + commission_pct * trade_value
        # Apply min/max
        commission = max(commission, min_commission)
        if max_commission is not None:
            commission = min(commission, max_commission)
        return commission

    def _close_position(self, symbol, price, close_time):
        """Close a position and record the result, applying advanced transaction costs."""
        try:
            position = self.positions[symbol]
            entry_price = position['entry_price']
            quantity = position['quantity']
            trade_value = quantity * price

            # Per-symbol overrides
            slippage_pct = self.symbol_cost_overrides.get(symbol, {}).get('slippage_pct', self.slippage_pct)
            bid_ask_spread_pct = self.symbol_cost_overrides.get(symbol, {}).get('bid_ask_spread_pct', self.bid_ask_spread_pct)

            # Apply slippage and bid-ask spread
            if position['type'] == 'BUY':
                effective_entry = entry_price * (1 + slippage_pct + bid_ask_spread_pct / 2)
                effective_exit = price * (1 - slippage_pct - bid_ask_spread_pct / 2)
                profit_loss = (effective_exit - effective_entry) * quantity
            else:  # SELL
                effective_entry = entry_price * (1 - slippage_pct - bid_ask_spread_pct / 2)
                effective_exit = price * (1 + slippage_pct + bid_ask_spread_pct / 2)
                profit_loss = (effective_entry - effective_exit) * quantity

            # Commission (entry + exit)
            if self.custom_cost_fn:
                commission = self.custom_cost_fn(symbol, trade_value, position)
            else:
                commission = 2 * self._get_commission(symbol, trade_value)
            profit_loss -= commission

            # Update capital
            self.current_capital += profit_loss

            # Calculate hold time
            hold_time = (close_time - position['entry_time']).total_seconds() / 3600

            # Record trade
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO backtest_results (
                    timestamp, symbol, signal_type, entry_price,
                    exit_price, quantity, profit_loss, hold_time_hours,
                    market_condition, confidence, success
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                close_time,
                symbol,
                position['type'],
                entry_price,
                price,
                quantity,
                profit_loss,
                hold_time,
                position['market_condition'],
                position['confidence'],
                profit_loss > 0
            ))
            conn.commit()
            conn.close()

            # Update metrics
            self.metrics['total_trades'] += 1
            if profit_loss > 0:
                self.metrics['winning_trades'] += 1
            else:
                self.metrics['losing_trades'] += 1
            self.metrics['total_profit_loss'] += profit_loss

            self.logger.info(
                f"Closed {position['type']} position for {symbol} at {price:.2f} "
                f"(P/L: {profit_loss:.2f}, Commission: {commission:.2f})"
            )

            # Remove position
            del self.positions[symbol]

        except Exception as e:
            self.logger.error(f"Error closing position for {symbol}: {e}")
            raise
    
    def _get_last_price(self, symbol):
        """Get the last available price for a symbol."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT close_price 
                FROM technical_analysis 
                WHERE symbol = ? 
                ORDER BY timestamp DESC 
                LIMIT 1
            """, (symbol,))
            
            row = cursor.fetchone()
            conn.close()
            
            if row:
                return row[0]
            else:
                raise ValueError(f"No price data found for {symbol}")
                
        except Exception as e:
            self.logger.error(f"Error getting last price for {symbol}: {e}")
            raise
    
    def _calculate_metrics(self):
        """Calculate performance metrics from backtest results."""
        try:
            if self.metrics['total_trades'] > 0:
                # Calculate win rate
                self.metrics['win_rate'] = (
                    self.metrics['winning_trades'] / self.metrics['total_trades']
                )
                
                # Calculate average profit per trade
                self.metrics['avg_profit_per_trade'] = (
                    self.metrics['total_profit_loss'] / self.metrics['total_trades']
                )
                
                # Calculate max drawdown and Sharpe ratio
                conn = sqlite3.connect(self.db_path)
                df = pd.read_sql_query("""
                    SELECT timestamp, profit_loss 
                    FROM backtest_results 
                    ORDER BY timestamp
                """, conn)
                conn.close()
                
                if not df.empty:
                    # Calculate cumulative returns
                    df['cumulative_returns'] = (
                        df['profit_loss'].cumsum() / self.initial_capital
                    )
                    
                    # Calculate max drawdown
                    df['peak'] = df['cumulative_returns'].cummax()
                    df['drawdown'] = df['peak'] - df['cumulative_returns']
                    self.metrics['max_drawdown'] = df['drawdown'].max()
                    
                    # Calculate Sharpe ratio (assuming risk-free rate of 0.02)
                    returns = df['profit_loss'] / self.initial_capital
                    excess_returns = returns - 0.02 / 252  # Daily risk-free rate
                    self.metrics['sharpe_ratio'] = (
                        np.sqrt(252) * excess_returns.mean() / excess_returns.std()
                    )
            
        except Exception as e:
            self.logger.error(f"Error calculating metrics: {e}")
            raise
    
    def _save_metrics(self):
        """Save backtest metrics to database."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO backtest_metrics (
                    timestamp, total_trades, winning_trades,
                    losing_trades, total_profit_loss, win_rate,
                    avg_profit_per_trade, max_drawdown, sharpe_ratio,
                    initial_capital, final_capital
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                datetime.now(),
                self.metrics['total_trades'],
                self.metrics['winning_trades'],
                self.metrics['losing_trades'],
                self.metrics['total_profit_loss'],
                self.metrics['win_rate'],
                self.metrics['avg_profit_per_trade'],
                self.metrics['max_drawdown'],
                self.metrics['sharpe_ratio'],
                self.initial_capital,
                self.current_capital
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Error saving metrics: {e}")
            raise
