"""
Test suite for the backtesting module.
"""

import unittest
import logging
from datetime import datetime, timedelta
from unittest.mock import patch
from backtesting import BacktestEngine

def dummy_signals(_self, symbols):
    # Return a list of dummy signals for testing
    return [{
        'symbol': symbols[0] if symbols else '^GDAXI',
        'signal_type': 'BUY',
        'confidence': 1.0,
        'close_price': 10000.0,
        'market_condition': 'bullish'
    }]

def dummy_get_last_price(self, symbol):
    return 10000.0

class TestBacktestEngine(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up test environment before all tests."""
        cls.db_path = "market_data.db"
        cls.test_symbol = "^GDAXI"
        
        # Configure logging for tests
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] %(message)s'
        )
        cls.logger = logging.getLogger(__name__)

    @patch('backtesting.SignalGenerator.generate_signals', new=dummy_signals)
    @patch('backtesting.BacktestEngine._get_last_price', new=dummy_get_last_price)
    def test_basic_backtest(self):
        """Test basic backtesting functionality."""
        # Set up test parameters
        start_date = datetime.now() - timedelta(days=5)
        end_date = datetime.now()
        symbols = [self.test_symbol]
        
        engine = BacktestEngine(self.db_path)
        metrics = engine.run_backtest(symbols, start_date, end_date)
        
        # Verify metrics structure
        self.assertIsNotNone(metrics)
        required_metrics = [
            'total_trades',
            'winning_trades',
            'losing_trades',
            'total_profit_loss',
            'win_rate',
            'avg_profit_per_trade',
            'max_drawdown',
            'sharpe_ratio'
        ]
        for metric in required_metrics:
            self.assertIn(metric, metrics)
            
        # Verify metric values
        self.assertGreaterEqual(metrics['total_trades'], 0)
        self.assertGreaterEqual(metrics['winning_trades'], 0)
        self.assertGreaterEqual(metrics['losing_trades'], 0)
        self.assertEqual(
            metrics['total_trades'],
            metrics['winning_trades'] + metrics['losing_trades']
        )
        
        if metrics['total_trades'] > 0:
            self.assertGreaterEqual(metrics['win_rate'], 0.0)
            self.assertLessEqual(metrics['win_rate'], 1.0)
        
        self.logger.info(f"Backtest completed successfully. Metrics: {metrics}")

    @patch('backtesting.SignalGenerator.generate_signals', new=dummy_signals)
    @patch('backtesting.BacktestEngine._get_last_price', new=dummy_get_last_price)
    def test_fixed_and_percentage_commission(self):
        """Test fixed and percentage commission application."""
        engine = BacktestEngine(self.db_path, commission_per_trade=5.0, commission_pct=0.001)
        start_date = datetime.now() - timedelta(days=5)
        end_date = datetime.now()
        symbols = [self.test_symbol]
        metrics = engine.run_backtest(symbols, start_date, end_date)
        self.assertIn('total_profit_loss', metrics)
        self.logger.info(f"Fixed/percentage commission test metrics: {metrics}")

    @patch('backtesting.SignalGenerator.generate_signals', new=dummy_signals)
    @patch('backtesting.BacktestEngine._get_last_price', new=dummy_get_last_price)
    def test_tiered_commission(self):
        """Test tiered commission application."""
        tiers = [(10000, 0.0005), (0, 0.001)]  # 0.05% for >=10k, else 0.1%
        engine = BacktestEngine(self.db_path, commission_tiers=tiers)
        start_date = datetime.now() - timedelta(days=5)
        end_date = datetime.now()
        symbols = [self.test_symbol]
        metrics = engine.run_backtest(symbols, start_date, end_date)
        self.assertIn('total_profit_loss', metrics)
        self.logger.info(f"Tiered commission test metrics: {metrics}")

    @patch('backtesting.SignalGenerator.generate_signals', new=dummy_signals)
    @patch('backtesting.BacktestEngine._get_last_price', new=dummy_get_last_price)
    def test_per_symbol_override(self):
        """Test per-symbol commission/slippage override."""
        overrides = {
            self.test_symbol: {
                'commission_per_trade': 2.0,
                'commission_pct': 0.002,
                'slippage_pct': 0.01
            }
        }
        engine = BacktestEngine(self.db_path, symbol_cost_overrides=overrides)
        start_date = datetime.now() - timedelta(days=5)
        end_date = datetime.now()
        symbols = [self.test_symbol]
        metrics = engine.run_backtest(symbols, start_date, end_date)
        self.assertIn('total_profit_loss', metrics)
        self.logger.info(f"Per-symbol override test metrics: {metrics}")

    @patch('backtesting.SignalGenerator.generate_signals', new=dummy_signals)
    @patch('backtesting.BacktestEngine._get_last_price', new=dummy_get_last_price)
    def test_min_max_commission(self):
        """Test min and max commission enforcement."""
        engine = BacktestEngine(self.db_path, commission_pct=0.01, min_commission=10.0, max_commission=20.0)
        start_date = datetime.now() - timedelta(days=5)
        end_date = datetime.now()
        symbols = [self.test_symbol]
        metrics = engine.run_backtest(symbols, start_date, end_date)
        self.assertIn('total_profit_loss', metrics)
        self.logger.info(f"Min/max commission test metrics: {metrics}")

    @patch('backtesting.SignalGenerator.generate_signals', new=dummy_signals)
    @patch('backtesting.BacktestEngine._get_last_price', new=dummy_get_last_price)
    def test_custom_cost_function(self):
        """Test custom transaction cost function."""
        def custom_cost(symbol, trade_value, position):
            # Flat $3 per trade plus 0.2% of trade value
            return 3.0 + 0.002 * trade_value
        engine = BacktestEngine(self.db_path, custom_cost_fn=custom_cost)
        start_date = datetime.now() - timedelta(days=5)
        end_date = datetime.now()
        symbols = [self.test_symbol]
        metrics = engine.run_backtest(symbols, start_date, end_date)
        self.assertIn('total_profit_loss', metrics)
        self.logger.info(f"Custom cost function test metrics: {metrics}")

    @patch('backtesting.SignalGenerator.generate_signals', new=dummy_signals)
    @patch('backtesting.BacktestEngine._get_last_price', new=dummy_get_last_price)
    def test_position_management(self):
        """Test position opening and closing logic."""
        # Set up test parameters
        start_date = datetime.now() - timedelta(days=5)
        end_date = datetime.now()
        symbols = [self.test_symbol]
        
        engine = BacktestEngine(self.db_path)
        engine.run_backtest(symbols, start_date, end_date)
        
        # Verify all positions are closed
        self.assertEqual(len(engine.positions), 0)
        
        # Verify capital has been properly tracked
        self.assertNotEqual(engine.current_capital, 0.0)
        self.logger.info(
            f"Final capital: {engine.current_capital:.2f} "
            f"(Initial: {engine.initial_capital:.2f})"
        )

    def test_database_initialization(self):
        """Test database tables are properly initialized."""
        engine = BacktestEngine(self.db_path)
        self.assertIsNotNone(engine)

    def test_metrics_calculation(self):
        """Test performance metrics calculation."""
        engine = BacktestEngine(self.db_path)
        engine._calculate_metrics()
        
        # Verify metric values are valid
        self.assertGreaterEqual(engine.metrics['max_drawdown'], 0.0)
        self.assertLessEqual(engine.metrics['max_drawdown'], 1.0)
        
        if engine.metrics['total_trades'] > 0:
            self.assertNotEqual(engine.metrics['sharpe_ratio'], 0.0)
            
        self.logger.info(f"Metrics calculation successful")

    @patch('backtesting.SignalGenerator.generate_signals', new=dummy_signals)
    @patch('backtesting.BacktestEngine._get_last_price', new=dummy_get_last_price)
    def test_walk_forward_analysis(self):
        """Test walk-forward analysis functionality."""
        start_date = datetime.now() - timedelta(days=60)
        end_date = datetime.now()
        symbols = [self.test_symbol]
        engine = BacktestEngine(self.db_path)
        results = engine.run_walk_forward_analysis(symbols, start_date, end_date, window_size=10, step_size=5)
        
        # Verify results structure
        self.assertIsInstance(results, list)
        self.assertGreater(len(results), 0)
        for metrics in results:
            self.assertIn('total_trades', metrics)
        
        self.logger.info(f"Walk-forward analysis completed. Results: {results}")

    @patch('backtesting.SignalGenerator.generate_signals', new=dummy_signals)
    @patch('backtesting.BacktestEngine._get_last_price', new=dummy_get_last_price)
    def test_monte_carlo_simulation(self):
        """Test Monte Carlo simulation functionality."""
        # First, run a backtest to populate results
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
        symbols = [self.test_symbol]
        engine = BacktestEngine(self.db_path)
        engine.run_backtest(symbols, start_date, end_date)
        
        results = engine.run_monte_carlo_simulation(n_simulations=10, n_days=5)
        
        # Verify results structure
        self.assertIsInstance(results, list)
        self.assertEqual(len(results), 10)
        
        self.logger.info(f"Monte Carlo simulation completed. Results: {results}")

    @patch('backtesting.SignalGenerator.generate_signals', new=dummy_signals)
    @patch('backtesting.BacktestEngine._get_last_price', new=dummy_get_last_price)
    def test_risk_based_position_sizing(self):
        """Test risk-based position sizing functionality."""
        engine = BacktestEngine(self.db_path, risk_per_trade=0.02, stop_loss_pct=0.01)
        start_date = datetime.now() - timedelta(days=5)
        end_date = datetime.now()
        symbols = [self.test_symbol]
        metrics = engine.run_backtest(symbols, start_date, end_date)
        self.assertIn('total_trades', metrics)
        self.logger.info(f"Risk-based position sizing test metrics: {metrics}")

    @patch('backtesting.SignalGenerator.generate_signals', new=dummy_signals)
    @patch('backtesting.BacktestEngine._get_last_price', new=dummy_get_last_price)
    def test_stop_loss_take_profit(self):
        """Test stop-loss and take-profit functionality."""
        engine = BacktestEngine(self.db_path, stop_loss_pct=0.01, take_profit_pct=0.01)
        start_date = datetime.now() - timedelta(days=5)
        end_date = datetime.now()
        symbols = [self.test_symbol]
        metrics = engine.run_backtest(symbols, start_date, end_date)
        self.assertIn('total_trades', metrics)
        self.logger.info(f"Stop-loss/take-profit test metrics: {metrics}")

    @patch('backtesting.SignalGenerator.generate_signals', new=dummy_signals)
    @patch('backtesting.BacktestEngine._get_last_price', new=dummy_get_last_price)
    def test_max_exposure_and_positions(self):
        """Test max exposure and max positions functionality."""
        engine = BacktestEngine(self.db_path, max_exposure=0.1, max_positions=1)
        start_date = datetime.now() - timedelta(days=5)
        end_date = datetime.now()
        symbols = [self.test_symbol, 'TEST2', 'TEST3']
        metrics = engine.run_backtest(symbols, start_date, end_date)
        self.assertIn('total_trades', metrics)
        self.logger.info(f"Max exposure/positions test metrics: {metrics}")

if __name__ == '__main__':
    unittest.main(verbosity=2)
