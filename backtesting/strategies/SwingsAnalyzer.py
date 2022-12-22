import time

from backtesting.Backtester import Backtester
from utils.indicators import add_support, add_resistance


class SwingsAnalyzer(Backtester):
    def __init__(self, asset, year, timeframe='H4',
                 profit1_keep_ratio=0.5, adjusted_take_profit=1, move_stop_to_breakeven=False,
                 trading_cost=0.0002, pip_value=0.0001, signal_expiry=100):

        start = time.time()

        super().__init__(asset, year, timeframe, profit1_keep_ratio,
                         adjusted_take_profit, move_stop_to_breakeven, trading_cost, pip_value, signal_expiry)

        self._init_execution_time = time.time() - start

    def prepare_data(self):
        df = super().prepare_data()

        df = add_support(df)
        df = add_resistance(df)

        return df
    
    def _calculate_triggers(self):
        df = self._data
