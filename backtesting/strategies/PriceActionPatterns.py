from backtesting.Backtester import Backtester
from constants import frames
from utils.indicators import add_support, add_resistance
import time


# https://forums.babypips.com/t/my-price-action-trading-strategy/582002
class PriceActionPatterns(Backtester):
    def __init__(self, asset, year, timeframe='D1',
                 profit1_keep_ratio=0.5, adjusted_take_profit=1,
                 trading_cost=0.0002, pip_value=0.0001, signal_expiry=3):

        start = time.time()

        super().__init__(asset, year, timeframe, profit1_keep_ratio,
                         adjusted_take_profit, trading_cost, pip_value, signal_expiry)

        self._init_execution_time = time.time() - start

    def prepare_data(self):
        df = super().prepare_data()

        return df

    def _calculate_triggers(self):
        df = self._data
