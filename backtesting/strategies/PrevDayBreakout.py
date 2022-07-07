from backtesting.Backtester import Backtester
from constants import frames
from datetime import timedelta
import numpy as np
import time


# https://forums.babypips.com/t/10-pips-daily-for-eurusd/213756
class PrevDayBreakout(Backtester):
    def __init__(self, asset, year, timeframe='M5',
                 risk=0.0010, entry=0.0003, profit=0.0005,
                 profit1_keep_ratio=0.5, adjusted_take_profit=1,
                 trading_cost=0.0005, pip_value=0.0001, signal_expiry=3):

        start = time.time()

        self._risk = risk
        self._entry = entry
        self._profit = profit

        super().__init__(asset, year, timeframe, profit1_keep_ratio,
                         adjusted_take_profit, trading_cost, pip_value, signal_expiry)

        self._init_execution_time = time.time() - start

    def prepare_data(self):
        df = super().prepare_data()

        return df

    def _calculate_triggers(self):
        df = self._data
