from backtesting.Backtester import Backtester
from constants import frames
from datetime import timedelta
import numpy as np
import time


# https://forums.babypips.com/t/10-pips-daily-for-eurusd/213756
class PrevDayBreakout(Backtester):
    def __init__(self, asset, year, timeframe='M1',
                 risk=0.0005, entry=0.0001, profit=0.0005,
                 timeframe_low='D1', time_format='%Y-%m-%d',
                 profit1_keep_ratio=0.5, adjusted_take_profit=1,
                 trading_cost=0.0002, pip_value=0.0001, signal_expiry=3):

        start = time.time()

        self._risk = risk
        self._entry = entry
        self._profit = profit
        self._timeframe_low = timeframe_low
        self._time_format = time_format

        super().__init__(asset, year, timeframe, profit1_keep_ratio,
                         adjusted_take_profit, trading_cost, pip_value, signal_expiry)

        self._data_low = self.acquire_data(timeframe_low)

        self._init_execution_time = time.time() - start

    def prepare_data(self):
        df = super().prepare_data()

        df['date_index'] = df['timestamp'].dt.strftime(self._time_format)

        return df

    def _calculate_triggers(self):
        df = self._data
        df_low = self._data_low.copy()

        rows = zip(df_low['timestamp'],
                   df_low['high_price'], df_low['low_price'])

        is_first = True
        for i, (timestamp, high_price, low_price) in enumerate(rows):
            if not is_first:
                formatted_ts = timestamp.strftime(self._time_format)
                mask = df['date_index'].eq(formatted_ts)

                maskBuy = mask & (df['high_price'] >=
                                  prev_high_price + self._entry)
                df.loc[maskBuy & maskBuy.cumsum().eq(1),
                       ['signal', 'entry', 'stop', 'profit1', 'profit2']] = [
                    1,
                    prev_high_price + self._entry,
                    prev_high_price + self._entry - self._risk,
                    prev_high_price + self._entry + self._profit,
                    prev_high_price + self._entry + self._profit]

                maskSell = mask & (df['low_price'] <=
                                   prev_low_price - self._entry)
                df.loc[maskSell & maskSell.cumsum().eq(1),
                       ['signal', 'entry', 'stop', 'profit1', 'profit2']] = [
                    -1,
                    prev_low_price - self._entry,
                    prev_low_price - self._entry + self._risk,
                    prev_low_price - self._entry - self._profit,
                    prev_low_price - self._entry - self._profit]

            is_first = False
            prev_high_price = high_price
            prev_low_price = low_price
