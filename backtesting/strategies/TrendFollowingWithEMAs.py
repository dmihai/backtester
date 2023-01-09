import time

from backtesting.Backtester import Backtester


class TrendFollowingWithEMAs(Backtester):
    def __init__(self, asset, year, timeframe='H4',
                 timeframe_low='D1',
                 ema_1=8, ema_2=13, ema_3=21,
                 entry_offset=3, stop_offset=3,
                 profit1_keep_ratio=0.5, adjusted_take_profit=1, move_stop_to_breakeven=False,
                 trading_cost=0.0002, pip_value=0.0001, signal_expiry=100):

        start = time.time()

        self._timeframe_low = timeframe_low
        self._ema_1 = ema_1
        self._ema_2 = ema_2
        self._ema_3 = ema_3
        self._entry_offset = entry_offset
        self._stop_offset = stop_offset

        super().__init__(asset, year, timeframe, profit1_keep_ratio,
                         adjusted_take_profit, move_stop_to_breakeven, trading_cost, pip_value, signal_expiry)
        
        self._data_low = self.acquire_data(timeframe_low)
        self._data_low = self.prepare_data_low()

        self._init_execution_time = time.time() - start


    def prepare_data(self):
        df = super().prepare_data()

        df['ema_1'] = df['close_price'].ewm(span=self._ema_1).mean()
        df['ema_2'] = df['close_price'].ewm(span=self._ema_2).mean()
        df['ema_3'] = df['close_price'].ewm(span=self._ema_3).mean()

        return df
    

    def prepare_data_low(self):
        df_low = self._data_low.copy()

        df_low['ema_1'] = df_low['close_price'].ewm(span=self._ema_1).mean()
        df_low['ema_2'] = df_low['close_price'].ewm(span=self._ema_2).mean()
        df_low['ema_3'] = df_low['close_price'].ewm(span=self._ema_3).mean()

        return df_low
    

    def _calculate_triggers(self):
        pass
