from backtesting.Backtester import Backtester
from constants import frames
from datetime import timedelta
import pandas as pd
import numpy as np
import time


class EMAPullback(Backtester):
    def __init__(self, asset, year, timeframe='M5', timeframe_low='H1',
                 lowres_ema_1=8, lowres_ema_2=21,
                 hires_ema_1=8, hires_ema_2=13, hires_ema_3=21,
                 entry_offset=3, stop_offset=3,
                 min_diff_emas=1.5, max_ratio_emas=0.2,
                 profit1_keep_ratio=0.5, adjusted_take_profit=1,
                 trading_cost=0.0005, pip_value=0.0001):

        start = time.time()

        self._timeframe_low = timeframe_low
        self._lowres_ema_1 = lowres_ema_1
        self._lowres_ema_2 = lowres_ema_2
        self._hires_ema_1 = hires_ema_1
        self._hires_ema_2 = hires_ema_2
        self._hires_ema_3 = hires_ema_3
        self._entry_offset = entry_offset
        self._stop_offset = stop_offset
        self._min_diff_emas = min_diff_emas
        self._max_ratio_emas = max_ratio_emas

        super().__init__(asset, year, timeframe, profit1_keep_ratio, adjusted_take_profit, trading_cost, pip_value)

        self._data_low = self.acquire_data(timeframe_low)
        self._data_low = self.prepare_data_low()

        self._init_execution_time = time.time() - start

    def prepare_data(self):
        df = self._data.copy()
        df['ema_1'] = df['close_price'].ewm(span=self._hires_ema_1).mean()
        df['ema_2'] = df['close_price'].ewm(span=self._hires_ema_2).mean()
        df['ema_3'] = df['close_price'].ewm(span=self._hires_ema_3).mean()
        df['signal'] = 0
        df['sell_start'] = df.low_price
        df['buy_start'] = df.high_price
        df['stop'] = 0.0
        df['profit1'] = 0.0
        df['profit2'] = 0.0
        df['begin_offset'] = 0
        df['end_offset'] = 0
        df['status'] = ''
        df['pnl'] = 0.0
        df['sig'] = 0

        for i in range(1, 6):
            df.loc[df.shift(i).low_price < df.sell_start,
                   'sell_start'] = df.shift(i).low_price
            df.loc[df.shift(i).high_price > df.buy_start,
                   'buy_start'] = df.shift(i).high_price

        return df

    def prepare_data_low(self):
        df_low = self._data_low.copy()

        df_low['ema_1'] = df_low['close_price'].ewm(
            span=self._lowres_ema_1).mean()
        df_low['ema_2'] = df_low['close_price'].ewm(
            span=self._lowres_ema_2).mean()

        return df_low
    
    def _calculate_triggers(self):
        df_low = self._data_low
        df = self._data

        frame_minutes = frames[self._timeframe_low]
        min_diff_emas = self._min_diff_emas * self._pip_value
        entry_offset = self._entry_offset * self._pip_value
        stop_offset = self._stop_offset * self._pip_value

        sell_timestamps = df_low[(df_low.ema_1 < df_low.ema_2) &
                              (df_low.close_price < df_low.ema_1) &
                              (df_low.shift(1).ema_1 < df_low.shift(1).ema_2) &
                              (df_low.shift(1).close_price < df_low.shift(1).ema_1)
                              ].timestamp + timedelta(minutes=frame_minutes)

        buy_timestamps = df_low[(df_low.ema_1 > df_low.ema_2) &
                             (df_low.open_price > df_low.ema_1) &
                             (df_low.shift(1).ema_1 > df_low.shift(1).ema_2) &
                             (df_low.shift(1).open_price > df_low.shift(1).ema_1)
                             ].timestamp + timedelta(minutes=frame_minutes)

        timestamp_vals = df.timestamp.values

        sell_start_time = sell_timestamps.values
        sell_end_time = (sell_timestamps +
                         timedelta(minutes=frame_minutes)).values

        buy_start_time = buy_timestamps.values
        buy_end_time = (buy_timestamps +
                        timedelta(minutes=frame_minutes)).values

        # https://stackoverflow.com/questions/44367672/best-way-to-join-merge-by-range-in-pandas/44601120#44601120
        sell_index, _ = np.where(
            (timestamp_vals[:, None] >= sell_start_time) &
            (timestamp_vals[:, None] < sell_end_time)
        )

        buy_index, _ = np.where(
            (timestamp_vals[:, None] >= buy_start_time) &
            (timestamp_vals[:, None] < buy_end_time)
        )

        diff_ema_21 = df.ema_2 - df.ema_1
        diff_ema_31 = df.ema_3 - df.ema_1
        diff_ema_32 = df.ema_3 - df.ema_2
        df.loc[(df.index.isin(sell_index)) &
                (df.ema_1 < df.ema_2) &
                (df.ema_2 < df.ema_3) &
                (df.ema_1 < df.high_price) &
                (df.close_price < df.ema_3) &
                (diff_ema_21 > min_diff_emas) &
                (diff_ema_32 > min_diff_emas) &
                (abs((diff_ema_21 / diff_ema_31) - 0.5) < self._max_ratio_emas) &
                (df.shift(1).ema_1 < df.shift(1).ema_2) &
                (df.shift(1).ema_2 < df.shift(1).ema_3) &
                (df.shift(1).high_price < df.shift(1).ema_1) &
                (df.sell_start < df.low_price),
                'signal'
                ] = -1

        diff_ema_12 = df.ema_1 - df.ema_2
        diff_ema_13 = df.ema_1 - df.ema_3
        diff_ema_23 = df.ema_2 - df.ema_3
        df.loc[(df.index.isin(buy_index)) &
                (df.ema_1 > df.ema_2) &
                (df.ema_2 > df.ema_3) &
                (df.ema_1 > df.low_price) &
                (df.close_price > df.ema_3) &
                (diff_ema_12 > min_diff_emas) &
                (diff_ema_23 > min_diff_emas) &
                (abs((diff_ema_12 / diff_ema_13) - 0.5) < self._max_ratio_emas) &
                (df.shift(1).ema_1 > df.shift(1).ema_2) &
                (df.shift(1).ema_2 > df.shift(1).ema_3) &
                (df.shift(1).low_price > df.shift(1).ema_1) &
                (df.buy_start > df.high_price),
                'signal'
                ] = 1

        df.loc[df.signal == -1, 'sell_start'] = df.sell_start - entry_offset
        df.loc[df.signal == -1, 'stop'] = df.high_price + stop_offset

        sell_risk = self._adjusted_take_profit * (df.stop - df.sell_start)
        df.loc[df.signal == -1, 'profit1'] = df.sell_start - sell_risk
        df.loc[df.signal == -1, 'profit2'] = df.profit1 - sell_risk

        df.loc[df.signal == 1, 'buy_start'] = df.buy_start + entry_offset
        df.loc[df.signal == 1, 'stop'] = df.low_price - stop_offset

        buy_risk = self._adjusted_take_profit * (df.buy_start - df.stop)
        df.loc[df.signal == 1, 'profit1'] = df.buy_start + buy_risk
        df.loc[df.signal == 1, 'profit2'] = df.profit1 + buy_risk

        df.loc[df.signal != 0, 'sig'] = 1

        sell_sig = df.loc[df.signal == -1]
        sell_pos = pd.concat(
            [sell_sig.index.to_series(), sell_sig.sell_start, sell_sig.stop, sell_sig.stop, sell_sig.profit1, sell_sig.profit2],
            keys=['signal_offset', 'entry', 'stop', 'cancel', 'profit1', 'profit2'],
            axis=1
        )
        sell_pos['type'] = 'sell'

        buy_sig = df.loc[df.signal == 1]
        buy_pos = pd.concat(
            [buy_sig.index.to_series(), buy_sig.sell_start, buy_sig.stop, buy_sig.stop, buy_sig.profit1, buy_sig.profit2],
            keys=['signal_offset', 'entry', 'stop', 'cancel', 'profit1', 'profit2'],
            axis=1
        )
        buy_pos['type'] = 'buy'

        self._positions = pd.concat([sell_pos, buy_pos]).sort_values('signal_offset', ignore_index=True).reset_index()
        self._positions['status'] = ''
