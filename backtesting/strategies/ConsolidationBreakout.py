from backtesting.Backtester import Backtester
from constants import frames
from datetime import timedelta
import numpy as np
import time


class ConsolidationBreakout(Backtester):
    def __init__(self, asset, year, timeframe='M5',
                 entry_offset=3, stop_offset=3,
                 profit1_keep_ratio=0.5, adjusted_take_profit=1, move_stop_to_breakeven=False,
                 trading_cost=0.0005, pip_value=0.0001):

        start = time.time()

        self._entry_offset = entry_offset
        self._stop_offset = stop_offset

        super().__init__(asset, year, timeframe, profit1_keep_ratio, adjusted_take_profit, move_stop_to_breakeven, trading_cost, pip_value)

        self._init_execution_time = time.time() - start

    def prepare_data(self):
        df = self._data.copy()
        df['signal'] = 0
        df['sell_start'] = df.low_price
        df['buy_start'] = df.high_price
        df['stop'] = 0.0
        df['profit1'] = 0.0
        df['profit2'] = 0.0
        df['start_offset'] = 0
        df['stop_offset'] = 0
        df['status'] = ''
        df['pnl'] = 0.0

        for i in range(1, 6):
            df.loc[df.shift(i).low_price < df.sell_start,
                   'sell_start'] = df.shift(i).low_price
            df.loc[df.shift(i).high_price > df.buy_start,
                   'buy_start'] = df.shift(i).high_price

        return df

    def _calculate_triggers(self):
        df_low = self._data_low
        df = self._data

        frame_minutes = frames[self._timeframe_low]
        min_diff_emas = self._min_diff_emas * self._pip_value
        start_offset = self._start_offset * self._pip_value
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

        df.loc[df.signal == -1, 'sell_start'] = df.sell_start - start_offset
        df.loc[df.signal == -1, 'stop'] = df.high_price + stop_offset

        sell_risk = self._adjusted_take_profit * (df.stop - df.sell_start)
        df.loc[df.signal == -1, 'profit1'] = df.sell_start - sell_risk
        df.loc[df.signal == -1, 'profit2'] = df.profit1 - sell_risk

        df.loc[df.signal == 1, 'buy_start'] = df.buy_start + start_offset
        df.loc[df.signal == 1, 'stop'] = df.low_price - stop_offset

        buy_risk = self._adjusted_take_profit * (df.buy_start - df.stop)
        df.loc[df.signal == 1, 'profit1'] = df.buy_start + buy_risk
        df.loc[df.signal == 1, 'profit2'] = df.profit1 + buy_risk
