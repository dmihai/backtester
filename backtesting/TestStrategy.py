from backtesting.Backtester import Backtester
from constants import frames
from datetime import timedelta
import numpy as np
import time


class TestStrategy(Backtester):
    def __init__(self, asset, year, timeframe='M5', timeframe_low='H1',
                 lowres_ema_1=8, lowres_ema_2=21,
                 hires_ema_1=8, hires_ema_2=13, hires_ema_3=21,
                 start_offset=3, stop_offset=3,
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
        self._start_offset = start_offset
        self._stop_offset = stop_offset
        self._profit1_keep_ratio = profit1_keep_ratio
        self._min_diff_emas = min_diff_emas
        self._max_ratio_emas = max_ratio_emas
        self._adjusted_take_profit = adjusted_take_profit

        super().__init__(asset, year, timeframe, trading_cost, pip_value)

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

    def prepare_data_low(self):
        df_low = self._data_low.copy()

        df_low['ema_1'] = df_low['close_price'].ewm(
            span=self._lowres_ema_1).mean()
        df_low['ema_2'] = df_low['close_price'].ewm(
            span=self._lowres_ema_2).mean()

        return df_low

    def test(self):
        start = time.time()

        df1 = self._data_low
        df2 = self._data

        frame_minutes = frames[self._timeframe_low]
        min_diff_emas = self._min_diff_emas * self._pip_value
        start_offset = self._start_offset * self._pip_value
        stop_offset = self._stop_offset * self._pip_value

        sell_timestamps = df1[(df1.ema_1 < df1.ema_2) &
                              (df1.close_price < df1.ema_1) &
                              (df1.shift(1).ema_1 < df1.shift(1).ema_2) &
                              (df1.shift(1).close_price < df1.shift(1).ema_1)
                              ].timestamp + timedelta(minutes=frame_minutes)

        buy_timestamps = df1[(df1.ema_1 > df1.ema_2) &
                             (df1.open_price > df1.ema_1) &
                             (df1.shift(1).ema_1 > df1.shift(1).ema_2) &
                             (df1.shift(1).open_price > df1.shift(1).ema_1)
                             ].timestamp + timedelta(minutes=frame_minutes)

        timestamp_vals = df2.timestamp.values

        sell_start_time = sell_timestamps.values
        sell_end_time = (sell_timestamps +
                         timedelta(minutes=frame_minutes)).values

        buy_start_time = buy_timestamps.values
        buy_end_time = (buy_timestamps +
                        timedelta(minutes=frame_minutes)).values

        # https://stackoverflow.com/questions/44367672/best-way-to-join-merge-by-range-in-pandas/44601120#44601120
        sell_index, j = np.where(
            (timestamp_vals[:, None] >= sell_start_time) &
            (timestamp_vals[:, None] < sell_end_time)
        )

        buy_index, j = np.where(
            (timestamp_vals[:, None] >= buy_start_time) &
            (timestamp_vals[:, None] < buy_end_time)
        )

        # df2.loc[i, :]

        diff_ema_21 = df2.ema_2 - df2.ema_1
        diff_ema_31 = df2.ema_3 - df2.ema_1
        diff_ema_32 = df2.ema_3 - df2.ema_2
        df2.loc[(df2.index.isin(sell_index)) &
                (df2.ema_1 < df2.ema_2) &
                (df2.ema_2 < df2.ema_3) &
                (df2.ema_1 < df2.high_price) &
                (df2.close_price < df2.ema_3) &
                (diff_ema_21 > min_diff_emas) &
                (diff_ema_32 > min_diff_emas) &
                (abs((diff_ema_21 / diff_ema_31) - 0.5) < self._max_ratio_emas) &
                (df2.shift(1).ema_1 < df2.shift(1).ema_2) &
                (df2.shift(1).ema_2 < df2.shift(1).ema_3) &
                (df2.shift(1).high_price < df2.shift(1).ema_1) &
                (df2.sell_start < df2.low_price),
                'signal'
                ] = -1

        diff_ema_12 = df2.ema_1 - df2.ema_2
        diff_ema_13 = df2.ema_1 - df2.ema_3
        diff_ema_23 = df2.ema_2 - df2.ema_3
        df2.loc[(df2.index.isin(buy_index)) &
                (df2.ema_1 > df2.ema_2) &
                (df2.ema_2 > df2.ema_3) &
                (df2.ema_1 > df2.low_price) &
                (df2.close_price > df2.ema_3) &
                (diff_ema_12 > min_diff_emas) &
                (diff_ema_23 > min_diff_emas) &
                (abs((diff_ema_12 / diff_ema_13) - 0.5) < self._max_ratio_emas) &
                (df2.shift(1).ema_1 > df2.shift(1).ema_2) &
                (df2.shift(1).ema_2 > df2.shift(1).ema_3) &
                (df2.shift(1).low_price > df2.shift(1).ema_1) &
                (df2.buy_start > df2.high_price),
                'signal'
                ] = 1

        df2.loc[df2.signal == -1, 'sell_start'] = df2.sell_start - start_offset
        df2.loc[df2.signal == -1, 'stop'] = df2.high_price + stop_offset

        sell_risk = self._adjusted_take_profit * (df2.stop - df2.sell_start)
        df2.loc[df2.signal == -1, 'profit1'] = df2.sell_start - sell_risk
        df2.loc[df2.signal == -1, 'profit2'] = df2.profit1 - sell_risk

        df2.loc[df2.signal == 1, 'buy_start'] = df2.buy_start + start_offset
        df2.loc[df2.signal == 1, 'stop'] = df2.low_price - stop_offset

        buy_risk = self._adjusted_take_profit * (df2.buy_start - df2.stop)
        df2.loc[df2.signal == 1, 'profit1'] = df2.buy_start + buy_risk
        df2.loc[df2.signal == 1, 'profit2'] = df2.profit1 + buy_risk

        mode = 0
        status = ''
        trading = None
        trading_index = 0
        start_index = 0
        stop_index = 0

        for i, row in df2.iterrows():
            if mode == 0:  # looking for a signal
                if row['signal'] != 0:
                    mode = row['signal']
                    trading = row
                    trading_index = i
            elif mode == -1:  # looking for an entry after a sell signal
                if row['high_price'] >= trading['stop']:
                    mode = 0
                    status = 'cancel'
                    stop_index = i
                elif row['low_price'] <= trading['sell_start']:
                    mode = -2
                    status = 'trading'
                    start_index = i
            elif mode == 1:  # looking for an entry after a buy signal
                if row['low_price'] <= trading['stop']:
                    mode = 0
                    status = 'cancel'
                    stop_index = i
                elif row['high_price'] >= trading['buy_start']:
                    mode = 2
                    status = 'trading'
                    start_index = i
            elif mode == -2:  # in a sell trading
                if status == 'trading':
                    if row['high_price'] >= trading['stop']:
                        mode = 0
                        status = 'stop'
                        stop_index = i
                    elif row['low_price'] <= trading['profit1']:
                        status = 'profit1'
                        trading['stop'] = trading['sell_start']
                if status == 'profit1':
                    if row['high_price'] >= trading['stop']:
                        mode = 0
                        status = 'even'
                        stop_index = i
                    elif row['low_price'] <= trading['profit2']:
                        mode = 0
                        status = 'profit2'
                        stop_index = i
            elif mode == 2:  # in a buy trading
                if status == 'trading':
                    if row['low_price'] <= trading['stop']:
                        mode = 0
                        status = 'stop'
                        stop_index = i
                    elif row['high_price'] >= trading['profit1']:
                        status = 'profit1'
                        trading['stop'] = trading['buy_start']
                if status == 'profit1':
                    if row['low_price'] <= trading['stop']:
                        mode = 0
                        status = 'even'
                        stop_index = i
                    elif row['high_price'] >= trading['profit2']:
                        mode = 0
                        status = 'profit2'
                        stop_index = i

            if mode == 0 and status != '':
                df2.loc[trading_index, ['status', 'start_offset', 'stop_offset']] = [
                    status, start_index - trading_index, stop_index - trading_index
                ]
                status = ''

        df2.loc[(df2.signal == -1) & (df2.status == 'stop'),
                'pnl'] = -abs(df2.sell_start - df2.stop)
        profit1 = self._profit1_keep_ratio * abs(df2.profit1 - df2.sell_start)
        profit2 = (1 - self._profit1_keep_ratio) * \
            abs(df2.profit2 - df2.sell_start)
        df2.loc[(df2.signal == -1) & (df2.status == 'even'), 'pnl'] = profit1
        df2.loc[(df2.signal == -1) & (df2.status == 'profit2'),
                'pnl'] = profit1 + profit2

        df2.loc[(df2.signal == 1) & (df2.status == 'stop'),
                'pnl'] = -abs(df2.stop - df2.buy_start)
        profit1 = self._profit1_keep_ratio * abs(df2.profit1 - df2.buy_start)
        profit2 = (1 - self._profit1_keep_ratio) * \
            abs(df2.profit2 - df2.buy_start)
        df2.loc[(df2.signal == 1) & (df2.status == 'even'), 'pnl'] = profit1
        df2.loc[(df2.signal == 1) & (df2.status == 'profit2'),
                'pnl'] = profit1 + profit2

        df2.loc[df2.status.isin(
            ['stop', 'even', 'profit2']), 'pnl'] = df2.pnl - self._trading_cost
        self._results = df2.loc[df2.pnl != 0, [
            'timestamp', 'pnl']].reset_index(drop=True)

        self._test_execution_time = time.time() - start
