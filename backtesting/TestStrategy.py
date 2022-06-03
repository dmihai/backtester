from backtesting.Backtester import Backtester
from constants import frames
from datetime import timedelta
import numpy as np
import time


class TestStrategy(Backtester):
    def __init__(self, asset, year, timeframe='M5', timeframe_low='H1',
                 lowres_ema_1=8, lowres_ema_2=21,
                 hires_ema_1=8, hires_ema_2=13, hires_ema_3=21,
                 start_offset=0.0003, stop_offset=0.0003,
                 min_diff_emas=0.00015, max_ratio_emas=0.2,
                 profit1_keep_ratio=0.5, adjusted_take_profit=1, trading_cost=0.0005):

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

        super().__init__(asset, year, timeframe, trading_cost)

        self._data_low = self.acquire_data(timeframe_low)
        self._data_low = self.prepare_data_low()

        self._init_execution_time = time.time() - start

    def prepare_data(self):
        df = self._data.copy()
        df['ema_1'] = df['close_price'].ewm(span=self._hires_ema_1).mean()
        df['ema_2'] = df['close_price'].ewm(span=self._hires_ema_2).mean()
        df['ema_3'] = df['close_price'].ewm(span=self._hires_ema_3).mean()
        df['signal'] = 0
        df['start'] = df.low_price
        df['stop'] = 0.0
        df['profit1'] = 0.0
        df['profit2'] = 0.0
        df['start_offset'] = 0
        df['stop_offset'] = 0
        df['status'] = ''
        df['pnl'] = 0.0

        for i in range(1, 6):
            df.loc[df.shift(i).low_price < df.start,
                   'start'] = df.shift(i).low_price

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
        timestamps1 = df1[(df1.ema_1 < df1.ema_2) &
                          (df1.close_price < df1.ema_2) &
                          (df1.shift(1).ema_1 < df1.shift(1).ema_2) &
                          (df1.shift(1).close_price < df1.shift(1).ema_2)
                          ].timestamp + timedelta(minutes=frame_minutes)

        timestamps2 = df2.timestamp.values
        start_time = timestamps1.values
        end_time = (timestamps1 + timedelta(minutes=frame_minutes)).values

        # https://stackoverflow.com/questions/44367672/best-way-to-join-merge-by-range-in-pandas/44601120#44601120
        i, j = np.where(
            (timestamps2[:, None] >= start_time) &
            (timestamps2[:, None] < end_time)
        )

        # df2.loc[i, :]

        diff_ema_21 = df2.ema_2 - df2.ema_1
        diff_ema_31 = df2.ema_3 - df2.ema_1
        diff_ema_32 = df2.ema_3 - df2.ema_2
        df2.loc[(df2.index.isin(i)) &
                (df2.ema_1 < df2.ema_2) &
                (df2.ema_2 < df2.ema_3) &
                (df2.ema_1 < df2.high_price) &
                (df2.close_price < df2.ema_3) &
                (diff_ema_21 > self._min_diff_emas) &
                (diff_ema_32 > self._min_diff_emas) &
                (abs(((df2.ema_2 - df2.ema_1) / diff_ema_31) - 0.5) < self._max_ratio_emas) &
                (df2.shift(1).ema_1 < df2.shift(1).ema_2) &
                (df2.shift(1).ema_2 < df2.shift(1).ema_3) &
                (df2.shift(1).high_price < df2.shift(1).ema_1) &
                (df2.start < df2.low_price),
                'signal'
                ] = -1

        df2.loc[df2.signal == -1, 'start'] = df2.start - self._start_offset
        df2.loc[df2.signal == -1, 'stop'] = df2.high_price + self._stop_offset

        risk = self._adjusted_take_profit * (df2.stop - df2.start)
        df2.loc[df2.signal == -1, 'profit1'] = df2.start - risk
        df2.loc[df2.signal == -1, 'profit2'] = df2.profit1 - risk

        mode = 0
        status = ''
        trading = None
        trading_index = 0
        start_index = 0
        stop_index = 0

        for i, row in df2.iterrows():
            if mode == 0:  # looking for a signal
                if row['signal'] == -1:
                    mode = -1
                    trading = row
                    trading_index = i
            elif mode == -1:  # looking for an entry after a sell signal
                if row['high_price'] >= trading['stop']:
                    mode = 0
                    status = 'cancel'
                    stop_index = i
                elif row['low_price'] <= trading['start']:
                    mode = -2
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
                        trading['stop'] = trading['start']
                elif status == 'profit1':
                    if row['high_price'] >= trading['stop']:
                        mode = 0
                        status = 'even'
                        stop_index = i
                    elif row['low_price'] <= trading['profit2']:
                        mode = 0
                        status = 'profit2'
                        stop_index = i

            if mode == 0 and status != '':
                df2.loc[trading_index, ['status', 'start_offset', 'stop_offset']] = [
                    status, start_index - trading_index, stop_index - trading_index
                ]
                status = ''

        df2.loc[df2.status == 'stop', 'pnl'] = -abs(df2.start - df2.stop)
        df2.loc[df2.status == 'even', 'pnl'] = self._profit1_keep_ratio * \
            abs(df2.profit1 - df2.start)
        df2.loc[df2.status == 'profit2', 'pnl'] = (1 - self._profit1_keep_ratio) * \
            abs(df2.profit2 - df2.start)

        df2.loc[df2.status.isin(
            ['stop', 'even', 'profit2']), 'pnl'] = df2.pnl - self._trading_cost
        self._results = df2.loc[df2.pnl != 0, [
            'timestamp', 'pnl']].reset_index(drop=True)

        self._test_execution_time = time.time() - start
