from backtesting.Backtester import Backtester
from constants import frames
from datetime import timedelta
import numpy as np
import time


class TestStrategy(Backtester):
    def __init__(self, asset, year, timeframe='M5', timeframe_low='H1',
                 lowres_ema_1=8, lowres_ema_2=21,
                 hires_ema_1=8, hires_ema_2=13, hires_ema_3=21,
                 start_offset=0.0003, stop_offset=0.0003, trading_cost=0):

        start = time.time()

        self._timeframe_low = timeframe_low
        self._lowres_ema_1 = lowres_ema_1
        self._lowres_ema_2 = lowres_ema_2
        self._hires_ema_1 = hires_ema_1
        self._hires_ema_2 = hires_ema_2
        self._hires_ema_3 = hires_ema_3
        self._start_offset = start_offset
        self._stop_offset = stop_offset

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
        df['risk'] = 0.0
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

        df2.loc[(df2.index.isin(i)) &
                (df2.ema_1 < df2.ema_2) &
                (df2.ema_2 < df2.ema_3) &
                (df2.ema_1 < df2.high_price) &
                (df2.close_price < df2.ema_3) &
                (df2.ema_2 - df2.ema_1 > 0.0001) &
                (df2.ema_2 - df2.ema_1 < 0.0004) &
                (df2.ema_3 - df2.ema_2 > 0.0001) &
                (df2.ema_3 - df2.ema_2 < 0.0004) &
                (df2.shift(1).ema_1 < df2.shift(1).ema_2) &
                (df2.shift(1).ema_2 < df2.shift(1).ema_3) &
                (df2.shift(1).high_price < df2.shift(1).ema_1) &
                (df2.start < df2.low_price),
                'signal'
                ] = -1

        df2.loc[df2.signal == -1, 'start'] = df2.start - self._start_offset
        df2.loc[df2.signal == -1, 'stop'] = df2.high_price + self._stop_offset
        df2.loc[df2.signal == -1, 'risk'] = df2.stop - df2.start
        df2.loc[df2.signal == -1, 'profit1'] = df2.start - df2.risk
        df2.loc[df2.signal == -1, 'profit2'] = df2.profit1 - df2.risk

        for i in range(-1, -200, -1):
            df2.loc[(df2.signal == -1) &
                    (df2.status == '') &
                    (df2.shift(i).high_price >= df2.stop),
                    ['status', 'stop_offset']] = ['cancel', -i]
            df2.loc[(df2.signal == -1) &
                    (df2.status == '') &
                    (df2.shift(i).low_price < df2.start),
                    ['status', 'start_offset']] = ['trading', -i]
            df2.loc[(df2.signal == -1) &
                    (df2.status == 'trading') &
                    (df2.shift(i).high_price >= df2.stop),
                    ['status', 'stop_offset']] = ['stop', -i]
            df2.loc[(df2.signal == -1) &
                    (df2.status == 'profit1') &
                    (df2.shift(i).high_price >= df2.stop),
                    ['status', 'stop_offset']] = ['even', -i]
            df2.loc[(df2.signal == -1) &
                    (df2.status == 'trading') &
                    (df2.shift(i).low_price <= df2.profit1),
                    'status'] = 'profit1'
            df2.loc[(df2.signal == -1) &
                    (df2.status == 'profit1') &
                    (df2.shift(i).low_price <= df2.profit2),
                    ['status', 'stop_offset']] = ['profit2', -i]

        print(df2.loc[df2.status !=
                      ''].loc[df2.shift(1).start_offset + df2.shift(1).index + 10000 > df2.index])

        print(df2.groupby('status').size())

        print(df2.loc[(df2.status == 'stop') & (df2.timestamp > '2021-02-01'), [
              'timestamp', 'start_offset', 'start', 'stop_offset', 'stop']])

        self._test_execution_time = time.time() - start
