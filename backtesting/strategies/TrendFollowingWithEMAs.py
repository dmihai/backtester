import time

from backtesting.Backtester import Backtester
from utils.patterns import add_engulfing, add_kangaroo
from utils.functions import df_to_list


# https://www.youtube.com/watch?v=v3w4RprB-8Q
class TrendFollowingWithEMAs(Backtester):
    def __init__(self, asset, year, timeframe='H4',
                 timeframe_low='D1', trend_confirmation_bars=3,
                 engulfing_min_pips=10, engulfing_room_left=4,
                 pinbar_min_pips=10, pinbar_room_left=4,
                 ema_1=8, ema_2=13, ema_3=21,
                 entry_offset=3, stop_offset=3,
                 profit1_keep_ratio=0.5, adjusted_take_profit=1, move_stop_to_breakeven=False,
                 trading_cost=0.0002, pip_value=0.0001, signal_expiry=100):

        start = time.time()

        self._timeframe_low = timeframe_low
        self._trend_confirmation_bars = trend_confirmation_bars
        self._engulfing_min_pips = engulfing_min_pips
        self._engulfing_room_left = engulfing_room_left
        self._pinbar_min_pips = pinbar_min_pips
        self._pinbar_room_left = pinbar_room_left
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

        df['body_low_price'] = df[['close_price', 'open_price']].min(axis=1)
        df['body_high_price'] = df[['close_price', 'open_price']].max(axis=1)

        df['range'] = 0

        df = add_engulfing(df, pip_value=self._pip_value, min_pips=self._engulfing_min_pips, room_left=self._engulfing_room_left)
        df = add_kangaroo(df, pip_value=self._pip_value, min_pips=self._pinbar_min_pips, room_left=self._pinbar_room_left)

        return df
    

    def prepare_data_low(self):
        df_low = self._data_low.copy()

        df_low['ema_1'] = df_low['close_price'].ewm(span=self._ema_1).mean()
        df_low['ema_2'] = df_low['close_price'].ewm(span=self._ema_2).mean()
        df_low['ema_3'] = df_low['close_price'].ewm(span=self._ema_3).mean()

        return df_low
    

    def _calculate_triggers(self):
        df = self._data
        prices = df_to_list(self._data_low, ['timestamp', 'ema_1', 'ema_2', 'ema_3'])

        def is_buying(i):
            current_ema_cond = prices['ema_1'][i] > prices['ema_2'][i] and prices['ema_2'][i] > prices['ema_3'][i]
            prev_ema_cond = True
            if i > 0:
                prev_ema_cond = prices['ema_1'][i-1] < prices['ema_1'][i] and prices['ema_2'][i-1] < prices['ema_2'][i] and prices['ema_3'][i-1] < prices['ema_3'][i]
            return current_ema_cond and prev_ema_cond
        def is_selling(i):
            current_ema_cond = prices['ema_1'][i] < prices['ema_2'][i] and prices['ema_2'][i] < prices['ema_3'][i]
            prev_ema_cond = True
            if i > 0:
                prev_ema_cond = prices['ema_1'][i-1] > prices['ema_1'][i] and prices['ema_2'][i-1] > prices['ema_2'][i] and prices['ema_3'][i-1] > prices['ema_3'][i]
            return current_ema_cond and prev_ema_cond
        
        ranges = []
        range_start = None
        range_type = 0
        range_offset = 0
        for i in range(len(prices['timestamp'])):
            if range_start is None and range_offset > self._trend_confirmation_bars:
                range_start = i
            if is_buying(i):
                range_offset += 1
                range_type = 1
            elif is_selling(i):
                range_offset += 1
                range_type = -1
            else:
                if range_start is not None:
                    ranges.append({
                        'start': range_start,
                        'end': (i + 1) if i < (len(prices['timestamp']) - 1) else i,
                        'type': range_type
                    })
                range_offset = 0
                range_start = None
                range_type = 0
        
        for r in ranges:
            start = prices['timestamp'][r['start']]
            end = prices['timestamp'][r['end']]
            df.loc[(start <= df.timestamp) & (df.timestamp < end), 'range'] = r['type']
        
        stop_offset = self._stop_offset * self._pip_value
        entry_offset = self._entry_offset * self._pip_value

        df.loc[(df.range == 1) & (df.engulfing == 1), 'signal'] = 1
        df.loc[(df.range == 1) & (df.engulfing == 1), 'stop'] = df.low_price - stop_offset
        df.loc[(df.range == 1) & (df.engulfing == 1), 'entry'] = df.high_price + entry_offset
        df.loc[(df.range == 1) & (df.engulfing == 1) & (df.low_price > df.shift(1).low_price), 'stop'] = df.shift(1).low_price - stop_offset
        df.loc[(df.range == 1) & (df.engulfing == 1) & (df.high_price < df.shift(1).high_price), 'entry'] = df.shift(1).high_price + entry_offset
        df.loc[(df.range == 1) & (df.engulfing == 1), 'risk'] = df.entry - df.stop
        
        df.loc[(df.range == -1) & (df.engulfing == -1), 'signal'] = -1
        df.loc[(df.range == -1) & (df.engulfing == -1), 'stop'] = df.high_price + stop_offset
        df.loc[(df.range == -1) & (df.engulfing == -1), 'entry'] = df.low_price - entry_offset
        df.loc[(df.range == -1) & (df.engulfing == -1) & (df.high_price < df.shift(1).high_price), 'stop'] = df.shift(1).high_price + stop_offset
        df.loc[(df.range == -1) & (df.engulfing == -1) & (df.low_price > df.shift(1).low_price), 'entry'] = df.shift(1).low_price - entry_offset
        df.loc[(df.range == -1) & (df.engulfing == -1), 'risk'] = df.stop - df.entry

        df.loc[(df.range == 1) & (df.kangaroo == 1), 'signal'] = 1
        df.loc[(df.range == 1) & (df.kangaroo == 1), 'stop'] = df.low_price - stop_offset
        df.loc[(df.range == 1) & (df.kangaroo == 1), 'entry'] = df.high_price + entry_offset
        df.loc[(df.range == 1) & (df.kangaroo == 1), 'risk'] = df.entry - df.stop

        df.loc[(df.range == -1) & (df.kangaroo == -1), 'signal'] = -1
        df.loc[(df.range == -1) & (df.kangaroo == -1), 'stop'] = df.high_price + stop_offset
        df.loc[(df.range == -1) & (df.kangaroo == -1), 'entry'] = df.low_price - entry_offset
        df.loc[(df.range == -1) & (df.kangaroo == -1), 'risk'] = df.stop - df.entry

        df.loc[df.signal == 1, 'profit1'] = df.entry + df.risk
        df.loc[df.signal == 1, 'profit2'] = df.entry + (2 * df.risk)
        df.loc[df.signal == -1, 'profit1'] = df.entry - df.risk
        df.loc[df.signal == -1, 'profit2'] = df.entry - (2 * df.risk)
