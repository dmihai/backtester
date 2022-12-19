import pandas as pd
import time

from backtesting.Backtester import Backtester
from utils.indicators import add_support, add_resistance


# https://www.youtube.com/watch?v=tqB_NPRz3GQ
class DoubleTopDoubleBottom(Backtester):
    def __init__(self, asset, year, timeframe='H4',
                 profit1_keep_ratio=0.5, adjusted_take_profit=1, move_stop_to_breakeven=False,
                 trading_cost=0.0002, pip_value=0.0001, signal_expiry=100):

        start = time.time()

        super().__init__(asset, year, timeframe, profit1_keep_ratio,
                         adjusted_take_profit, move_stop_to_breakeven, trading_cost, pip_value, signal_expiry)

        self._init_execution_time = time.time() - start

    def prepare_data(self):
        df = super().prepare_data()

        df = add_support(df)
        df = add_resistance(df)

        df['body_low_price'] = df[['close_price', 'open_price']].min(axis=1)
        df['body_high_price'] = df[['close_price', 'open_price']].max(axis=1)

        # highest/lowest price of the swing high/low structure
        df['swing_low_wick'] = 0.0
        df['swing_high_wick'] = 0.0

        # highest/lowest body price of the swing high/low structure
        # the body price doesn't have to be on the same candle as the swing highest/lowest price
        df['swing_low_body'] = 0.0
        df['swing_high_body'] = 0.0

        # index to the left/right from the highest/lowest price delimiting the swing structure
        # if negative then the swing is not valid
        df['swing_low_left'] = 0
        df['swing_high_left'] = 0
        df['swing_low_right'] = 0
        df['swing_high_right'] = 0

        # index of the second swing of the double top/bottom structure (from the highest/lowest candle of the first swing)
        # if negative then the double top/bottom structure is not valid
        df['swing_low_next_start'] = 0
        df['swing_low_next_end'] = 0
        df['swing_high_next_start'] = 0
        df['swing_high_next_end'] = 0

        def compute_swing_params(df):
            if df.support_left >= 2 and df.support_right >= 2:
                df['swing_low_wick'] = df.low_price
                df['swing_low_body'] = df['body_low_price']
                df['swing_low_left'] = -1
                df['swing_low_right'] = -1
                df['swing_low_next_start'] = -1
            
            if df.resistance_left >= 2 and df.resistance_right >= 2:
                df['swing_high_wick'] = df.high_price
                df['swing_high_body'] = df['body_high_price']
                df['swing_high_left'] = -1
                df['swing_high_right'] = -1
                df['swing_high_next_start'] = -1
            
            return df

        df = df.apply(compute_swing_params, axis=1)

        for i in range(1, 40):
            if i < 10:
                # check if swing structure is valid
                # (zone between swing wick and swing body has room to left and right)
                df.loc[(df.swing_low_left == -1) & ((df.shift(i).low_price > df.swing_low_body)), 'swing_low_left'] = i
                df.loc[(df.swing_low_right == -1) & ((df.shift(-i).low_price > df.swing_low_body)), 'swing_low_right'] = i
                df.loc[(df.swing_high_left == -1) & ((df.shift(i).high_price < df.swing_high_body)), 'swing_high_left'] = i
                df.loc[(df.swing_high_right == -1) & ((df.shift(-i).high_price < df.swing_high_body)), 'swing_high_right'] = i

                # check if swing structure is invalid
                df.loc[(df.swing_low_left == -1) & ((df.shift(i).low_price < df.swing_low_wick)), 'swing_low_left'] = -2
                df.loc[(df.swing_low_right == -1) & ((df.shift(-i).low_price < df.swing_low_wick)), 'swing_low_right'] = -2
                df.loc[(df.swing_high_left == -1) & ((df.shift(i).high_price > df.swing_high_wick)), 'swing_high_left'] = -2
                df.loc[(df.swing_high_right == -1) & ((df.shift(-i).high_price > df.swing_high_wick)), 'swing_high_right'] = -2

                # update swing body
                df.loc[(df.swing_low_left == -1) & (df.shift(i).body_low_price < df.swing_low_body), 'swing_low_body'] = df.shift(i).body_low_price
                df.loc[(df.swing_low_right == -1) & (df.shift(-i).body_low_price < df.swing_low_body), 'swing_low_body'] = df.shift(-i).body_low_price
                df.loc[(df.swing_high_left == -1) & (df.shift(i).body_high_price > df.swing_high_body), 'swing_high_body'] = df.shift(i).body_high_price
                df.loc[(df.swing_high_right == -1) & (df.shift(-i).body_high_price > df.swing_high_body), 'swing_high_body'] = df.shift(-i).body_high_price

            # check if candle is touching the termination zone without prices closing beyond it
            df.loc[(df.swing_low_left > 0) & (df.swing_low_right > 0) & (df.swing_low_next_start == -1) &
                (df.shift(-i).low_price <= df.swing_low_body) & (df.shift(-i).body_low_price > df.swing_low_wick),
                ['swing_low_next_start', 'swing_low_next_end']] = [i, -1]
            df.loc[(df.swing_high_left > 0) & (df.swing_high_right > 0) & (df.swing_high_next_start == -1) &
                (df.shift(-i).high_price >= df.swing_high_body) & (df.shift(-i).body_high_price < df.swing_high_wick),
                ['swing_high_next_start', 'swing_high_next_end']] = [i, -1]
            
            # check if candle closes beyond the termination zone
            df.loc[(df.swing_low_left > 0) & (df.swing_low_right > 0) & (df.swing_low_next_start == -1) &
                (df.shift(-i).body_low_price <= df.swing_low_wick), 'swing_low_next_start'] = -2
            df.loc[(df.swing_high_left > 0) & (df.swing_high_right > 0) & (df.swing_high_next_start == -1) &
                (df.shift(-i).body_high_price >= df.swing_high_wick), 'swing_high_next_start'] = -2
            
            df.loc[(df.swing_low_next_start > 0) & (df.swing_low_next_end == -1) &
                (df.shift(-i).body_low_price <= df.swing_low_wick), 'swing_low_next_end'] = -2
            df.loc[(df.swing_high_next_start > 0) & (df.swing_high_next_end == -1) &
                (df.shift(-i).body_high_price >= df.swing_high_wick), 'swing_high_next_end'] = -2
            
            # we have a valid double top/bottom that ends at index i
            df.loc[(df.swing_low_next_start > 0) & (df.swing_low_next_end == -1) &
                (df.shift(-i).low_price > df.swing_low_body), 'swing_low_next_end'] = i
            df.loc[(df.swing_high_next_start > 0) & (df.swing_high_next_end == -1) &
                (df.shift(-i).high_price < df.swing_high_body), 'swing_high_next_end'] = i
        
        return df
    
    def _calculate_triggers(self):
        df = self._data
