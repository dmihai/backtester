from backtesting.Backtester import Backtester
from constants import frames
from utils.indicators import add_support, add_resistance
from utils.patterns import add_kangaroo
import time
import math
import numpy as np


# https://forums.babypips.com/t/my-price-action-trading-strategy/582002
class PriceActionPatterns(Backtester):
    def __init__(self, asset, year, timeframe='D1',
                 profit1_keep_ratio=0.5, adjusted_take_profit=1,
                 kangaroo_min_pips=20, kangaroo_pin_divisor=3.0, kangaroo_room_left=8, kangaroo_room_divisor=5.0,
                 sr_radius=100, line_score_window=200, line_score_pips=10,
                 trading_cost=0.0002, pip_value=0.0001, signal_expiry=3):

        start = time.time()

        self._kangaroo_min_pips = kangaroo_min_pips
        self._kangaroo_pin_divisor = kangaroo_pin_divisor
        self._kangaroo_room_left = kangaroo_room_left
        self._kangaroo_room_divisor = kangaroo_room_divisor
        self._sr_radius = sr_radius
        self._line_score_window = line_score_window
        self._line_score_pips = line_score_pips

        super().__init__(asset, year, timeframe, profit1_keep_ratio,
                         adjusted_take_profit, trading_cost, pip_value, signal_expiry)

        self._init_execution_time = time.time() - start

    def prepare_data(self):
        df = super().prepare_data()

        df = add_kangaroo(df,
                          pip_value=self._pip_value,
                          min_pips=self._kangaroo_min_pips,
                          pin_divisor=self._kangaroo_pin_divisor,
                          room_left=self._kangaroo_room_left,
                          room_divisor=self._kangaroo_room_divisor)
        
        return df
    
    def _calculate_triggers(self):
        df = self._data

        signals = df[df.kangaroo != 0]
        signal_indexes = [i for i in signals.index]

        for i in signal_indexes:
            start_index = max(0, i + 1 - self._line_score_window)
            sample = df.loc[start_index:i]
            line_scores = self._calculate_line_scores(sample)
            kangaroo = df.loc[i]

            score = self._get_kangaroo_score(kangaroo, line_scores)
            if score > 0:
                entry = max(kangaroo.open_price, kangaroo.close_price)
                stop = kangaroo.low_price
                if kangaroo.kangaroo == -1:
                    entry = min(kangaroo.open_price, kangaroo.close_price)
                    stop = kangaroo.high_price
                profit1 = (2 * entry) - stop
                profit2 = (3 * entry) - (2 * stop)

                df.loc[i+1, ['signal', 'entry', 'stop', 'profit1', 'profit2']] = [kangaroo.kangaroo, entry, stop, profit1, profit2]

    def _calculate_line_scores(self, data):
        df = data.copy()

        df = add_support(df, max_radius=self._sr_radius)
        df = add_resistance(df, max_radius=self._sr_radius)

        def compute_support_score(price, line, delta):
            body_price = price.close_price if price.close_price < price.open_price else price.open_price

            if price.low_price <= line and line <= body_price:
                return 1
            if body_price < line and line < body_price + delta:
                return (delta + body_price - line) / delta
            if price.low_price - delta < line and line < price.low_price:
                return (line - price.low_price + delta) / delta
            
            return 0
        
        def compute_resistance_score(price, line, delta):
            body_price = price.close_price if price.close_price > price.open_price else price.open_price

            if body_price <= line and line <= price.high_price:
                return 1
            if body_price - delta < line and line < body_price:
                return (line - body_price + delta) / delta
            if price.high_price < line and line < price.high_price + delta:
                return (delta + price.high_price - line) / delta
            
            return 0
        
        df['support_factor'] = df[['support_left', 'support_right']].min(axis=1)
        df['resistance_factor'] = df[['resistance_left', 'resistance_right']].min(axis=1)

        round_digits = math.ceil(math.log(1 / self._pip_value / self._line_score_pips, 10))
        start = round(df['low_price'].min() - (self._line_score_pips * self._pip_value / 2), round_digits)
        end = round(df['high_price'].max() + (self._line_score_pips * self._pip_value / 2), round_digits)
        step = self._pip_value * self._line_score_pips
        delta = self._pip_value * self._line_score_pips

        scores = {}
        for x in np.arange(start, end + step, step):
            x = round(x, round_digits)
            score = df['support_factor'] * df[['low_price', 'close_price', 'open_price']].apply(compute_support_score, axis=1, line=x, delta=delta)
            score+= df['resistance_factor'] * df[['high_price', 'close_price', 'open_price']].apply(compute_resistance_score, axis=1, line=x, delta=delta)
            scores[x] = score.sum()

        return scores
    
    def _get_kangaroo_score(self, kangaroo, line_scores):
        # buy signal
        tail_low = kangaroo.low_price
        tail_high = min(kangaroo.open_price, kangaroo.close_price)
        profit1_low = max(kangaroo.open_price, kangaroo.close_price)
        risk = profit1_low - kangaroo.low_price
        profit1_high = profit1_low + risk
        profit2_low = profit1_high
        profit2_high = profit2_low + risk

        # sell signal
        if kangaroo.kangaroo == -1:
            tail_low = max(kangaroo.open_price, kangaroo.close_price)
            tail_high = kangaroo.high_price
            profit1_high = min(kangaroo.open_price, kangaroo.close_price)
            risk = kangaroo.high_price - profit1_high
            profit1_low = profit1_high - risk
            profit2_high = profit1_low
            profit2_low = profit2_high - risk
        
        max_score = 0
        tail_score = 0
        profit1_score = 0
        profit2_score = 0
        for line, val in line_scores.items():
            if max_score < val:
                max_score = val
            if tail_low <= line and line <= tail_high and tail_score < val:
                tail_score = val
            if profit1_low <= line and line <= profit1_high and profit1_score < val:
                profit1_score = val
            if profit2_low <= line and line <= profit2_high and profit2_score < val:
                profit2_score = val

        tail_factor = 50
        profit1_factor = 35
        profit2_factor = 15
        segments = tail_factor + profit1_factor + profit2_factor

        segment_width = max_score / segments
        tail_segments = math.floor(tail_score / segment_width)
        profit1_segments = math.ceil(profit1_score / segment_width)
        profit2_segments = math.ceil(profit2_score / segment_width)

        score = tail_factor * (tail_segments - (segments / 2))
        score+= profit1_factor * (segments - profit1_segments - (segments / 2))
        score+= profit2_factor * (segments - profit2_segments - (segments / 2))

        return score
