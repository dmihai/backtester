from backtesting.Backtester import Backtester
from constants import frames
from utils.indicators import add_support, add_resistance
from utils.patterns import add_kangaroo
import time
import math
import numpy as np


# https://forums.babypips.com/t/my-price-action-trading-strategy/582002
class TestReversal(Backtester):
    def __init__(self, asset, year, timeframe='M1',
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

        self._kangaroo_min_length = self._kangaroo_min_pips * pip_value

        super().__init__(asset, year, timeframe, profit1_keep_ratio,
                         adjusted_take_profit, trading_cost, pip_value, signal_expiry)

        self._init_execution_time = time.time() - start

    def prepare_data(self):
        df = super().prepare_data()

        return df
    
    def _calculate_triggers(self):
        df = self._data

        candle_length = 240  # 4h

        kangaroos = 0
        n = len(df.index)
        for i in range(10 * candle_length, n):
            if i%10000==0:
                print(i, kangaroos)
            last_candle = df.loc[i-candle_length+1:i]
            prev_candle = df.loc[i+1-(2*candle_length):i-candle_length]
            if self._is_kangaroo(last_candle, prev_candle):
                kangaroos += 1
            
    def _is_kangaroo(self, candle, prev_candle):
        open_price = candle.iloc[0].open_price
        close_price = candle.iloc[-1].close_price
        high_price = candle['open_price'].max()
        low_price = candle['low_price'].min()

        if high_price - low_price < self._kangaroo_min_length:
            return False
        
        body_length = (high_price - low_price) / self._kangaroo_pin_divisor

        if open_price <= low_price + body_length and close_price <= low_price + body_length:
            prev_high_price = prev_candle['open_price'].max()
            prev_low_price = prev_candle['low_price'].min()
            if open_price <= prev_high_price and close_price <= prev_high_price and open_price >= prev_low_price and close_price >= prev_low_price:
                return True
        
        if open_price >= high_price - body_length and close_price >= high_price - body_length:
            prev_high_price = prev_candle['open_price'].max()
            prev_low_price = prev_candle['low_price'].min()
            if open_price <= prev_high_price and close_price <= prev_high_price and open_price >= prev_low_price and close_price >= prev_low_price:
                return True
        
        return False
