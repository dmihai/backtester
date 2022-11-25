from backtesting.Backtester import Backtester
import pandas as pd
import time

from utils.functions import calculate_line_scores, get_kangaroo_score


class HighFreqReversal(Backtester):
    def __init__(self, asset, year, timeframe='M1',
                 profit1_keep_ratio=0.5, adjusted_take_profit=1, move_stop_to_breakeven=False,
                 candle_length=240,
                 kangaroo_min_pips=20, kangaroo_pin_divisor=3.0, kangaroo_room_left=10, kangaroo_room_divisor=5.0,
                 kangaroo_min_score=0, kangaroo_max_score=10000,
                 sr_radius=100, line_score_window=200, line_score_pips=10,
                 profit1_risk_ratio=1, profit2_risk_ratio=1,
                 trading_cost=0.0002, pip_value=0.0001, signal_expiry=100):

        start = time.time()

        self._candle_length = candle_length
        self._kangaroo_min_pips = kangaroo_min_pips
        self._kangaroo_pin_divisor = kangaroo_pin_divisor
        self._kangaroo_room_left = kangaroo_room_left
        self._kangaroo_room_divisor = kangaroo_room_divisor
        self._kangaroo_min_score = kangaroo_min_score
        self._kangaroo_max_score = kangaroo_max_score
        self._sr_radius = sr_radius
        self._line_score_window = line_score_window
        self._line_score_pips = line_score_pips
        self._profit1_risk_ratio = profit1_risk_ratio
        self._profit2_risk_ratio = profit2_risk_ratio

        self._kangaroo_min_length = self._kangaroo_min_pips * pip_value

        self._scores_index = -1
        self._scores = {}

        super().__init__(asset, year, timeframe, profit1_keep_ratio,
                         adjusted_take_profit, move_stop_to_breakeven, trading_cost, pip_value, signal_expiry)

        self._init_execution_time = time.time() - start

    def prepare_data(self):
        df = super().prepare_data()

        return df
    
    def _calculate_triggers(self):
        df = self._data

        rows = zip(df['timestamp'], df['open_price'], df['high_price'], df['low_price'], df['close_price'])

        prices = {
            'open_price': [],
            'high_price': [],
            'low_price': [],
            'close_price': []
        }
        kangaroos = 0
        skip = 0
        for i, (timestamp, open_price, high_price, low_price, close_price) in enumerate(rows):
            if i%10000==0:
                print(i, kangaroos)
            
            prices['open_price'].append(open_price)
            prices['high_price'].append(high_price)
            prices['low_price'].append(low_price)
            prices['close_price'].append(close_price)

            if skip > 0:
                skip -= 1
                continue

            if i > (self._kangaroo_room_left * self._candle_length):
                candle = self._get_candle(prices, 0)
                trigger = self._get_trigger(candle)
                if self._is_kangaroo(prices, candle):
                    line_scores = self._get_line_scores(prices)
                    score = get_kangaroo_score(candle, line_scores, trigger[0] == 1)

                    if score >= self._kangaroo_min_score and score <= self._kangaroo_max_score:
                        trigger.append(score)
                        df.loc[i, ['signal', 'entry', 'stop', 'profit1', 'profit2', 'score']] = trigger
                        skip = 240
                        kangaroos += 1
    
    def _get_candle(self, prices, index):
        return {
            'open_price': self._get_price(prices['open_price'], index, 'open'),
            'high_price': self._get_price(prices['high_price'], index, 'high'),
            'low_price': self._get_price(prices['low_price'], index, 'low'),
            'close_price': self._get_price(prices['close_price'], index, 'close')
        }
    
    def _get_compressed_dataframe(self, prices):
        candles = min(len(prices['open_price']) // self._candle_length, self._line_score_window)

        data = []
        for i in range(1, candles):
            candle = self._get_candle(prices, i)
            data.append([candle['open_price'], candle['high_price'], candle['low_price'], candle['close_price']])
        
        return pd.DataFrame(data, columns=['open_price', 'high_price', 'low_price', 'close_price'])
    
    def _get_line_scores(self, prices):
        delta = self._candle_length
        last_index = len(prices['open_price'])
        if self._scores_index < 0 or last_index - self._scores_index > delta:
            df_low = self._get_compressed_dataframe(prices)
            self._scores = calculate_line_scores(df_low, self._sr_radius, self._line_score_pips, self._pip_value)
            self._scores_index = last_index

        return self._scores
    
    def _get_trigger(self, candle):
        body_length = (candle['high_price'] - candle['low_price']) / self._kangaroo_pin_divisor

        # buy signal
        signal = 1
        entry = candle['high_price']
        stop = candle['low_price']
        risk = candle['high_price'] - candle['low_price']
        profit1 = candle['high_price'] + (self._profit1_risk_ratio * risk)
        profit2 = profit1 + (self._profit2_risk_ratio * risk)

        # sell signal
        if candle['open_price'] <= candle['low_price'] + body_length and candle['close_price'] <= candle['low_price'] + body_length:
            signal = -1
            entry = candle['low_price']
            stop = candle['high_price']
            risk = candle['high_price'] - candle['low_price']
            profit1 = candle['low_price'] - (self._profit1_risk_ratio * risk)
            profit2 = profit1 - (self._profit2_risk_ratio * risk)
        
        return [signal, entry, stop, profit1, profit2]

    def _is_kangaroo(self, prices, candle):
        open_price = candle['open_price']
        high_price = candle['high_price']
        low_price = candle['low_price']
        close_price = candle['close_price']

        if high_price- low_price < self._kangaroo_min_length:
            return False
        
        body_length = (high_price - low_price) / self._kangaroo_pin_divisor
        room_length = (high_price - low_price) / self._kangaroo_room_divisor

        # look for sell signals
        if open_price <= (low_price + body_length) and close_price <= (low_price + body_length):
            prev_high_price = self._get_price(prices['high_price'], 1, 'high')
            prev_low_price = self._get_price(prices['low_price'], 1, 'low')
            if open_price > prev_high_price or close_price > prev_high_price or open_price < prev_low_price or close_price < prev_low_price:
                return False
            for i in range(1, self._kangaroo_room_left + 1):
                if self._get_price(prices['high_price'], i, 'high') > (high_price - room_length):
                    return False
        # look for buy signals
        elif open_price >= (high_price - body_length) and close_price >= (high_price - body_length):
            prev_high_price = self._get_price(prices['high_price'], 1, 'high')
            prev_low_price = self._get_price(prices['low_price'], 1, 'low')
            if open_price > prev_high_price or close_price > prev_high_price or open_price < prev_low_price or close_price < prev_low_price:
                return False
            for i in range(1, self._kangaroo_room_left + 1):
                if self._get_price(prices['low_price'], i, 'low') < (low_price + room_length):
                    return False
        else:
            return False
        
        return True
    
    def _get_price(self, list, index, type):
        length = len(list)
        start_index = length - ((index + 1) * self._candle_length)
        end_index = length - (index * self._candle_length) - 1

        if end_index < 0:
            raise Exception("Candle out of bound.")
        
        start_index = max(0, start_index)

        if type == 'open':
            return list[start_index]
        elif type == 'close':
            return list[end_index]
        
        peak = list[start_index]
        for i in range(start_index + 1, end_index + 1):
            if (type == 'high' and peak < list[i]) or (type == 'low' and peak > list[i]):
                peak = list[i]
        
        return peak
