from backtesting.Backtester import Backtester
import time


class HighFreqReversal(Backtester):
    def __init__(self, asset, year, timeframe='M1',
                 profit1_keep_ratio=0.5, adjusted_take_profit=1,
                 candle_length=240,
                 kangaroo_min_pips=20, kangaroo_pin_divisor=3.0, kangaroo_room_left=8, kangaroo_room_divisor=5.0,
                 sr_radius=100, line_score_window=200, line_score_pips=10,
                 trading_cost=0.0002, pip_value=0.0001, signal_expiry=100):

        start = time.time()

        self._candle_length = candle_length
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

        rows = zip(df['timestamp'], df['open_price'], df['high_price'], df['low_price'], df['close_price'])

        prices = {
            'open': [],
            'high': [],
            'low': [],
            'close': []
        }
        kangaroos = 0
        skip = 0
        for i, (timestamp, open_price, high_price, low_price, close_price) in enumerate(rows):
            if i%10000==0:
                print(i, kangaroos)
            
            prices['open'].append(open_price)
            prices['high'].append(high_price)
            prices['low'].append(low_price)
            prices['close'].append(close_price)

            if skip > 0:
                skip -= 1
                continue

            if i > (8 * self._candle_length):
                candle = self._get_candle(prices, 0)
                if self._is_kangaroo(prices, candle):
                    df.loc[i, ['signal', 'entry', 'stop', 'profit1', 'profit2']] = self._get_trigger(candle)
                    skip = 240
                    kangaroos += 1
    
    def _get_candle(self, prices, index):
        return {
            'open': self._get_price(prices['open'], index, 'open'),
            'high': self._get_price(prices['high'], index, 'high'),
            'low': self._get_price(prices['low'], index, 'low'),
            'close': self._get_price(prices['close'], index, 'close')
        }
    
    def _get_trigger(self, candle):
        body_length = (candle['high'] - candle['low']) / self._kangaroo_pin_divisor

        # buy signal
        signal = 1
        entry = candle['high']
        stop = candle['low']
        risk = candle['high'] - candle['low']
        profit1 = candle['high'] + risk
        profit2 = profit1 + risk

        # sell signal
        if candle['open'] <= candle['low'] + body_length and candle['close'] <= candle['low'] + body_length:
            signal = -1
            entry = candle['low']
            stop = candle['high']
            risk = candle['high'] - candle['low']
            profit1 = candle['low'] - risk
            profit2 = profit1 - risk
        
        return [signal, entry, stop, profit1, profit2]

    def _is_kangaroo(self, prices, candle):
        if candle['high']- candle['low'] < self._kangaroo_min_length:
            return False
        
        body_length = (candle['high'] - candle['low']) / self._kangaroo_pin_divisor
        room_length = (candle['high'] - candle['low']) / self._kangaroo_room_divisor

        # look for sell signals
        if candle['open'] <= candle['low'] + body_length and candle['close'] <= candle['low'] + body_length:
            prev_high_price = self._get_price(prices['high'], 1, 'high')
            prev_low_price = self._get_price(prices['low'], 1, 'low')
            if candle['open'] > prev_high_price or candle['close'] > prev_high_price or candle['open'] < prev_low_price or candle['close'] < prev_low_price:
                return False
            for i in range(1, self._kangaroo_room_left + 1):
                if self._get_price(prices['high'], i, 'high') > candle['high'] - room_length:
                    return False
        # look for buy signals
        elif candle['open'] >= candle['high'] - body_length and candle['close'] >= candle['high'] - body_length:
            prev_high_price = self._get_price(prices['high'], 1, 'high')
            prev_low_price = self._get_price(prices['low'], 1, 'low')
            if candle['open'] > prev_high_price or candle['close'] > prev_high_price or candle['open'] < prev_low_price or candle['close'] < prev_low_price:
                return False
            for i in range(1, self._kangaroo_room_left + 1):
                if self._get_price(prices['low'], i, 'low') < candle['low'] + room_length:
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
