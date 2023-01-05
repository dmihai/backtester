import time

from backtesting.Backtester import Backtester
from utils.indicators import add_support, add_resistance, add_atr


class HarmonicPatterns(Backtester):
    def __init__(self, asset, year, timeframe='H4',
                 leg_max_length=20, legs_matching_buffer=0.05,
                 profit1_keep_ratio=0.5, adjusted_take_profit=1, move_stop_to_breakeven=False,
                 trading_cost=0.0002, pip_value=0.0001, signal_expiry=100):

        start = time.time()

        self._leg_max_length = leg_max_length
        self._legs_matching_buffer = legs_matching_buffer

        super().__init__(asset, year, timeframe, profit1_keep_ratio,
                         adjusted_take_profit, move_stop_to_breakeven, trading_cost, pip_value, signal_expiry)

        self._init_execution_time = time.time() - start

    def prepare_data(self):
        df = super().prepare_data()

        df = add_support(df)
        df = add_resistance(df)
        df = add_atr(df)

        df['body_low_price'] = df[['close_price', 'open_price']].min(axis=1)
        df['body_high_price'] = df[['close_price', 'open_price']].max(axis=1)

        df['swing_low_score'] = 0
        df['swing_high_score'] = 0

        swing_vals = [1, 2, 30]
        for i in range(len(swing_vals)):
            val = swing_vals[i]
            df.loc[(df.support_left >= val) & (df.support_right >= val), 'swing_low_score'] = i + 1
            df.loc[(df.resistance_left >= val) & (df.resistance_right >= val), 'swing_high_score'] = i + 1
        
        df_swings = df[(df.swing_low_score > 0) | (df.swing_high_score > 0)]
        rows = zip(
            df_swings.index,
            df_swings['timestamp'],
            df_swings['high_price'],
            df_swings['low_price'],
            df_swings['body_low_price'],
            df_swings['body_high_price'],
            df_swings['swing_low_score'],
            df_swings['swing_high_score']
        )

        start_found = False
        swings = []
        for i, (index, timestamp, high_price, low_price, body_low_price, body_high_price, swing_low_score, swing_high_score) in enumerate(rows):
            if not(start_found):
                if swing_low_score == 3 or swing_high_score == 3:
                    start_found = True
                    type = 'low' if swing_low_score == 3 else 'high'
                    swings.append({
                        'timestamp': timestamp.strftime('%Y-%m-%d %H:%M'),
                        'type': type,
                        'index': index,
                        'wick': low_price if type == 'low' else high_price,
                        'body': body_low_price if type == 'low' else body_high_price
                    })
                
                continue
            
            if swing_low_score > 0:
                swings.append({
                    'timestamp': timestamp.strftime('%Y-%m-%d %H:%M'),
                    'type': 'low',
                    'index': index,
                    'wick': low_price,
                    'body': body_low_price
                })
            if swing_high_score > 0:
                swings.append({
                    'timestamp': timestamp.strftime('%Y-%m-%d %H:%M'),
                    'type': 'high',
                    'index': index,
                    'wick': high_price,
                    'body': body_high_price
                })

        res = []

        for i in range(0, len(swings)-4):
            self._walk_swings(swings, [i,], res)
        print(len(res))

        bats = []
        prices = self._df_to_list(df)
        for pattern in res:
            if len(pattern) != 5:
                continue

            xa = self._get_leg_price(swings, pattern[0], pattern[1])
            ab = self._get_leg_price(swings, pattern[1], pattern[2])
            bc = self._get_leg_price(swings, pattern[2], pattern[3])
            cd = self._get_leg_price(swings, pattern[3], pattern[4])

            if self._legs_matching_fibo(xa, ab, 0.382) or self._legs_matching_fibo(xa, ab, 0.5):
                if self._legs_matching_fibo(ab, bc, 0.382):
                    if self._legs_matching_fibo(bc, cd, 1.618):
                        bats.append(pattern)
                if self._legs_matching_fibo(ab, bc, 0.886):
                    if self._legs_matching_fibo(bc, cd, 2.618):
                        bats.append(pattern)

        print(len(bats))
        for bat in bats:
            print('--------------------------------')
            for swing in bat:
                print(swings[swing]['timestamp'])

        return df
    

    def _walk_swings(self, swings, pattern, res):
        length = len(pattern)
        if length >= 4 and length <= 6:
            res.append(pattern)
            if length == 6:
                return
        
        last_index = pattern[-1]
        last_swing = swings[last_index]
        last_type = last_swing['type']
        min_price = last_swing['wick']
        max_price = last_swing['wick']

        i = last_index + 1
        while i < len(swings) and swings[i]['index'] - last_swing['index'] <= self._leg_max_length:
            valid = False
            if last_type == 'low':
                if swings[i]['type'] == 'low' and swings[i]['wick'] < min_price:
                    break
                if swings[i]['type'] == 'high':
                    if swings[i]['wick'] > max_price:
                        max_price = swings[i]['wick']
                    if max_price <= swings[i]['wick']:
                        valid = True
            elif last_type == 'high':
                if swings[i]['type'] == 'high' and swings[i]['wick'] > max_price:
                    break
                if swings[i]['type'] == 'low':
                    if swings[i]['wick'] < min_price:
                        min_price = swings[i]['wick']
                    if min_price >= swings[i]['wick']:
                        valid = True
            
            if valid:
                new_pattern = pattern + [i,]
                self._walk_swings(swings, new_pattern, res)

            i += 1
    

    def _df_to_list(self, df):
        rows = zip(
            df['timestamp'],
            df['open_price'],
            df['high_price'],
            df['low_price'],
            df['close_price'],
            df['body_low_price'],
            df['body_high_price'],
            df['support_left'],
            df['resistance_left']
        )

        prices = {
            'timestamp': [],
            'open_price': [],
            'high_price': [],
            'low_price': [],
            'close_price': [],
            'body_low_price': [],
            'body_high_price': [],
            'support_left': [],
            'resistance_left': [],
        }

        for i, (timestamp, open_price, high_price, low_price, close_price, body_low_price, body_high_price, support_left, resistance_left) in enumerate(rows):
            prices['timestamp'].append(timestamp.strftime('%Y-%m-%d %H:%M'))
            prices['open_price'].append(open_price)
            prices['high_price'].append(high_price)
            prices['low_price'].append(low_price)
            prices['close_price'].append(close_price)
            prices['body_low_price'].append(body_low_price)
            prices['body_high_price'].append(body_high_price)
            prices['support_left'].append(support_left)
            prices['resistance_left'].append(resistance_left)
        
        return prices
    

    def _get_leg_price(self, swings, index1, index2):
        return abs(swings[index1]['wick'] - swings[index2]['wick'])
    

    def _legs_matching_fibo(self, leg1, leg2, fibo):
        fibo_leg = leg1 * fibo
        min_leg = fibo_leg * (1 - self._legs_matching_buffer)
        max_leg = fibo_leg * (1 + self._legs_matching_buffer)

        return min_leg <= leg2 and leg2 <= max_leg

    
    def _calculate_triggers(self):
        df = self._data
