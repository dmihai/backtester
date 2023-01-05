import time

from backtesting.Backtester import Backtester
from utils.indicators import add_support, add_resistance, add_atr


class HarmonicPatterns(Backtester):
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
            df_swings['open_price'],
            df_swings['high_price'],
            df_swings['low_price'],
            df_swings['close_price'],
            df_swings['body_low_price'],
            df_swings['body_high_price'],
            df_swings['swing_low_score'],
            df_swings['swing_high_score']
        )

        start_found = False
        swings = []
        for i, (index, timestamp, open_price, high_price, low_price, close_price, body_low_price, body_high_price, swing_low_score, swing_high_score) in enumerate(rows):
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
        #print(swings)

        res = []

        # pattern = [0,]
        # self._walk_swings(swings, pattern, res)
        # for pattern in res:
        #     print('--------------------------')
        #     for swing in pattern:
        #         print(swings[swing]['timestamp'])
        for i in range(0, len(swings)-4):
            self._walk_swings(swings, [i], res)
        print(len(res))

        return df
    

    def _walk_swings(self, swings, pattern, res):
        length = len(pattern)
        if length == 4 or length == 6:
            res.append(pattern)
            if length == 6:
                return
        
        leg_max_length = 20
        last_index = pattern[-1]
        last_swing = swings[last_index]
        last_type = last_swing['type']
        min_price = last_swing['wick']
        max_price = last_swing['wick']

        i = last_index + 1
        while i < len(swings) and swings[i]['index'] - last_swing['index'] <= leg_max_length:
            if last_type == 'low' and swings[i]['type'] == 'high' and swings[i]['wick'] > max_price:
                max_price = swings[i]['wick']
            if last_type == 'high' and swings[i]['type'] == 'low' and swings[i]['wick'] < min_price:
                min_price = swings[i]['wick']
            
            if last_type == 'low' and swings[i]['type'] == 'low' and swings[i]['wick'] < min_price:
                break
            if last_type == 'high' and swings[i]['type'] == 'high' and swings[i]['wick'] > max_price:
                break
            
            if last_type == 'low' and swings[i]['type'] == 'high' and max_price <= swings[i]['wick']:
                new_pattern = pattern + [i,]
                self._walk_swings(swings, new_pattern, res)
            
            if last_type == 'high' and swings[i]['type'] == 'low' and min_price >= swings[i]['wick']:
                new_pattern = pattern + [i,]
                self._walk_swings(swings, new_pattern, res)
            
            i += 1

    
    def _calculate_triggers(self):
        df = self._data
