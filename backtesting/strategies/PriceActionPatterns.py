import time
import math
from backtesting.Backtester import Backtester
from utils.patterns import add_kangaroo
from utils.functions import calculate_line_scores, get_kangaroo_score


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
            line_scores = calculate_line_scores(sample, self._sr_radius, self._line_score_pips, self._pip_value)
            kangaroo = df.loc[i]

            score = get_kangaroo_score(kangaroo, line_scores, kangaroo.kangaroo == 1)
            if score > 0:
                entry = max(kangaroo.open_price, kangaroo.close_price)
                stop = kangaroo.low_price
                if kangaroo.kangaroo == -1:
                    entry = min(kangaroo.open_price, kangaroo.close_price)
                    stop = kangaroo.high_price
                profit1 = (2 * entry) - stop
                profit2 = (3 * entry) - (2 * stop)

                df.loc[i+1, ['signal', 'entry', 'stop', 'profit1', 'profit2']] = [kangaroo.kangaroo, entry, stop, profit1, profit2]
