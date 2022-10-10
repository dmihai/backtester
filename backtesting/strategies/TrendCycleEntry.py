from backtesting.Backtester import Backtester
from constants import frames
from utils.indicators import add_heikin_ashi, add_rsi, add_stochastic
import time


# https://www.forexfactory.com/thread/1134457-trend-cycle-entry
class TrendCycleEntry(Backtester):
    def __init__(self, asset, year, timeframe='D1',
                 low_ma=15, high_ma=50, bollinger_ma=20, bollinger_std_factor=2,
                 rsi_length=13, stoch_length=10, k_length=3, d_length=3,
                 profit1_keep_ratio=0.5, adjusted_take_profit=1,
                 trading_cost=0.0002, pip_value=0.0001, signal_expiry=3):

        start = time.time()

        self._low_ma = low_ma
        self._high_ma = high_ma
        self._bollinger_ma = bollinger_ma
        self._bollinger_std_factor = bollinger_std_factor
        self._rsi_length = rsi_length
        self._stoch_length = stoch_length
        self._k_length = k_length
        self._d_length = d_length

        super().__init__(asset, year, timeframe, profit1_keep_ratio,
                         adjusted_take_profit, trading_cost, pip_value, signal_expiry)

        self._init_execution_time = time.time() - start

    def prepare_data(self):
        df = super().prepare_data()

        # Heikin Ashi
        df = add_heikin_ashi(df)

        # Moving Averages
        df['low_ma'] = df['close_price'].ewm(span=self._low_ma).mean()
        df['high_ma'] = df['close_price'].rolling(self._high_ma).mean()

        # Bollinger Bands
        df['bollinger_middle'] = df['close_price'].rolling(self._bollinger_ma).mean()
        bollinger_std = df['close_price'].rolling(self._bollinger_ma).std()
        df['bollinger_upper'] = df['bollinger_middle'] + (bollinger_std * self._bollinger_std_factor)
        df['bollinger_lower'] = df['bollinger_middle'] - (bollinger_std * self._bollinger_std_factor)

        # Stochastic RSI
        df = add_rsi(df, periods=self._rsi_length, ema=True, column='close_price')
        df = add_stochastic(df, stoch_length=self._stoch_length, k_length=self._k_length, d_length=self._d_length, column='rsi')

        return df

    def _calculate_triggers(self):
        df = self._data

        df.loc[((df.low_ha > df.high_ha) & (df.low_ha <= df.bollinger_lower) & (df.bollinger_lower <= df.high_ha)) | 
               ((df.low_ha < df.high_ha) & (df.low_ha <= df.bollinger_upper) & (df.bollinger_upper <= df.high_ha)), 'bollinger_touch'] = 1
        
        print(df.loc[300:, ['timestamp', 'open_price', 'high_price', 'low_price', 'close_price', 'open_ha', 'high_ha', 'low_ha', 'close_ha', 'bollinger_upper', 'bollinger_middle', 'bollinger_lower', 'stoch_k', 'stoch_d']])
