def add_support(df, max_radius=200):
    df['support_left'] = 0
    df['support_right'] = 0

    for i in range(1, max_radius):
        df.loc[(df.shift(i).low_price >= df.low_price) & (df.support_left == (i-1)),
               'support_left'] = i
        df.loc[(df.shift(-i).low_price >= df.low_price) & (df.support_right == (i-1)),
               'support_right'] = i

    return df


def add_resistance(df, max_radius=200):
    df['resistance_left'] = 0
    df['resistance_right'] = 0

    for i in range(1, max_radius):
        df.loc[(df.shift(i).high_price <= df.high_price) & (df.resistance_left == (i-1)),
               'resistance_left'] = i
        df.loc[(df.shift(-i).high_price <= df.high_price) & (df.resistance_right == (i-1)),
               'resistance_right'] = i

    return df

def add_heikin_ashi(df):
    df['close_ha'] = (df.open_price + df.high_price + df.low_price + df.close_price) / 4
    df['open_ha'] = (df.open_price + df.close_price) / 2

    for i in range(1, len(df)):
        df.at[i, 'open_ha'] = (df.at[i-1, 'open_ha'] + df.at[i-1, 'close_ha']) / 2

    df['high_ha'] = df[['open_ha', 'close_ha', 'high_price']].max(axis=1)
    df['low_ha'] = df[['open_ha', 'close_ha', 'low_price']].min(axis=1)

    return df

def add_rsi(df, periods=14, ema=True, column='close_price'):
    close_delta = df[column].diff()

    up = close_delta.clip(lower=0)
    down = -1 * close_delta.clip(upper=0)

    if ema == True:
       # use exponential moving average (ema)
       ma_up = up.ewm(com=periods-1, adjust=True, min_periods=periods).mean()
       ma_down = down.ewm(com=periods-1, adjust=True, min_periods=periods).mean()
    else:
       # use simple moving average (sma)
       ma_up = up.rolling(window=periods, adjust=False).mean()
       ma_down = down.rolling(window=periods, adjust=False).mean()

    rsi = ma_up / ma_down
    df['rsi'] = 100 - (100 / (1 + rsi))

    return df

def add_stochastic(df, stoch_length=14, k_length=3, d_length=3, column='close_price'):
    data = df[column]

    min_val = data.rolling(window=stoch_length, center=False).min()
    max_val = data.rolling(window=stoch_length, center=False).max()

    stoch = ((data - min_val) / (max_val - min_val)) * 100

    df['stoch_k'] = stoch.rolling(window=k_length, center=False).mean()
    df['stoch_d'] = df['stoch_k'].rolling(window=d_length, center=False).mean()

    return df
