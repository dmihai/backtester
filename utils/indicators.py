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

def add_rsi(df, periods=14, column='close_price', ema=True):
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
