def add_pin_bar(df, pip_value=0.0001, min_pips=20):
    df['pin'] = 0

    min_length = min_pips * pip_value
    third_length = (df.high_price - df.low_price) / 3

    df.loc[(df.high_price - df.low_price >= min_length) &
           (df.shift(1).low_price <= df.open_price) &
           (df.shift(1).low_price <= df.close_price) &
           (df.open_price <= df.shift(1).high_price) &
           (df.close_price <= df.shift(1).high_price) &
           (df.open_price <= df.low_price + third_length) &
           (df.close_price <= df.low_price + third_length), 'pin'] = -1
    
    df.loc[(df.high_price - df.low_price >= min_length) &
           (df.shift(1).low_price <= df.open_price) &
           (df.shift(1).low_price <= df.close_price) &
           (df.open_price <= df.shift(1).high_price) &
           (df.close_price <= df.shift(1).high_price) &
           (df.open_price >= df.high_price - third_length) &
           (df.close_price >= df.high_price - third_length), 'pin'] = 1

    return df