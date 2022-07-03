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
