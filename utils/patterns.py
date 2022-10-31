# Marks kangaroo tails for sell and buy
# https://www.youtube.com/watch?v=zDgql0g85Ak
def add_kangaroo(df, pip_value=0.0001, min_pips=20, pin_divisor=3.0, space_left=6, space_divisor=3.0):
    df['kangaroo'] = 0

    min_length = min_pips * pip_value
    body_length = (df.high_price - df.low_price) / pin_divisor
    space_length = (df.high_price - df.low_price) / space_divisor

    # mark kangaroo with upper tail (sell signal)
    df.loc[(df.high_price - df.low_price >= min_length) &
           (df.shift(1).low_price <= df.open_price) &
           (df.shift(1).low_price <= df.close_price) &
           (df.open_price <= df.shift(1).high_price) &
           (df.close_price <= df.shift(1).high_price) &
           (df.open_price <= df.low_price + body_length) &
           (df.close_price <= df.low_price + body_length), 'kangaroo'] = -1
    
    # mark kangaroo with lower tail (buy signal)
    df.loc[(df.high_price - df.low_price >= min_length) &
           (df.shift(1).low_price <= df.open_price) &
           (df.shift(1).low_price <= df.close_price) &
           (df.open_price <= df.shift(1).high_price) &
           (df.close_price <= df.shift(1).high_price) &
           (df.open_price >= df.high_price - body_length) &
           (df.close_price >= df.high_price - body_length), 'kangaroo'] = 1

    # check if there is space to the left
    for i in range(1, space_left+1):
        df.loc[(df.kangaroo == -1) & (df.shift(i).high_price > df.high_price - space_length),
               'kangaroo'] = 0
        df.loc[(df.kangaroo == 1) & (df.shift(i).low_price < df.low_price + space_length),
               'kangaroo'] = 0

    return df
