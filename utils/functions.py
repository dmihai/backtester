import math
import numpy as np
from utils.indicators import add_support, add_resistance

def calculate_line_scores(data, sr_radius, line_score_pips, pip_value):
    df = data.copy()

    df = add_support(df, max_radius=sr_radius)
    df = add_resistance(df, max_radius=sr_radius)

    def compute_support_score(price, line, delta):
        body_price = price.close_price if price.close_price < price.open_price else price.open_price

        if price.low_price <= line and line <= body_price:
            return 1
        if body_price < line and line < body_price + delta:
            return (delta + body_price - line) / delta
        if price.low_price - delta < line and line < price.low_price:
            return (line - price.low_price + delta) / delta
        
        return 0
    
    def compute_resistance_score(price, line, delta):
        body_price = price.close_price if price.close_price > price.open_price else price.open_price

        if body_price <= line and line <= price.high_price:
            return 1
        if body_price - delta < line and line < body_price:
            return (line - body_price + delta) / delta
        if price.high_price < line and line < price.high_price + delta:
            return (delta + price.high_price - line) / delta
        
        return 0
    
    df['support_factor'] = df[['support_left', 'support_right']].min(axis=1)
    df['resistance_factor'] = df[['resistance_left', 'resistance_right']].min(axis=1)

    round_digits = math.ceil(math.log(1 / pip_value / line_score_pips, 10))
    start = round(df['low_price'].min() - (line_score_pips * pip_value / 2), round_digits)
    end = round(df['high_price'].max() + (line_score_pips * pip_value / 2), round_digits)
    step = pip_value * line_score_pips
    delta = pip_value * line_score_pips

    scores = {}
    for x in np.arange(start, end + step, step):
        x = round(x, round_digits)
        score = df['support_factor'] * df[['low_price', 'close_price', 'open_price']].apply(compute_support_score, axis=1, line=x, delta=delta)
        score+= df['resistance_factor'] * df[['high_price', 'close_price', 'open_price']].apply(compute_resistance_score, axis=1, line=x, delta=delta)
        scores[x] = score.sum()

    return scores
