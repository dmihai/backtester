import math
import numpy as np
from utils.indicators import add_support, add_resistance


def df_to_list(data, columns):
    df = data.copy()
    df_list = []
    prices = {}

    for column in columns:
        df_list.append(df[column])
        prices[column] = []
    
    rows = zip(*df_list)

    for i, val in enumerate(rows):
        for i in range(len(columns)):
            prices[columns[i]].append(val[i])

    return prices


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

def get_kangaroo_score(kangaroo, line_scores, is_buy_signal):
        open_price = kangaroo['open_price']
        high_price = kangaroo['high_price']
        low_price = kangaroo['low_price']
        close_price = kangaroo['close_price']

        if is_buy_signal:
            tail_low = low_price
            tail_high = min(open_price, close_price)
            profit1_low = max(open_price, close_price)
            risk = profit1_low - low_price
            profit1_high = profit1_low + risk
            profit2_low = profit1_high
            profit2_high = profit2_low + risk
        else:
            tail_low = max(open_price, close_price)
            tail_high = high_price
            profit1_high = min(open_price, close_price)
            risk = high_price - profit1_high
            profit1_low = profit1_high - risk
            profit2_high = profit1_low
            profit2_low = profit2_high - risk
        
        max_score = 0
        tail_score = 0
        profit1_score = 0
        profit2_score = 0
        for line, val in line_scores.items():
            if max_score < val:
                max_score = val
            if tail_low <= line and line <= tail_high and tail_score < val:
                tail_score = val
            if profit1_low <= line and line <= profit1_high and profit1_score < val:
                profit1_score = val
            if profit2_low <= line and line <= profit2_high and profit2_score < val:
                profit2_score = val

        tail_factor = 40
        profit1_factor = 40
        profit2_factor = 20

        score = tail_factor * tail_score
        score-= profit1_factor * profit1_score
        score-= profit2_factor * profit2_score

        return score
