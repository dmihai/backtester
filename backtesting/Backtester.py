import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

import config
import constants


class Backtester:
    def __init__(self, asset, year, timeframe, trading_cost, pip_value):
        self._asset = asset
        self._year = year
        self._timeframe = timeframe
        self._trading_cost = trading_cost
        self._pip_value = pip_value

        self._data = {}
        self._results = None

        self._data = self.acquire_data(self._timeframe)
        self._data = self.prepare_data()

    def acquire_data(self, timeframe):
        input_file = f"{config.input_path}/{self._asset}/{self._asset}_{timeframe}_{self._year}.csv"

        return pd.read_csv(input_file,
                           header=0,
                           names=['timestamp', 'open_price',
                                  'high_price', 'low_price', 'close_price'],
                           parse_dates=['timestamp'])

    def prepare_data(self):
        return self._data.copy()

    def get_data(self, timeframe):
        return self._data

    def get_equity_curve(self, results, lot, investment):
        pnl = results.pnl * lot
        return pnl.cumsum() + investment

    def get_groupby_status(self):
        return self._data[
            (self._data.status != '') &
            (self._data.status != 'cancel')
        ].groupby('status').size()

    def get_results(self):
        res = self._results

        def get_session_results(results):
            orders = results.timestamp.count()
            winning_orders = results[results.pnl >= 0].timestamp.count()
            winning_ratio = winning_orders / orders if orders > 0 else 0

            gross_profit = results[results.pnl >= 0].pnl.sum()
            gross_loss = abs(results[results.pnl < 0].pnl.sum())

            average_gain = gross_profit / winning_orders if winning_orders > 0 else 0
            lossing_orders = orders - winning_orders
            average_loss = gross_loss / lossing_orders if lossing_orders > 0 else 0

            profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0
            if gross_loss == 0 and gross_profit > 0:
                profit_factor = 100000000

            equity = self.get_equity_curve(results, 10000, 10000)
            net_profit = (equity.iloc[-1] - 10000) if len(equity > 0) else 0

            return {
                "orders": orders,
                "winning_ratio": winning_ratio,
                "net_profit": net_profit,
                "average_gain": average_gain,
                "average_loss": average_loss,
                "profit_factor": profit_factor,
            }

        if res is not None:
            results = {
                'all': get_session_results(res)
            }

            for sess, hours in constants.sessions.items():
                sess_res = res[(res.timestamp.dt.hour >= hours[0]) &
                               (res.timestamp.dt.hour < hours[1])]
                results[sess] = get_session_results(sess_res)

            return results

        else:
            print("Please run .test() first")
    
    def get_pnl(self):
        return self._results

    def get_init_execution_time(self):
        return self._init_execution_time

    def get_test_execution_time(self):
        return self._test_execution_time

    def test(self):
        pass

    def plot_results(self):
        if self._results is not None:
            pass
        else:
            print("Please run test() first.")
