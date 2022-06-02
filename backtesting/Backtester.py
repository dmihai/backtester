import config
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime


class Backtester:
    def __init__(self, asset, year, timeframe, trading_cost):
        self._asset = asset
        self._year = year
        self._timeframe = timeframe
        self._trading_cost = trading_cost

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

    def get_equity_curve(self, lot, investment):
        if self._results is not None:
            pnl = self._results.pnl * lot
            return pnl.cumsum() + investment
        else:
            print("Please run test() first.")

    def get_groupby_status(self):
        return self._data[
            (self._data.status != '') &
            (self._data.status != 'cancel')
        ].groupby('status').size()

    def get_results(self):
        res = self._results

        if res is not None:
            orders = res.timestamp.count()
            winning_orders = res[res.pnl >= 0].timestamp.count()
            winning_ratio = winning_orders / orders

            gross_profit = res[res.pnl >= 0].pnl.sum()
            gross_loss = abs(res[res.pnl < 0].pnl.sum())

            average_gain = gross_profit / winning_orders
            average_loss = gross_loss / (orders - winning_orders)

            profit_factor = gross_profit / gross_loss

            equity = self.get_equity_curve(10000, 10000)
            net_profit = equity.iloc[-1] - 10000

            return {
                "orders": orders,
                "winning_ratio": winning_ratio,
                "net_profit": net_profit,
                "average_gain": average_gain,
                "average_loss": average_loss,
                "profit_factor": profit_factor,
            }

        else:
            print("Please run .test() first")

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
