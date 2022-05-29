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

    def get_results(self):
        if self._results is not None:
            return self._results
        else:
            print("Please run .test() first.")

    def get_init_execution_time(self):
        return self._init_execution_time

    def get_test_execution_time(self):
        return self._test_execution_time

    def test(self):
        pass

    def plot_results(self):
        if self._results is not None:
            print("Plotting Results.")
            title = f"{self._instrument}"
            self._results[["creturns", "cstrategy"]].plot(
                title=title, figsize=(12, 8))
            plt.show()
        else:
            print("Please run test() first.")
