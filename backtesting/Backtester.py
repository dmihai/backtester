import pandas as pd
import time

import config
import constants


class Backtester:
    def __init__(self, asset, year, timeframe, profit1_keep_ratio, adjusted_take_profit, trading_cost, pip_value):
        self._asset = asset
        self._year = year
        self._timeframe = timeframe
        self._profit1_keep_ratio = profit1_keep_ratio
        self._adjusted_take_profit = adjusted_take_profit
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
        df = self._data.copy()
        df['signal'] = 0
        df['stop'] = 0.0
        df['entry'] = 0.0
        df['profit1'] = 0.0
        df['profit2'] = 0.0
        df['begin_offset'] = 0
        df['end_offset'] = 0
        df['status'] = ''
        df['pnl'] = 0.0

        return df

    def get_data(self):
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

        if res is not None:
            results = {
                'all': self._get_session_results(res)
            }

            for sess, hours in constants.sessions.items():
                sess_res = res[(res.timestamp.dt.hour >= hours[0]) &
                               (res.timestamp.dt.hour < hours[1])]
                results[sess] = self._get_session_results(sess_res)

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
        start = time.time()

        self._calculate_triggers()
        self._trade()
        self._calculate_pnl()

        self._test_execution_time = time.time() - start

    def plot_results(self):
        if self._results is not None:
            pass
        else:
            print("Please run test() first.")
    
    def _calculate_triggers(self):
        pass

    def _trade(self):
        df = self._data

        mode = 0
        status = ''
        trading = None
        trading_index = 0
        start_index = 0
        stop_index = 0

        for i, row in df.iterrows():
            if mode == 0:  # looking for a signal
                if row['signal'] != 0:
                    mode = row['signal']
                    trading = row
                    trading_index = i
            elif mode == -1:  # looking for an entry after a sell signal
                if row['high_price'] >= trading['stop']:
                    mode = 0
                    status = 'cancel'
                    stop_index = i
                elif row['low_price'] <= trading['entry']:
                    mode = -2
                    status = 'trading'
                    start_index = i
            elif mode == 1:  # looking for an entry after a buy signal
                if row['low_price'] <= trading['stop']:
                    mode = 0
                    status = 'cancel'
                    stop_index = i
                elif row['high_price'] >= trading['entry']:
                    mode = 2
                    status = 'trading'
                    start_index = i
            elif mode == -2:  # in a sell trading
                if status == 'trading':
                    if row['high_price'] >= trading['stop']:
                        mode = 0
                        status = 'stop'
                        stop_index = i
                    elif row['low_price'] <= trading['profit1']:
                        status = 'profit1'
                        trading['stop'] = trading['entry']
                if status == 'profit1':
                    if row['high_price'] >= trading['stop']:
                        mode = 0
                        status = 'even'
                        stop_index = i
                    elif row['low_price'] <= trading['profit2']:
                        mode = 0
                        status = 'profit2'
                        stop_index = i
            elif mode == 2:  # in a buy trading
                if status == 'trading':
                    if row['low_price'] <= trading['stop']:
                        mode = 0
                        status = 'stop'
                        stop_index = i
                    elif row['high_price'] >= trading['profit1']:
                        status = 'profit1'
                        trading['stop'] = trading['entry']
                if status == 'profit1':
                    if row['low_price'] <= trading['stop']:
                        mode = 0
                        status = 'even'
                        stop_index = i
                    elif row['high_price'] >= trading['profit2']:
                        mode = 0
                        status = 'profit2'
                        stop_index = i

            if mode == 0 and status != '':
                df.loc[trading_index, ['status', 'begin_offset', 'end_offset']] = [
                    status, start_index - trading_index, stop_index - trading_index
                ]
                status = ''

    def _calculate_pnl(self):
        df = self._data

        df.loc[(df.signal == -1) & (df.status == 'stop'),
                'pnl'] = -abs(df.entry - df.stop)
        profit1 = self._profit1_keep_ratio * abs(df.profit1 - df.entry)
        profit2 = (1 - self._profit1_keep_ratio) * \
            abs(df.profit2 - df.entry)
        df.loc[(df.signal == -1) & (df.status == 'even'), 'pnl'] = profit1
        df.loc[(df.signal == -1) & (df.status == 'profit2'),
                'pnl'] = profit1 + profit2

        df.loc[(df.signal == 1) & (df.status == 'stop'),
                'pnl'] = -abs(df.stop - df.entry)
        profit1 = self._profit1_keep_ratio * abs(df.profit1 - df.entry)
        profit2 = (1 - self._profit1_keep_ratio) * \
            abs(df.profit2 - df.entry)
        df.loc[(df.signal == 1) & (df.status == 'even'), 'pnl'] = profit1
        df.loc[(df.signal == 1) & (df.status == 'profit2'),
                'pnl'] = profit1 + profit2

        df.loc[df.status.isin(
            ['stop', 'even', 'profit2']), 'pnl'] = df.pnl - self._trading_cost
        self._results = df.loc[df.pnl != 0, [
            'timestamp', 'pnl', 'status', 'begin_offset', 'end_offset']].reset_index(drop=True)
    
    def _get_session_results(self, results):
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
