import pandas as pd
import time

import config
import constants


class Backtester:
    def __init__(self, asset, year, timeframe,
                 profit1_keep_ratio, adjusted_take_profit, move_stop_to_breakeven,
                 trading_cost, pip_value, signal_expiry):
        self._asset = asset
        self._year = year
        self._timeframe = timeframe
        self._profit1_keep_ratio = profit1_keep_ratio
        self._adjusted_take_profit = adjusted_take_profit
        self._move_stop_to_breakeven = move_stop_to_breakeven
        self._trading_cost = trading_cost
        self._pip_value = pip_value
        self._signal_expiry = signal_expiry

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
            self._data.status != ''
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

        def is_target_hit(target, high_price, low_price):
            return low_price <= target and target <= high_price

        status = ''
        trade = {}
        orders = []
        finished_orders = []
        rows = zip(df['signal'], df['high_price'], df['low_price'],
                   df['entry'], df['stop'], df['profit1'], df['profit2'])

        for i, (signal, high_price, low_price, entry, stop, profit1, profit2) in enumerate(rows):
            if status == '':  # not in a trade
                if signal != 0:
                    orders.append({
                        'index': i,
                        'signal': signal,
                        'stop': stop,
                        'entry': entry,
                        'profit1': profit1,
                        'profit2': profit2,
                        'status': 'new',
                    })

                new_cancel_orders = [
                    order | {
                        "status": "cancel",
                        "start_index": i,
                        "stop_index": i
                    }
                    for order in orders
                    if is_target_hit(order['stop'], high_price, low_price)
                ]
                if len(new_cancel_orders) > 0:
                    finished_orders.extend(new_cancel_orders)

                new_expire_orders = [
                    order | {
                        "status": "expire",
                        "start_index": i,
                        "stop_index": i
                    }
                    for order in orders
                    if order['index'] + self._signal_expiry < i
                ]
                if len(new_expire_orders) > 0:
                    finished_orders.extend(new_expire_orders)

                # remove both expired and cancelled orders
                orders[:] = [
                    order for order in orders
                    if order['index'] + self._signal_expiry >= i and
                    not is_target_hit(order['stop'], high_price, low_price)
                ]

                active_orders = [
                    order | {"start_index": i}
                    for order in orders
                    if is_target_hit(order['entry'], high_price, low_price)
                ]
                if len(active_orders) > 0:
                    # take last order in case there are several
                    trade = active_orders[-1]
                    orders[:] = [
                        order for order in orders
                        if order['index'] != trade['index']
                    ]
                    status = 'trading'
            else:  # in a trade
                if status == 'trading':
                    if is_target_hit(trade['stop'], high_price, low_price):
                        finished_orders.append(trade | {
                            "status": "stop",
                            "stop_index": i,
                        })
                        status = ''
                    elif is_target_hit(trade['profit1'], high_price, low_price):
                        if self._move_stop_to_breakeven:
                            trade['stop'] = trade['entry']
                        status = 'profit1'

                if status == 'profit1':
                    if is_target_hit(trade['stop'], high_price, low_price):
                        finished_orders.append(trade | {
                            "status": "even",
                            "stop_index": i,
                        })
                        status = ''
                    elif is_target_hit(trade['profit2'], high_price, low_price):
                        finished_orders.append(trade | {
                            "status": "profit2",
                            "stop_index": i,
                        })
                        status = ''

        for order in finished_orders:
            df.loc[order['index'], ['status', 'begin_offset', 'end_offset']] = [
                order['status'],
                order['start_index'] - order['index'],
                order['stop_index'] - order['index']
            ]

    def _calculate_pnl(self):
        df = self._data

        df.loc[(df.signal != 0) & (df.status == 'stop'),
               'pnl'] = -abs(df.entry - df.stop)
        profit1 = self._profit1_keep_ratio * abs(df.profit1 - df.entry)
        loss1 = (1 - self._profit1_keep_ratio) * abs(df.entry - df.stop)
        profit2 = (1 - self._profit1_keep_ratio) * \
            abs(df.profit2 - df.entry)
        df.loc[(df.signal != 0) & (df.status == 'even'), 'pnl'] = profit1 - loss1
        df.loc[(df.signal != 0) & (df.status == 'profit2'),
               'pnl'] = profit1 + profit2

        df.loc[df.status.isin(
            ['stop', 'even', 'profit2']), 'pnl'] = df.pnl - self._trading_cost
        self._results = df.loc[df.pnl != 0, [
            'timestamp', 'signal', 'pnl', 'status', 'begin_offset', 'end_offset']].reset_index(drop=True)

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
