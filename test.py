from backtesting.TestStrategy import TestStrategy

test = TestStrategy('EURUSD', 2019)
test.test()
print(test.get_equity_curve(0.0001, 10000, 10000))
print(test.get_groupby_status())
