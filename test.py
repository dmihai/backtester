from backtesting.TestStrategy import TestStrategy

test = TestStrategy('EURUSD', 2019)
test.test()
# print(test.get_equity_curve(0.0001, 10000, 10000))
# print(test.get_groupby_status())
print(test.get_results())
print(f"Init exexution time {test.get_init_execution_time()}")
print(f"Test execution time {test.get_test_execution_time()}")
