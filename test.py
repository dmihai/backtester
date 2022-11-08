from backtesting.strategies.HighFreqReversal import HighFreqReversal

def pretty(d, indent=0):
   for key, value in d.items():
      print('\t' * indent + str(key))
      if isinstance(value, dict):
         pretty(value, indent+1)
      else:
         print('\t' * (indent+1) + str(value))

test = HighFreqReversal('EURUSD', 2021)
test.test()

print(test.get_test_execution_time())
print(test.get_groupby_status())
print(pretty(test.get_results()))
# print(test.get_pnl())
